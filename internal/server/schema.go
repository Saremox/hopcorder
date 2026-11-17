package server

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/saremox/hopcorder/internal/clickhouse"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version int
	name    string
	stmts   []string
}

var migrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// loadMigrations returns the embedded migrations in version order. Files are
// named NNNN_description.sql and hold statements separated by semicolons;
// "--" starts a comment.
func loadMigrations() ([]migration, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %s: name must be NNNN_description.sql", e.Name())
		}
		b, err := migrationFS.ReadFile(path.Join("migrations", e.Name()))
		if err != nil {
			return nil, err
		}
		v, _ := strconv.Atoi(m[1])
		mig := migration{version: v, name: m[2], stmts: splitStatements(string(b))}
		if len(mig.stmts) == 0 {
			return nil, fmt.Errorf("migration %s is empty", e.Name())
		}
		out = append(out, mig)
	}
	slices.SortFunc(out, func(a, b migration) int { return a.version - b.version })
	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %04d", out[i].version)
		}
	}
	return out, nil
}

func splitStatements(sql string) []string {
	var b strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	var out []string
	for _, s := range strings.Split(b.String(), ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Migrate creates the database if needed and applies every migration that
// is not recorded in schema_migrations yet, in order. Statements use IF NOT
// EXISTS, so a migration interrupted half-way is safe to re-run. It then
// checks what ingest relies on and no migration can restore (checkSchema).
//
// A migration that changes a materialized view uses ALTER TABLE ... MODIFY
// QUERY, never DROP VIEW and CREATE: between those two, inserts of another
// server (a rolling upgrade) succeed without the view's rows, and a
// migration that dies there, followed by a rollback to a version that has
// it recorded as applied, leaves no view at all (0002 and 0003 still did).
// The ingester's fillViews queries change with the views.
func Migrate(ctx context.Context, c *clickhouse.Client, log *slog.Logger) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if err := c.ExecServer(ctx, "CREATE DATABASE IF NOT EXISTS "+clickhouse.QuoteIdent(c.Database())); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	if err := c.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations
		(version UInt32, name String, applied_at DateTime DEFAULT now())
		ENGINE = MergeTree ORDER BY version`, nil); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	type row struct {
		Version int `json:"version"`
	}
	done, err := clickhouse.QueryInto[row](ctx, c, "SELECT version FROM schema_migrations", nil)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	applied := map[int]bool{}
	for _, r := range done {
		applied[r.Version] = true
	}
	for _, m := range migs {
		if applied[m.version] {
			continue
		}
		log.Info("applying migration", "version", m.version, "name", m.name)
		for i, s := range m.stmts {
			if err := c.Exec(ctx, s, nil); err != nil {
				return fmt.Errorf("migration %04d_%s statement %d: %w", m.version, m.name, i+1, err)
			}
		}
		if err := c.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ({v:UInt32}, {n:String})",
			clickhouse.Params{"v": strconv.Itoa(m.version), "n": m.name}); err != nil {
			return fmt.Errorf("record migration %04d: %w", m.version, err)
		}
	}
	return checkSchema(ctx, c)
}

// viewsOfCycles are the materialized views ingest needs on cycles, with the
// migration that defines each: a cycle's hop and loop rows are written only
// by them (and by the ingester's fillViews after a failed insert).
var viewsOfCycles = []struct{ name, migration string }{
	{"hops_mv", "0003_hop_loop_ttls.sql"}, {"loop_events_mv", "0001_init.sql"},
}

// checkSchema refuses (the server then stays unready, and says why) what
// would make ingest lose rows without an error: a view of cycles missing,
// agent_seq_mv back (it moved the mark past cycles whose hop rows a later
// view failed to write; the server writes agent_seq since 0004), or a
// ClickHouse user that cannot stop its own queries (the ingester stops a
// timed-out insert before it looks at what the insert stored).
func checkSchema(ctx context.Context, c *clickhouse.Client) error {
	type row struct {
		Views []string `json:"dependencies_table"`
	}
	rows, err := clickhouse.QueryInto[row](ctx, c, `SELECT dependencies_table FROM system.tables
		WHERE database = currentDatabase() AND name = 'cycles'`, nil)
	if err != nil {
		return fmt.Errorf("check the views of cycles: %w", err)
	}
	var views []string
	if len(rows) == 1 {
		views = rows[0].Views
	}
	for _, v := range viewsOfCycles {
		if !slices.Contains(views, v.name) {
			return fmt.Errorf("materialized view %s of cycles is missing, so cycles would be stored without its rows "+
				"(did a migration stop half-way, and a version that skips it run since?): create it as "+
				"internal/server/migrations/%s does, then restart", v.name, v.migration)
		}
	}
	if slices.Contains(views, "agent_seq_mv") {
		return errors.New("materialized view agent_seq_mv exists again (recreated for a rollback?): " +
			"drop it (DROP VIEW agent_seq_mv), the server writes agent_seq itself since migration 0004")
	}
	if err := c.Exec(ctx, "KILL QUERY WHERE query_id = {id:String} SYNC", clickhouse.Params{"id": queryIDPrefix + " check"}); err != nil {
		return fmt.Errorf("the ClickHouse user cannot stop its own queries, which ingest needs after a failed insert "+
			"(GRANT SELECT(query_id, user, query) ON system.processes TO the server's user): %w", err)
	}
	return nil
}

// retentionTables lists the tables bounded by the retention setting and the
// column their age is measured on. agent_seq is tiny and never expires: it
// is what keeps deduplication correct.
var retentionTables = []struct{ table, column string }{
	{"cycles", "ts"}, {"hops", "ts"}, {"loop_events", "ts"}, {"path_changes", "ts"}, {"outages", "start_ts"},
}

// ApplyRetention sets (days > 0) or removes (days == 0) a TTL on every data
// table. It runs on every start so the configured value always applies. The
// TTL is not materialized on existing parts right away (that would rewrite
// them all); ClickHouse drops old rows as it merges.
func ApplyRetention(ctx context.Context, c *clickhouse.Client, days int) error {
	if days < 0 {
		return fmt.Errorf("retention days %d: must not be negative", days)
	}
	for _, t := range retentionTables {
		var q string
		if days > 0 {
			q = fmt.Sprintf("ALTER TABLE %s MODIFY TTL toDateTime(%s) + INTERVAL %d DAY SETTINGS materialize_ttl_after_modify = 0",
				clickhouse.QuoteIdent(t.table), clickhouse.QuoteIdent(t.column), days)
		} else {
			type row struct {
				N int `json:"n"`
			}
			has, err := clickhouse.QueryInto[row](ctx, c, `SELECT count() AS n FROM system.tables
				WHERE database = currentDatabase() AND name = {t:String} AND position(create_table_query, ' TTL ') > 0`,
				clickhouse.Params{"t": t.table})
			if err != nil {
				return fmt.Errorf("retention: %w", err)
			}
			if len(has) == 0 || has[0].N == 0 {
				continue
			}
			q = "ALTER TABLE " + clickhouse.QuoteIdent(t.table) + " REMOVE TTL"
		}
		if err := c.Exec(ctx, q, nil); err != nil {
			return fmt.Errorf("retention on %s: %w", t.table, err)
		}
	}
	return nil
}
