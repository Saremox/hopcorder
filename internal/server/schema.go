package server

import (
	"context"
	"embed"
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
// EXISTS, so a migration interrupted half-way is safe to re-run.
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
	return nil
}
