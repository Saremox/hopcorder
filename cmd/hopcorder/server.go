package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/config"
	"github.com/saremox/hopcorder/internal/enrich"
	"github.com/saremox/hopcorder/internal/server"
	"github.com/saremox/hopcorder/internal/ui"
)

const serverUsage = `Usage: hopcorder server [flags]

Runs the ingest server: agents POST to /api/v1/ingest, records go to
ClickHouse (migrations are applied on startup), and /api/v1/query/ serves
them back. hopcorder does no authentication; put a reverse proxy in front.

Flags:
`

func runServer(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, serverUsage)
		fs.PrintDefaults()
	}
	configPath := fs.String("config", "", "path to the YAML config file")
	listen := fs.String("listen", "", "HTTP listen address: host:port, unix:<path> or systemd (socket activation) (default 127.0.0.1:8080)")
	allowedHosts := fs.String("allowed-hosts", "",
		"comma-separated host names the server answers (DNS rebinding protection; default: the localhost names when listening on loopback, else any; * for any)")
	chURL := fs.String("clickhouse-url", "", "ClickHouse HTTP interface URL (default http://127.0.0.1:8123)")
	logLevel := fs.String("log-level", "", "debug, info, warn or error")
	logFormat := fs.String("log-format", "", "text or json")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "hopcorder server: unexpected arguments %v\n", fs.Args())
		return 2
	}
	cfg := &config.ServerConfig{}
	if *configPath != "" {
		var err error
		if cfg, err = config.LoadServer(*configPath); err != nil {
			fmt.Fprintf(stderr, "hopcorder server: %v\n", err)
			return 2
		}
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "listen":
			cfg.Listen = *listen
		case "allowed-hosts":
			cfg.AllowedHosts = nil
			for _, h := range strings.Split(*allowedHosts, ",") {
				if h = strings.TrimSpace(h); h != "" {
					cfg.AllowedHosts = append(cfg.AllowedHosts, h)
				}
			}
		case "clickhouse-url":
			cfg.ClickHouse.URL = *chURL
		case "log-level":
			cfg.Log.Level = *logLevel
		case "log-format":
			cfg.Log.Format = *logFormat
		}
	})
	if err := cfg.Finalize(); err != nil {
		fmt.Fprintf(stderr, "hopcorder server: invalid configuration:\n  %s\n", strings.ReplaceAll(err.Error(), "\n", "\n  "))
		return 2
	}
	logger := newLogger(cfg.Log, stderr)
	if limit := setMemoryLimit(os.DirFS("/")); limit > 0 {
		logger.Info("Go memory limit set from the cgroup's (set GOMEMLIMIT to override)", "bytes", limit)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, cfg, logger, nil); err != nil {
		logger.Error("server stopped", "err", err)
		return 1
	}
	return 0
}

// serve runs the server until ctx ends. ready, if not nil, receives the
// listening address once the HTTP server accepts connections.
func serve(ctx context.Context, cfg *config.ServerConfig, logger *slog.Logger, ready chan<- string) error {
	pw, err := cfg.ClickHousePassword()
	if err != nil {
		return err
	}
	ch, err := clickhouse.New(clickhouse.Config{URL: cfg.ClickHouse.URL, Database: cfg.ClickHouse.Database,
		User: cfg.ClickHouse.User, Password: pw, Timeout: cfg.ClickHouse.Timeout.D()})
	if err != nil {
		return err
	}

	var bg sync.WaitGroup
	bgCtx, stopBG := context.WithCancel(ctx)
	defer func() { stopBG(); bg.Wait() }()

	var en server.Enrichment
	if *cfg.Enrich.ASN.Enabled {
		if strings.HasPrefix(*cfg.Enrich.ASN.URL, "http://") {
			logger.Warn("enrich.asn.url is plain http: anyone on the path can replace the ASN database",
				"url", *cfg.Enrich.ASN.URL)
		}
		en.ASNs = &enrich.ASNSource{URL: *cfg.Enrich.ASN.URL, File: cfg.Enrich.ASN.File,
			Refresh: cfg.Enrich.ASN.Refresh.D(), Logger: logger}
		bg.Go(func() { en.ASNs.Run(bgCtx) })
	}
	if r := cfg.Enrich.RDNS; *r.Enabled {
		en.Names = &enrich.RDNS{Workers: r.Workers, TTL: r.TTL.D(), NegativeTTL: r.NegativeTTL.D(),
			MaxEntries: r.MaxEntries, Logger: logger}
		bg.Go(func() { en.Names.Run(bgCtx) })
	}

	var reg *prometheus.Registry
	if *cfg.Metrics {
		reg = prometheus.NewRegistry()
		reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		build := prometheus.NewGauge(prometheus.GaugeOpts{Name: "hopcorder_build_info", Help: "Build information.",
			ConstLabels: prometheus.Labels{"version": versionString()}})
		build.Set(1)
		reg.MustRegister(build)
		server.RegisterEnrichmentMetrics(reg, en)
	}
	var uiHandler http.Handler
	if *cfg.UI {
		if uiHandler, err = ui.Handler(); err != nil {
			return err
		}
	}
	// Before the server is built: its Host allowlist depends on what it
	// listens on (see listenerHosts).
	ln, activated, err := listenServer(ctx, cfg)
	if err != nil {
		return err
	}
	allowed := listenerHosts(cfg, ln.Addr(), activated)
	srv := server.New(server.Options{Store: ch, Pinger: ch, Enricher: en, Logger: logger, Registry: reg,
		MaxBodyBytes: int64(cfg.Ingest.MaxBodyBytes), MaxDecodedBytes: int64(cfg.Ingest.MaxDecodedBytes),
		MaxConcurrentIngest: cfg.Ingest.MaxConcurrent, MaxConcurrentQueries: cfg.Query.MaxConcurrent,
		MaxStreams: cfg.Ingest.MaxStreams, MaxStreamsPerAgent: cfg.Ingest.MaxStreamsPerAgent, MaxAgents: cfg.Ingest.MaxAgents,
		StreamIdleTTL:    cfg.Ingest.StreamIdleTimeout.D(),
		MaxRecordAge:     cfg.Ingest.MaxRecordAge.D(),
		MaxResponseBytes: int64(cfg.Query.MaxResponseBytes), MaxQueryMemory: int64(cfg.Query.MaxMemoryBytes),
		AllowedHosts: allowed, UI: uiHandler})

	// Every phase of a connection is bounded, so stalled clients cannot pin
	// connections (and the decode buffers of their requests) indefinitely.
	// Read-API responses also have a deadline for each part (internal/server).
	hs := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 2 * time.Minute,
		WriteTimeout: server.WriteTimeout, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10}
	served := make(chan error, 1)
	go func() { served <- hs.Serve(ln) }()
	hosts := "any"
	if allowed != nil {
		hosts = strings.Join(allowed, ",")
	}
	logger.Info("server listening", "addr", ln.Addr().String(), "network", ln.Addr().Network(),
		"systemd_socket", activated, "clickhouse", cfg.ClickHouse.URL,
		"database", cfg.ClickHouse.Database, "allowed_hosts", hosts)
	if ready != nil {
		ready <- ln.Addr().String()
	}

	// ClickHouse may come up after us (e.g. started together); keep trying.
	// Until then /readyz and ingest answer 503 and agents keep spooling.
	bg.Go(func() {
		for attempt := 0; ; attempt++ {
			err := server.Migrate(bgCtx, ch, logger)
			if err == nil {
				err = server.ApplyRetention(bgCtx, ch, *cfg.ClickHouse.RetentionDays)
			}
			if err == nil {
				srv.SetReady()
				logger.Info("ClickHouse schema is current; accepting ingest")
				return
			}
			if bgCtx.Err() != nil {
				return
			}
			if attempt == 0 || attempt%30 == 0 {
				logger.Warn("cannot prepare ClickHouse schema; retrying", "err", err)
			}
			select {
			case <-bgCtx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	})

	select {
	case err := <-served:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := hs.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
