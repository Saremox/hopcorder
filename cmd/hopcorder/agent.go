package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/saremox/hopcorder/internal/agent"
	"github.com/saremox/hopcorder/internal/config"
	"github.com/saremox/hopcorder/internal/output"
	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/spool"
)

const agentUsage = `Usage: hopcorder agent [flags] [target ...]

Runs the probing agent. Targets given as arguments replace the targets of the
config file. Flags override the corresponding global config file settings;
per-target settings in the file still take precedence.

Examples:
  hopcorder agent --once example.net 192.0.2.1
  hopcorder agent --config /etc/hopcorder/agent.yaml

Flags:
`

type agentFlags struct {
	configPath    string
	once          bool
	agentID       string
	interval      time.Duration
	timeout       time.Duration
	maxTTL        int
	probePast     int
	protocol      string
	flowID        int
	paris         bool
	af            int
	spoolDir      string
	jsonl         string
	metricsListen string
	logLevel      string
	logFormat     string
}

func runAgent(args []string, stdout, stderr io.Writer) int {
	var f agentFlags
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, agentUsage)
		fs.PrintDefaults()
	}
	fs.StringVar(&f.configPath, "config", "", "path to the YAML config file")
	fs.BoolVar(&f.once, "once", false, "run one cycle per target, print the records to stdout and exit")
	fs.StringVar(&f.agentID, "agent-id", "", "agent identifier (default: hostname)")
	fs.DurationVar(&f.interval, "interval", 0, "probe interval per target (default 5s)")
	fs.DurationVar(&f.timeout, "timeout", 0, "per-probe timeout (default 1s)")
	fs.IntVar(&f.maxTTL, "max-ttl", 0, "highest TTL probed (default 30)")
	fs.IntVar(&f.probePast, "probe-past", 0, "TTLs kept beyond the last responding hop of an unreached target (default 2)")
	fs.StringVar(&f.protocol, "protocol", "", "default probe protocol: icmp, udp or tcp (default icmp)")
	fs.IntVar(&f.flowID, "flow-id", 0, "default flow_id: ICMP identifier, UDP/TCP source port (default 33434); "+
		"processes on one host need different ones")
	fs.BoolVar(&f.paris, "paris", true, "keep the flow identifier constant across TTLs (paris traceroute)")
	fs.IntVar(&f.af, "af", 0, "address family (4 or 6) for targets given as arguments")
	fs.StringVar(&f.spoolDir, "spool-dir", "", "spool directory")
	fs.StringVar(&f.jsonl, "jsonl", "", "append records to this file (\"-\" for stdout)")
	fs.StringVar(&f.metricsListen, "metrics-listen", "", "serve Prometheus metrics on this address, e.g. 127.0.0.1:9469")
	fs.StringVar(&f.logLevel, "log-level", "", "debug, info, warn or error (default info)")
	fs.StringVar(&f.logFormat, "log-format", "", "text or json (default text)")
	targets, err := parseInterspersed(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	cfg, err := buildConfig(fs, &f, targets)
	if err != nil {
		fmt.Fprintf(stderr, "hopcorder agent: %v\n", err)
		return 2
	}
	logger := newLogger(cfg.Log, stderr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	prober := probe.NewRaw(logger)
	defer prober.Close()
	if err := prober.Check(); err != nil {
		fmt.Fprintf(stderr, "hopcorder agent: %v\n", err)
		return 1
	}

	if f.once {
		return runOnce(ctx, cfg, prober, logger, stdout)
	}
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	err = superviseDaemon(ctx, cfg, prober, reload,
		func() (*config.Config, error) { return buildConfig(fs, &f, targets) },
		func(c *config.Config) *slog.Logger { return newLogger(c.Log, stderr) })
	if err != nil {
		logger.Error("agent stopped", "err", err)
		return 1
	}
	return 0
}

// superviseDaemon runs the daemon and restarts it with a freshly loaded
// configuration whenever reload fires (SIGHUP). An invalid configuration is
// logged and the running one is kept. A valid one replaces everything except
// the prober (raw sockets stay open); the spool persists, so seq continues.
// If the new configuration then fails to start (say its metrics address is
// taken), the previous one is started again.
func superviseDaemon(ctx context.Context, cfg *config.Config, prober probe.Prober, reload <-chan os.Signal,
	load func() (*config.Config, error), mkLogger func(*config.Config) *slog.Logger) error {
	var previous *config.Config // set while a reloaded config is being tried
	for {
		logger := mkLogger(cfg)
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- runDaemon(runCtx, cfg, prober, logger) }()

		var next *config.Config
		for next == nil {
			select {
			case err := <-done:
				cancel()
				if err != nil && previous != nil && ctx.Err() == nil {
					logger.Error("new configuration failed to start; reverting to the previous one", "err", err)
					next, previous = previous, nil
					continue
				}
				return err
			case <-reload:
				c, err := load()
				if err != nil {
					logger.Error("reload: invalid configuration; keeping the current one", "err", err)
					continue
				}
				logger.Info("reloading configuration")
				cancel()
				if err := <-done; err != nil {
					return err
				}
				next, previous = c, cfg
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return nil // SIGTERM arrived during the reload
		default:
		}
		cfg = next
	}
}

// parseInterspersed parses flags that may appear before, between or after
// the positional targets (the standard flag package stops at the first
// positional argument). Everything after "--" is positional.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		consumed := len(args) - fs.NArg()
		if consumed > 0 && args[consumed-1] == "--" {
			return append(positional, fs.Args()...), nil
		}
		if fs.NArg() == 0 {
			return positional, nil
		}
		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// buildConfig loads the config file (if any), applies flags and positional
// targets on top, then defaults and validates the result.
func buildConfig(fs *flag.FlagSet, f *agentFlags, targets []string) (*config.Config, error) {
	cfg := &config.Config{}
	if f.configPath != "" {
		var err error
		if cfg, err = config.Load(f.configPath); err != nil {
			return nil, err
		}
	}
	fs.Visit(func(fl *flag.Flag) {
		switch fl.Name {
		case "agent-id":
			cfg.AgentID = f.agentID
		case "interval":
			cfg.Interval = config.Duration(f.interval)
		case "timeout":
			cfg.Timeout = config.Duration(f.timeout)
		case "max-ttl":
			cfg.MaxTTL = f.maxTTL
		case "probe-past":
			cfg.ProbePast = &f.probePast
		case "protocol":
			cfg.Protocol = f.protocol
		case "flow-id":
			cfg.FlowID = f.flowID
		case "paris":
			cfg.Paris = &f.paris
		case "spool-dir":
			cfg.Spool.Dir = f.spoolDir
		case "jsonl":
			cfg.Outputs.JSONL = &config.JSONL{Path: f.jsonl}
		case "metrics-listen":
			cfg.Outputs.Metrics = &config.Metrics{Listen: f.metricsListen}
		case "log-level":
			cfg.Log.Level = f.logLevel
		case "log-format":
			cfg.Log.Format = f.logFormat
		}
	})
	if len(targets) > 0 {
		cfg.Targets = nil
		for _, h := range targets {
			cfg.Targets = append(cfg.Targets, config.Target{Host: h, AF: f.af})
		}
	}
	if err := cfg.Finalize(config.FinalizeOptions{Once: f.once}); err != nil {
		return nil, fmt.Errorf("invalid configuration:\n  %s", strings.ReplaceAll(err.Error(), "\n", "\n  "))
	}
	return cfg, nil
}

func newLogger(c config.Log, w io.Writer) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(c.Level)) // validated by config
	opts := &slog.HandlerOptions{Level: level}
	if c.Format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

func agentTargets(cfg *config.Config) []agent.Target {
	out := make([]agent.Target, len(cfg.Targets))
	for i, t := range cfg.Targets {
		out[i] = agent.Target{
			Host:        t.Host,
			Protocol:    probe.Protocol(t.Protocol),
			Port:        uint16(t.Port),   //nolint:gosec // validated 0..65535
			FlowID:      uint16(t.FlowID), //nolint:gosec // validated 1..65535
			Paris:       *t.Paris,
			AF:          t.AF,
			Interval:    t.Interval.D(),
			Timeout:     t.Timeout.D(),
			MaxTTL:      t.MaxTTL,
			ProbePast:   *t.ProbePast,
			AdaptiveTTL: *t.AdaptiveMaxTTL,
		}
	}
	return out
}

// holdFlows makes the prober hold the local identifiers of the targets'
// flows (see probe.FlowHolder), releasing those of earlier configurations.
func holdFlows(prober probe.Prober, targets []agent.Target) error {
	h, ok := prober.(probe.FlowHolder)
	if !ok {
		return nil
	}
	flows := make([]probe.Flow, len(targets))
	for i, t := range targets {
		flows[i] = probe.Flow{Protocol: t.Protocol, ID: t.FlowID}
	}
	if err := h.HoldFlows(flows); err != nil {
		return fmt.Errorf("%w (set another with --flow-id or flow_id in the config)", err)
	}
	return nil
}

func runOnce(ctx context.Context, cfg *config.Config, prober probe.Prober, logger *slog.Logger, stdout io.Writer) int {
	targets := agentTargets(cfg)
	if err := holdFlows(prober, targets); err != nil {
		logger.Error("cannot start", "err", err)
		return 1
	}
	a, err := agent.New(agent.Options{
		AgentID: cfg.AgentID, Targets: targets, DNSTTL: cfg.DNSTTL.D(),
		Prober: prober, Logger: logger,
	})
	if err != nil {
		logger.Error("cannot start agent", "err", err)
		return 1
	}
	recs, err := a.RunOnce(ctx)
	enc := json.NewEncoder(stdout)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			logger.Error("write record", "err", err)
			return 1
		}
	}
	if err != nil {
		logger.Error("some targets failed", "err", err)
		return 1
	}
	return 0
}

// shutdownGrace bounds how long the agent waits for outputs to stop after
// SIGINT/SIGTERM. Nothing is lost by cutting it short: undelivered records
// stay in the spool.
var shutdownGrace = 3 * time.Second

func runDaemon(ctx context.Context, cfg *config.Config, prober probe.Prober, logger *slog.Logger) error {
	// First, so that a flow another process holds fails the start (or the
	// reload) before anything else is opened.
	targets := agentTargets(cfg)
	if err := holdFlows(prober, targets); err != nil {
		return err
	}
	var outs []output.Output
	var drainOpts []output.DrainOptions
	defer func() {
		for _, o := range outs {
			if err := o.Close(); err != nil {
				logger.Warn("closing output", "output", o.Name(), "err", err)
			}
		}
	}()
	if j := cfg.Outputs.JSONL; j != nil {
		o, err := output.NewJSONL(j.Path)
		if err != nil {
			return err
		}
		outs = append(outs, o)
		drainOpts = append(drainOpts, output.DrainOptions{Logger: logger})
	}
	if h := cfg.Outputs.HTTP; h != nil {
		if h.CleartextCredentials() {
			logger.Warn("outputs.http.url is plain http: the configured credentials and headers travel unencrypted",
				"url", h.URL)
		}
		hc := output.HTTPConfig{
			URL: h.URL, Headers: h.Headers, BearerToken: h.BearerToken, BearerTokenFile: h.BearerTokenFile,
			CAFile: h.TLS.CAFile, CertFile: h.TLS.CertFile, KeyFile: h.TLS.KeyFile, ServerName: h.TLS.ServerName,
			Timeout: h.Timeout.D(), UserAgent: "hopcorder/" + versionString(),
		}
		if b := h.BasicAuth; b != nil {
			hc.BasicUser, hc.BasicPassword, hc.BasicPasswordFile = b.Username, b.Password, b.PasswordFile
		}
		o, err := output.NewHTTP(hc)
		if err != nil {
			return err
		}
		outs = append(outs, o)
		drainOpts = append(drainOpts, output.DrainOptions{
			MaxRecords: h.BatchRecords, MaxBytes: int(h.BatchBytes), FlushInterval: h.FlushInterval.D(),
			RetryMin: h.RetryMin.D(), RetryMax: h.RetryMax.D(), Logger: logger,
		})
	}

	names := make([]string, len(outs))
	for i, o := range outs {
		names[i] = o.Name()
	}
	sp, err := spool.Open(spool.Options{
		Dir: cfg.Spool.Dir, MaxAge: cfg.Spool.MaxAge.D(), MaxBytes: int64(cfg.Spool.MaxBytes),
		Consumers: names, Logger: logger,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := sp.Close(); err != nil {
			logger.Error("closing spool", "err", err)
		}
	}()

	var observer agent.Observer
	if mc := cfg.Outputs.Metrics; mc != nil {
		stats, _ := prober.(probe.StatsProvider)
		m := output.NewMetrics(output.MetricsOptions{
			AgentID: cfg.AgentID, Version: versionString(), Targets: targets, Outputs: names,
			Spool: sp, Prober: stats,
		})
		stopMetrics, err := m.Serve(ctx, mc.Listen, mc.Path, logger)
		if err != nil {
			return err
		}
		defer stopMetrics()
		for i := range drainOpts {
			drainOpts[i].OnError = m.OutputError(names[i])
		}
		observer = m
	}

	a, err := agent.New(agent.Options{
		AgentID: cfg.AgentID, Targets: targets, DNSTTL: cfg.DNSTTL.D(),
		Prober: prober, Sink: sp, Observer: observer, Logger: logger,
	})
	if err != nil {
		return err
	}

	// Outputs drain on their own context so they can be stopped after the
	// schedulers, letting the last records reach them when that is quick.
	drainCtx, stopDrains := context.WithCancel(context.WithoutCancel(ctx))
	defer stopDrains()
	flushNow := make(chan struct{})
	var drains sync.WaitGroup
	for i, o := range outs {
		drainOpts[i].FlushNow = flushNow
		drains.Go(func() { output.Drain(drainCtx, sp, o, drainOpts[i]) })
	}

	logger.Info("agent started", "agent_id", cfg.AgentID, "targets", len(cfg.Targets),
		"outputs", names, "spool", cfg.Spool.Dir, "spool_records", sp.Stats().Records)
	err = a.Run(ctx)
	logger.Info("shutting down")
	close(flushNow) // ship partial batches now rather than after flush_interval
	waitIdle(sp, names, shutdownGrace)
	stopDrains()
	drains.Wait()
	return err
}

// waitIdle waits up to d for every consumer to catch up with the spool.
func waitIdle(sp *spool.Spool, names []string, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		idle := true
		for _, n := range names {
			if sp.Pending(n) > 0 {
				idle = false
			}
		}
		if idle {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
