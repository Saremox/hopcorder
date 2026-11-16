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
	"github.com/saremox/hopcorder/internal/record"
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
	fs.StringVar(&f.protocol, "protocol", "", "default probe protocol: icmp or udp (default icmp)")
	fs.BoolVar(&f.paris, "paris", true, "keep the flow identifier constant across TTLs (paris traceroute)")
	fs.IntVar(&f.af, "af", 0, "address family (4 or 6) for targets given as arguments")
	fs.StringVar(&f.spoolDir, "spool-dir", "", "spool directory")
	fs.StringVar(&f.jsonl, "jsonl", "", "append records to this file (\"-\" for stdout)")
	fs.StringVar(&f.metricsListen, "metrics-listen", "", "serve Prometheus metrics on this address, e.g. 127.0.0.1:9469")
	fs.StringVar(&f.logLevel, "log-level", "", "debug, info, warn or error (default info)")
	fs.StringVar(&f.logFormat, "log-format", "", "text or json (default text)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	cfg, err := buildConfig(fs, &f)
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
	if err := runDaemon(ctx, cfg, prober, logger); err != nil {
		logger.Error("agent stopped", "err", err)
		return 1
	}
	return 0
}

// buildConfig loads the config file (if any), applies flags and positional
// targets on top, then defaults and validates the result.
func buildConfig(fs *flag.FlagSet, f *agentFlags) (*config.Config, error) {
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
	if fs.NArg() > 0 {
		cfg.Targets = nil
		for _, h := range fs.Args() {
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
			Host:      t.Host,
			Protocol:  probe.Protocol(t.Protocol),
			Port:      uint16(t.Port),   //nolint:gosec // validated 0..65535
			FlowID:    uint16(t.FlowID), //nolint:gosec // validated 1..65535
			Paris:     *t.Paris,
			AF:        t.AF,
			Interval:  t.Interval.D(),
			Timeout:   t.Timeout.D(),
			MaxTTL:    t.MaxTTL,
			ProbePast: *t.ProbePast,
		}
	}
	return out
}

func runOnce(ctx context.Context, cfg *config.Config, prober probe.Prober, logger *slog.Logger, stdout io.Writer) int {
	a, err := agent.New(agent.Options{
		AgentID: cfg.AgentID, Targets: agentTargets(cfg), DNSTTL: cfg.DNSTTL.D(),
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

func runDaemon(ctx context.Context, cfg *config.Config, prober probe.Prober, logger *slog.Logger) error {
	if cfg.Outputs.HTTP != nil || cfg.Outputs.Metrics != nil {
		return errors.New("http and metrics outputs are not implemented yet")
	}
	if cfg.Outputs.JSONL == nil {
		return errors.New("no jsonl output configured")
	}
	j, err := output.NewJSONL(cfg.Outputs.JSONL.Path)
	if err != nil {
		return err
	}
	defer j.Close()
	a, err := agent.New(agent.Options{
		AgentID: cfg.AgentID, Targets: agentTargets(cfg), DNSTTL: cfg.DNSTTL.D(),
		Prober: prober, Sink: &directSink{out: j}, Logger: logger,
	})
	if err != nil {
		return err
	}
	logger.Info("agent started", "agent_id", cfg.AgentID, "targets", len(cfg.Targets))
	return a.Run(ctx)
}

// directSink writes records straight to an output with an in-memory seq.
// It is replaced by the spool.
type directSink struct {
	mu  sync.Mutex
	seq uint64
	out output.Output
}

func (s *directSink) Publish(ctx context.Context, rec *record.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	rec.Seq = s.seq
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.out.Deliver(ctx, [][]byte{b})
}
