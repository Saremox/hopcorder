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
	"slices"
	"strings"
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
	fs.IntVar(&f.flowID, "flow-id", 0, "default flow_id: ICMP identifier, UDP/TCP source port (default 29434); "+
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

// Waits between attempts to start a configuration that ran before but now
// fails to start (doubling from the first to the second).
var restartBackoffMin, restartBackoffMax = time.Second, time.Minute

// superviseDaemon runs the daemon and restarts it with a freshly loaded
// configuration whenever reload fires (SIGHUP). An invalid configuration is
// logged and the running one is kept. A valid one replaces everything except
// the prober (raw sockets stay open) and the metrics listener while its
// address stays the same; the spool persists, so seq continues.
//
// Only the first start can end the supervisor (returning its error); a
// running agent is never stopped by a reload. If a new configuration fails
// to start, the previous one is started again, and if that fails too (say
// its JSONL directory went away), it is retried with a growing wait, until
// it starts or a reload brings one that does.
func superviseDaemon(ctx context.Context, cfg *config.Config, prober probe.Prober, reload <-chan os.Signal,
	load func() (*config.Config, error), mkLogger func(*config.Config) *slog.Logger) error {
	s := &supervisor{prober: prober}
	defer s.closeMetrics()
	var (
		good    *config.Config // the configuration that started last; nil before the first start
		next    = cfg          // the configuration to start
		backoff time.Duration  // wait before the next start; 0: at once
	)
	for {
		logger := mkLogger(next)
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-reload:
				if c, err := load(); err != nil {
					logger.Error("reload: invalid configuration", "err", err)
				} else {
					logger.Info("reloading configuration")
					next, logger = c, mkLogger(c)
				}
			case <-time.After(backoff):
			}
		}
		r, err := s.start(ctx, next, logger)
		switch {
		case ctx.Err() != nil && r != nil:
			return r.stop() // SIGTERM arrived as it started
		case ctx.Err() != nil:
			return nil // SIGTERM arrived during the start
		case err != nil && good == nil:
			return err // the first start fails loudly
		case err != nil && next != good:
			logger.Error("new configuration failed to start; reverting to the previous one", "err", err)
			next, backoff = good, 0
			continue
		case err != nil:
			backoff = min(max(2*backoff, restartBackoffMin), restartBackoffMax)
			logger.Error("cannot start the configuration again; retrying", "err", err, "in", backoff)
			continue
		}
		good, backoff = next, 0

		for r != nil {
			select {
			case err := <-r.done:
				if ctx.Err() != nil {
					return err
				}
				// runDaemon returns on its own only after a failure;
				// start the configuration again.
				logger.Error("agent stopped unexpectedly; restarting it", "err", err)
				backoff = restartBackoffMin
				r = nil
			case <-reload:
				c, err := load()
				if err != nil {
					logger.Error("reload: invalid configuration; keeping the current one", "err", err)
					continue
				}
				logger.Info("reloading configuration")
				if err := r.stop(); err != nil {
					logger.Error("agent stopped with an error for the reload", "err", err)
				}
				next, r = c, nil
			case <-ctx.Done():
				return r.stop()
			}
		}
	}
}

// supervisor owns what superviseDaemon keeps across configurations besides
// the prober: the metrics listener, and the flows the prober holds.
type supervisor struct {
	prober  probe.Prober
	metrics *output.MetricsServer // bound for the running configuration; nil: it has no metrics
	flows   []probe.Flow          // held for the configuration that started last
}

// daemonRun is a started runDaemon.
type daemonRun struct {
	cancel context.CancelFunc
	done   chan error
}

// stop cancels the daemon and returns its error once it has stopped.
func (r *daemonRun) stop() error {
	r.cancel()
	return <-r.done
}

// start starts runDaemon with cfg and returns once it runs, or its error if
// it cannot start. cfg's metrics address is bound unless the running
// configuration's listener has it; a new one is bound before the old one is
// released, which happens only once cfg runs, so the port is never free
// and going back to the previous configuration finds its listener. The
// flows are handled the same way: cfg's are held besides those of the
// configuration that started last, which are released once cfg runs, so a
// local process never finds the identifiers the agent may go back to free.
func (s *supervisor) start(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*daemonRun, error) {
	flows := flowsOf(agentTargets(cfg))
	held := unionFlows(s.flows, flows)
	holdFlows(s.prober, held, logger)
	var ms *output.MetricsServer
	if mc := cfg.Outputs.Metrics; mc != nil {
		if s.metrics != nil && s.metrics.Addr() == mc.Listen {
			ms = s.metrics
		} else {
			var err error
			if ms, err = output.ListenMetrics(ctx, mc.Listen, logger); err != nil {
				return nil, err
			}
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	r := &daemonRun{cancel: cancel, done: make(chan error, 1)}
	started := make(chan struct{})
	go func() {
		r.done <- runDaemon(runCtx, cfg, s.prober, logger,
			daemonEnv{metrics: ms, flowsHeld: true, started: func() { close(started) }})
	}()
	select {
	case <-started:
	case err := <-r.done:
		cancel()
		if ms != nil && ms != s.metrics {
			ms.Close()
		}
		if err == nil {
			err = errors.New("stopped while starting")
		}
		return nil, err
	}
	if s.metrics != nil && s.metrics != ms {
		s.metrics.Close()
	}
	s.metrics = ms
	if len(held) != len(flows) {
		// Release the previous configuration's own flows. Those still
		// in use by another process are logged, again, by the targets.
		holdFlows(s.prober, flows, nil)
	}
	s.flows = flows
	return r, nil
}

func (s *supervisor) closeMetrics() {
	if s.metrics != nil {
		s.metrics.Close()
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

// flowsOf returns the distinct flows of targets, in order.
func flowsOf(targets []agent.Target) []probe.Flow {
	var flows []probe.Flow
	for _, t := range targets {
		if f := probe.NewFlow(t.Protocol, t.FlowID, t.Paris, t.MaxTTL); !slices.Contains(flows, f) {
			flows = append(flows, f)
		}
	}
	return flows
}

// unionFlows returns a's flows and then those of b that a lacks.
func unionFlows(a, b []probe.Flow) []probe.Flow {
	out := slices.Clone(a)
	for _, f := range b {
		if !slices.Contains(out, f) {
			out = append(out, f)
		}
	}
	return out
}

// holdFlows makes the prober hold the local identifiers of flows (ICMP echo
// identifiers, TCP source ports; see probe.FlowHolder), releasing every
// other. An identifier it cannot hold (another process has it) stops only
// the targets of that flow: the prober refuses their cycles, and retries
// the identifier on each, while every other target is probed. That is
// logged here unless logger is nil, and then per target by the agent (and
// counted in hopcorder_flow_in_use).
func holdFlows(prober probe.Prober, flows []probe.Flow, logger *slog.Logger) {
	h, ok := prober.(probe.FlowHolder)
	if !ok {
		return
	}
	if err := h.HoldFlows(flows); err != nil && logger != nil {
		logger.Error("cannot hold the identifier or port of some flows: their targets are not probed until it is free "+
			"(retried every cycle; set another with --flow-id or flow_id in the config)", "err", err)
	}
}

func runOnce(ctx context.Context, cfg *config.Config, prober probe.Prober, logger *slog.Logger, stdout io.Writer) int {
	// Targets on a flow another process holds fail like any target that
	// cannot be probed: the others' records are printed, and the exit
	// status is 1.
	targets := agentTargets(cfg)
	holdFlows(prober, flowsOf(targets), logger)
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

// shutdownGrace bounds how long a configuration that stops (SIGINT/SIGTERM,
// or a reload) waits for its outputs to deliver what is spooled, and then
// how long for their drains to return. Nothing is lost by cutting it short:
// undelivered records stay in the spool.
var shutdownGrace = 3 * time.Second

// daemonEnv is what runDaemon gets from its supervisor.
type daemonEnv struct {
	// metrics is the listener for cfg's metrics address, which runDaemon
	// serves its metrics on (and 503 once it stops) but never closes. If
	// nil, runDaemon binds the address itself, for as long as it runs.
	metrics *output.MetricsServer
	// flowsHeld says the supervisor holds cfg's flows (see
	// supervisor.start); if false, runDaemon holds them itself.
	flowsHeld bool
	// started, if set, is called once the daemon runs.
	started func()
}

// runDaemon runs the agent with cfg until ctx is cancelled. An error means
// it could not start.
func runDaemon(ctx context.Context, cfg *config.Config, prober probe.Prober, logger *slog.Logger, env daemonEnv) error {
	// A flow another process holds stops only its own targets.
	targets := agentTargets(cfg)
	if !env.flowsHeld {
		holdFlows(prober, flowsOf(targets), logger)
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
	var metrics *output.Metrics
	if mc := cfg.Outputs.Metrics; mc != nil {
		warnSharedSeries(cfg.Targets, logger)
		stats, _ := prober.(probe.StatsProvider)
		metrics = output.NewMetrics(output.MetricsOptions{
			AgentID: cfg.AgentID, Version: versionString(), Targets: targets, Outputs: names,
			Spool: sp, Prober: stats,
		})
		for i := range drainOpts {
			drainOpts[i].OnError = metrics.OutputError(names[i])
		}
		observer = metrics
	}

	a, err := agent.New(agent.Options{
		AgentID: cfg.AgentID, Targets: targets, DNSTTL: cfg.DNSTTL.D(),
		Prober: prober, Sink: sp, Observer: observer, Logger: logger,
	})
	if err != nil {
		return err
	}
	var ms *output.MetricsServer
	if mc := cfg.Outputs.Metrics; mc != nil {
		if ms = env.metrics; ms == nil {
			if ms, err = output.ListenMetrics(ctx, mc.Listen, logger); err != nil {
				return err
			}
			defer ms.Close()
		}
		ms.Serve(mc.Path, metrics)
	}
	if env.started != nil {
		env.started()
	}

	flushNow := make(chan struct{})
	for i := range drainOpts {
		drainOpts[i].FlushNow = flushNow
	}
	drains := output.StartDrains(ctx, sp, outs, drainOpts)

	logger.Info("agent started", "agent_id", cfg.AgentID, "targets", len(cfg.Targets),
		"outputs", names, "spool", cfg.Spool.Dir, "spool_records", sp.Stats().Records)
	err = a.Run(ctx)
	if ms != nil {
		ms.Serve("", nil) // 503 from now until the next configuration runs: nothing is probed
	}
	logger.Info("shutting down")
	close(flushNow) // ship partial batches now rather than after flush_interval
	waitIdle(sp, names, shutdownGrace)
	// A delivery that blocks for good (a pipe nobody reads) is left
	// behind after another shutdownGrace: a reload or shutdown never
	// waits for it longer.
	drains.Stop(shutdownGrace, logger)
	return err
}

// warnSharedSeries logs the targets whose metrics share their series with
// an earlier target's: those with the same host and protocol (another port
// or address family, and so another flow_id, which config requires), since
// labels are only ever the configured host and protocol. Their counters count both, the RTT histogram mixes them, and
// hopcorder_path_length is whichever cycle ended last.
func warnSharedSeries(targets []config.Target, logger *slog.Logger) {
	first := map[[2]string]int{}
	for i, t := range targets {
		k := [2]string{t.Host, t.Protocol}
		if j, ok := first[k]; ok {
			logger.Warn("targets share their metrics series (same host and protocol); "+
				"give one an address or another name for the host to tell them apart",
				"target", t.Host, "protocol", t.Protocol, "targets", []int{j, i})
			continue
		}
		first[k] = i
	}
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
