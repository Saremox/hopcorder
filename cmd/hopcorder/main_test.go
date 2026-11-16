package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunCommands(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"version"}, &out, &errb); code != 0 || !strings.HasPrefix(out.String(), "hopcorder ") {
		t.Errorf("version: code %d out %q", code, out.String())
	}
	if code := run(nil, io.Discard, &errb); code != 2 {
		t.Errorf("no args: code %d", code)
	}
	if code := run([]string{"bogus"}, io.Discard, io.Discard); code != 2 {
		t.Errorf("unknown command: code %d", code)
	}
	if code := run([]string{"server", "--listen", "nope"}, io.Discard, io.Discard); code != 2 {
		t.Errorf("server with bad listen address: code %d", code)
	}
	errb.Reset()
	if code := run([]string{"server", "-h"}, io.Discard, &errb); code != 0 || !strings.Contains(errb.String(), "-clickhouse-url") {
		t.Errorf("server -h: code %d, usage %q", code, errb.String())
	}
	errb.Reset()
	if code := run([]string{"agent", "-h"}, io.Discard, &errb); code != 0 || !strings.Contains(errb.String(), "-once") {
		t.Errorf("agent -h: code %d, usage %q", code, errb.String())
	}
}

func TestAgentRejectsInvalidConfig(t *testing.T) {
	var errb bytes.Buffer
	code := run([]string{"agent", "--interval", "10ms"}, io.Discard, &errb)
	if code != 2 {
		t.Errorf("code %d", code)
	}
	for _, want := range []string{"invalid configuration", "targets: at least one target", "spool.dir: required"} {
		if !strings.Contains(errb.String(), want) {
			t.Errorf("stderr lacks %q:\n%s", want, errb.String())
		}
	}
}

func parseAgentFlags(t *testing.T, args ...string) (*flag.FlagSet, *agentFlags, []string) {
	t.Helper()
	var f agentFlags
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	fs.StringVar(&f.configPath, "config", "", "")
	fs.BoolVar(&f.once, "once", false, "")
	fs.DurationVar(&f.interval, "interval", 0, "")
	fs.StringVar(&f.protocol, "protocol", "", "")
	fs.IntVar(&f.flowID, "flow-id", 0, "")
	fs.StringVar(&f.jsonl, "jsonl", "", "")
	fs.IntVar(&f.af, "af", 0, "")
	fs.BoolVar(&f.paris, "paris", true, "")
	targets, err := parseInterspersed(fs, args)
	if err != nil {
		t.Fatal(err)
	}
	return fs, &f, targets
}

func TestFlagsMayFollowTargets(t *testing.T) {
	_, f, targets := parseAgentFlags(t, "a.example", "-protocol", "udp", "b.example", "--once", "--", "-weird-host")
	if strings.Join(targets, ",") != "a.example,b.example,-weird-host" || f.protocol != "udp" || !f.once {
		t.Errorf("targets %v protocol %q once %v", targets, f.protocol, f.once)
	}
}

func TestBuildConfigFlagsOverrideFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "agent.yaml")
	yml := `
agent_id: from-file
interval: 5s
spool: {dir: /var/lib/hopcorder}
targets:
  - host: a.example
  - host: b.example
    interval: 20s
outputs: {jsonl: {path: /var/log/x.jsonl}}
`
	if err := os.WriteFile(p, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, f, targets := parseAgentFlags(t, "-config", p, "-interval", "2s", "-jsonl", "-", "-paris=false")
	cfg, err := buildConfig(fs, f, targets)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentID != "from-file" || cfg.Outputs.JSONL.Path != "-" || *cfg.Paris {
		t.Errorf("cfg %+v", cfg)
	}
	if cfg.Targets[0].Interval.D() != 2*time.Second || cfg.Targets[1].Interval.D() != 20*time.Second {
		t.Errorf("intervals %v %v: flag must override the global, not per-target settings",
			cfg.Targets[0].Interval.D(), cfg.Targets[1].Interval.D())
	}

	fs, f, targets = parseAgentFlags(t, "-config", p, "x.example", "-once", "-af", "6", "-protocol", "udp")
	cfg, err = buildConfig(fs, f, targets)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].Host != "x.example" || cfg.Targets[0].AF != 6 ||
		cfg.Targets[0].Protocol != "udp" || cfg.Targets[0].Port != 33434 || cfg.Targets[0].FlowID != 33434 {
		t.Errorf("positional targets: %+v", cfg.Targets)
	}

	// --flow-id sets the default flow_id, e.g. for --once next to the daemon.
	fs, f, targets = parseAgentFlags(t, "x.example", "-once", "-flow-id", "40000")
	if cfg, err = buildConfig(fs, f, targets); err != nil {
		t.Fatal(err)
	}
	if cfg.Targets[0].FlowID != 40000 {
		t.Errorf("flow_id %d, want 40000 from --flow-id", cfg.Targets[0].FlowID)
	}
}
