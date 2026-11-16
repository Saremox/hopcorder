package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/saremox/hopcorder/internal/config"
	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
	"github.com/saremox/hopcorder/internal/spool"
)

// mockIngest is a stand-in for the M2 server that can simulate a dead network
// (connections reset) or an unhealthy server (503).
type mockIngest struct {
	mode atomic.Value // "up", "reset", "503"

	mu       sync.Mutex
	received map[uint64]int // seq -> times received
	agents   map[string]bool
	invalid  []string
	posts    int
}

func newMockIngest() *mockIngest {
	m := &mockIngest{received: map[uint64]int{}, agents: map[string]bool{}}
	m.mode.Store("up")
	return m
}

func (m *mockIngest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch m.mode.Load() {
	case "reset":
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0) // RST, like a vanished peer
				}
				_ = c.Close()
				return
			}
		}
		panic(http.ErrAbortHandler)
	case "503":
		http.Error(w, "down for maintenance", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Path != "/api/v1/ingest" || r.Header.Get("Content-Encoding") != "zstd" ||
		r.Header.Get("Authorization") != "Bearer t0ken" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	dec, err := zstd.NewReader(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer dec.Close()
	body, err := io.ReadAll(dec)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.posts++
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var rec record.Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			m.invalid = append(m.invalid, err.Error())
			continue
		}
		if err := rec.Validate(); err != nil {
			m.invalid = append(m.invalid, err.Error())
		}
		m.agents[rec.AgentID] = true
		m.received[rec.Seq]++
	}
	w.WriteHeader(http.StatusAccepted)
}

func (m *mockIngest) snapshot() (unique int, dupes int, maxSeq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for seq, n := range m.received {
		unique++
		dupes += n - 1
		maxSeq = max(maxSeq, seq)
	}
	return
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func daemonConfig(t *testing.T, spoolDir, ingestURL, metricsAddr, tokenFile string) *config.Config {
	t.Helper()
	yml := fmt.Sprintf(`
agent_id: e2e
interval: 100ms
timeout: 50ms
max_ttl: 6
spool: {dir: %q}
targets:
  - host: 192.0.2.1
  - host: 192.0.2.2
  - host: 192.0.2.3
    protocol: udp
outputs:
  http:
    url: %q
    bearer_token_file: %q
    batch_max_records: 20
    flush_interval: 100ms
    retry_min: 20ms
    retry_max: 150ms
    timeout: 1s
  metrics:
    listen: %q
`, spoolDir, ingestURL, tokenFile, metricsAddr)
	cfg, err := config.Parse([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Finalize(config.FinalizeOptions{}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func fakeNetwork() *probe.Fake {
	f := probe.NewFake()
	for i := 1; i <= 3; i++ {
		dst := netip.AddrFrom4([4]byte{192, 0, 2, byte(i)})
		f.Set(dst, probe.Scenario{
			Hops: []probe.FakeHop{
				{IPs: []netip.Addr{netip.MustParseAddr("10.0.0.1")}, RTT: time.Millisecond},
				{IPs: []netip.Addr{netip.MustParseAddr("198.18.120.1")}, RTT: 5 * time.Millisecond, ReplyEvery: 3},
			},
			Reachable: true, DestRTT: 20 * time.Millisecond,
		})
	}
	return f
}

func startDaemon(t *testing.T, cfg *config.Config, f *probe.Fake) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	logger := slog.New(slog.DiscardHandler)
	go func() { done <- runDaemon(ctx, cfg, f, logger) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runDaemon: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("daemon did not shut down")
		}
	}
}

func getMetrics(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// TestOutageAndRecovery is the M1 acceptance scenario against a mock ingest
// endpoint: the network dies mid-run (connections reset), the server then
// answers 503, the agent is restarted during the outage, and once the
// endpoint recovers every spooled record arrives, with no gaps and no
// duplicates after dedup on (agent_id, seq).
func TestOutageAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end test")
	}
	ingest := newMockIngest()
	srv := httptest.NewServer(ingest)
	defer srv.Close()
	dir := t.TempDir()
	tokenFile := dir + "/token"
	writeTestFile(t, tokenFile, "t0ken\n")
	metricsAddr := freeAddr(t)
	cfg := daemonConfig(t, dir+"/spool", srv.URL, metricsAddr, tokenFile)

	stop := startDaemon(t, cfg, fakeNetwork())
	time.Sleep(400 * time.Millisecond)
	if u, _, _ := ingest.snapshot(); u == 0 {
		t.Fatal("nothing delivered while the endpoint was up")
	}

	ingest.mode.Store("reset") // the network dies
	time.Sleep(600 * time.Millisecond)
	ingest.mode.Store("503") // reachable again, but the server is unhealthy
	time.Sleep(300 * time.Millisecond)
	m := getMetrics(t, metricsAddr)
	if !strings.Contains(m, `hopcorder_output_errors_total{agent_id="e2e",output="http"}`) ||
		strings.Contains(m, `hopcorder_output_errors_total{agent_id="e2e",output="http"} 0`) {
		t.Errorf("output errors not counted:\n%s", grep(m, "output_"))
	}
	if !strings.Contains(m, `hopcorder_cycles_total{agent_id="e2e",protocol="icmp",result="reached",target="192.0.2.1"}`) {
		t.Errorf("cycle metrics missing:\n%s", grep(m, "cycles_total"))
	}
	shutdownGrace = 200 * time.Millisecond // the endpoint is down; do not wait long
	stop()                                 // agent restarted during the outage

	// Records produced so far must all be in the spool or delivered.
	ingest.mode.Store("up")
	stop = startDaemon(t, cfg, fakeNetwork())
	time.Sleep(500 * time.Millisecond)
	shutdownGrace = 5 * time.Second // now drain everything before exiting
	stop()

	sp, err := spool.Open(spool.Options{Dir: dir + "/spool", MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"http"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	st := sp.Stats()
	unique, dupes, maxSeq := ingest.snapshot()
	t.Logf("spool last seq %d, delivered %d unique records (%d duplicates) in %d posts", st.LastSeq, unique, dupes, ingest.posts)
	if st.LastSeq < 30 {
		t.Fatalf("only %d records produced", st.LastSeq)
	}
	if p := sp.Pending("http"); p != 0 {
		t.Errorf("%d records still pending after recovery", p)
	}
	if maxSeq != st.LastSeq || uint64(unique) != st.LastSeq {
		t.Errorf("delivered %d unique records up to seq %d; spool assigned %d", unique, maxSeq, st.LastSeq)
	}
	for seq := uint64(1); seq <= st.LastSeq; seq++ {
		if ingest.received[seq] == 0 {
			t.Errorf("seq %d never delivered", seq)
		}
	}
	if len(ingest.invalid) > 0 {
		t.Errorf("invalid records: %v", ingest.invalid[:min(5, len(ingest.invalid))])
	}
	if len(ingest.agents) != 1 || !ingest.agents["e2e"] {
		t.Errorf("agents %v", ingest.agents)
	}
	if st.Dropped != 0 {
		t.Errorf("spool dropped %d records", st.Dropped)
	}
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// With the default 10s flush_interval, records produced shortly before
// SIGTERM must still be shipped during the shutdown grace period.
func TestShutdownShipsPartialBatch(t *testing.T) {
	ingest := newMockIngest()
	srv := httptest.NewServer(ingest)
	defer srv.Close()
	dir := t.TempDir()
	tokenFile := dir + "/token"
	writeTestFile(t, tokenFile, "t0ken")
	cfg := daemonConfig(t, dir+"/spool", srv.URL, freeAddr(t), tokenFile)
	cfg.Outputs.HTTP.FlushInterval = config.Duration(config.DefaultHTTPFlushInterval)
	shutdownGrace = 3 * time.Second

	stop := startDaemon(t, cfg, fakeNetwork())
	time.Sleep(500 * time.Millisecond)
	start := time.Now()
	stop()
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("shutdown took %v; it should end as soon as the spool is drained", d)
	}
	sp, err := spool.Open(spool.Options{Dir: dir + "/spool", MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"http"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	unique, _, _ := ingest.snapshot()
	if p := sp.Pending("http"); p != 0 || unique == 0 || uint64(unique) != sp.Stats().LastSeq {
		t.Errorf("pending %d, delivered %d of %d", p, unique, sp.Stats().LastSeq)
	}
}

func readRecords(t *testing.T, path string) []record.Record {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []record.Record
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l == "" {
			continue
		}
		var r record.Record
		if err := json.Unmarshal([]byte(l), &r); err != nil {
			t.Fatalf("bad line %q: %v", l, err)
		}
		out = append(out, r)
	}
	return out
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReloadOnSIGHUP(t *testing.T) {
	dir := t.TempDir()
	cfgPath := dir + "/agent.yaml"
	writeCfg := func(target, out string) {
		writeTestFile(t, cfgPath, fmt.Sprintf(`
agent_id: reload
interval: 100ms
timeout: 50ms
spool: {dir: %q}
targets: [{host: %s}]
outputs: {jsonl: {path: %q}}
`, dir+"/spool", target, out))
	}
	load := func() (*config.Config, error) {
		c, err := config.Load(cfgPath)
		if err != nil {
			return nil, err
		}
		return c, c.Finalize(config.FinalizeOptions{})
	}
	writeCfg("192.0.2.1", dir+"/a.jsonl")
	cfg, err := load()
	if err != nil {
		t.Fatal(err)
	}
	shutdownGrace = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	reload := make(chan os.Signal, 1)
	done := make(chan error, 1)
	quiet := func(*config.Config) *slog.Logger { return slog.New(slog.DiscardHandler) }
	go func() { done <- superviseDaemon(ctx, cfg, fakeNetwork(), reload, load, quiet) }()

	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/a.jsonl")) >= 3 }, "records before reload")

	// An invalid configuration is rejected and the agent keeps running.
	writeTestFile(t, cfgPath, "bogus: true\n")
	reload <- syscall.SIGHUP
	n := len(readRecords(t, dir+"/a.jsonl"))
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/a.jsonl")) > n+2 }, "records after rejected reload")

	// A valid one switches target and output file; seq continues.
	writeCfg("192.0.2.2", dir+"/b.jsonl")
	reload <- syscall.SIGHUP
	waitFor(t, 5*time.Second, func() bool { return len(readRecords(t, dir+"/b.jsonl")) >= 3 }, "records after reload")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not stop")
	}

	a, b := readRecords(t, dir+"/a.jsonl"), readRecords(t, dir+"/b.jsonl")
	var maxA uint64
	for _, r := range a {
		if r.Target != "192.0.2.1" {
			t.Errorf("a.jsonl has target %s", r.Target)
		}
		maxA = max(maxA, r.Seq)
	}
	for _, r := range b {
		if r.Target != "192.0.2.2" {
			t.Errorf("b.jsonl has target %s", r.Target)
		}
		if r.Seq <= maxA {
			t.Errorf("seq %d after reload does not continue past %d", r.Seq, maxA)
		}
	}
}

func TestReloadRevertsWhenNewConfigCannotStart(t *testing.T) {
	dir := t.TempDir()
	good := daemonConfigJSONL(t, dir+"/spool", dir+"/out.jsonl", "")
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	bad := daemonConfigJSONL(t, dir+"/spool", dir+"/out.jsonl", busy.Addr().String()) // metrics port taken
	shutdownGrace = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reload := make(chan os.Signal, 1)
	done := make(chan error, 1)
	quiet := func(*config.Config) *slog.Logger { return slog.New(slog.DiscardHandler) }
	go func() {
		done <- superviseDaemon(ctx, good, fakeNetwork(), reload, func() (*config.Config, error) { return bad, nil }, quiet)
	}()
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/out.jsonl")) >= 2 }, "records")
	reload <- syscall.SIGHUP
	time.Sleep(1500 * time.Millisecond)
	n := len(readRecords(t, dir+"/out.jsonl"))
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/out.jsonl")) > n+2 }, "records after revert")
	select {
	case err := <-done:
		t.Fatalf("supervisor exited: %v", err)
	default:
	}
	// Stop it and wait, so no daemon outlives the test.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("supervisor: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func daemonConfigJSONL(t *testing.T, spoolDir, out, metrics string) *config.Config {
	t.Helper()
	yml := fmt.Sprintf("agent_id: r\ninterval: 100ms\ntimeout: 50ms\nspool: {dir: %q}\ntargets: [{host: 192.0.2.1}]\noutputs:\n  jsonl: {path: %q}\n", spoolDir, out)
	if metrics != "" {
		yml += fmt.Sprintf("  metrics: {listen: %q}\n", metrics)
	}
	c, err := config.Parse([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(config.FinalizeOptions{}); err != nil {
		t.Fatal(err)
	}
	return c
}
