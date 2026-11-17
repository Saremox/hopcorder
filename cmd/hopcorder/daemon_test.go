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
	"strconv"
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

// startDaemon runs the agent daemon until stop is called or the test ends:
// stop is also a cleanup, so a test that fails early cannot leave a daemon
// running into the next test (which would race on package state such as
// shutdownGrace).
func startDaemon(t *testing.T, cfg *config.Config, f *probe.Fake) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	logger := slog.New(slog.DiscardHandler)
	go func() { done <- runDaemon(ctx, cfg, f, logger, daemonEnv{}) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("runDaemon: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("daemon did not shut down")
			}
		})
	}
	t.Cleanup(stop)
	return stop
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
	base := newSpool(t, dir+"/spool")

	stop := startDaemon(t, cfg, fakeNetwork())
	waitFor(t, 10*time.Second, func() bool {
		u, _, _ := ingest.snapshot()
		return u > 0
	}, "a delivery while the endpoint was up")

	// The outage lasts a number of the agent's cycles, not a fixed time, so
	// a slow runner cannot leave it with too few records to check.
	before, _, _ := ingest.snapshot()
	c0 := cycles(getMetrics(t, metricsAddr))
	ingest.mode.Store("reset") // the network dies
	waitFor(t, 10*time.Second, func() bool { return cycles(getMetrics(t, metricsAddr)) >= c0+5 },
		"cycles while the network is down")
	ingest.mode.Store("503") // reachable again, but the server is unhealthy
	waitFor(t, 10*time.Second, func() bool { return cycles(getMetrics(t, metricsAddr)) >= c0+8 },
		"cycles while the server answers 503")
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
	produced := st.LastSeq - base
	t.Logf("spool produced %d records, delivered %d unique (%d duplicates) in %d posts", produced, unique, dupes, ingest.posts)
	// At least the 8 outage cycles beyond what was delivered before it
	// (allowing for the last cycles being counted before they are spooled).
	if produced < uint64(before)+6 {
		t.Fatalf("only %d records produced, %d delivered before the outage", produced, before)
	}
	if p := sp.Pending("http"); p != 0 {
		t.Errorf("%d records still pending after recovery", p)
	}
	if maxSeq != st.LastSeq || uint64(unique) != produced {
		t.Errorf("delivered %d unique records up to seq %d; spool assigned %d up to %d", unique, maxSeq, produced, st.LastSeq)
	}
	for seq := base + 1; seq <= st.LastSeq; seq++ {
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

// cycles sums hopcorder_cycles_total over its series in a metrics page.
func cycles(page string) (n float64) {
	for _, l := range strings.Split(page, "\n") {
		if f := strings.Fields(l); len(f) == 2 && strings.HasPrefix(f[0], "hopcorder_cycles_total{") {
			v, _ := strconv.ParseFloat(f[1], 64)
			n += v
		}
	}
	return n
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
	base := newSpool(t, dir+"/spool")

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
	if p := sp.Pending("http"); p != 0 || unique == 0 || uint64(unique) != sp.Stats().LastSeq-base {
		t.Errorf("pending %d, delivered %d of %d", p, unique, sp.Stats().LastSeq-base)
	}
}

// newSpool creates the spool an http-only daemon will use and returns the
// seq before its first record: a new spool's seqs start at the clock.
func newSpool(t *testing.T, dir string) uint64 {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"http"}})
	if err != nil {
		t.Fatal(err)
	}
	base := sp.Stats().LastSeq
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}
	return base
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

// A metrics path the endpoint cannot serve is a configuration error, so a
// reload to one keeps the running configuration. Each of these passed
// validation and made the reload panic in http.ServeMux.Handle (round
// four): the agent exited with status 2, and again on every restart.
func TestReloadRefusesBadMetricsPath(t *testing.T) {
	dir := t.TempDir()
	cfgPath, out, addr := dir+"/agent.yaml", dir+"/out.jsonl", freeAddr(t)
	write := func(path string) {
		writeTestFile(t, cfgPath, fmt.Sprintf("agent_id: r\ninterval: 100ms\ntimeout: 50ms\nspool: {dir: %q}\n"+
			"targets: [{host: 192.0.2.1}]\noutputs:\n  jsonl: {path: %q}\n  metrics: {listen: %q, path: %q}\n",
			dir+"/spool", out, addr, path))
	}
	loaded := make(chan error, 1)
	load := func() (*config.Config, error) {
		c, err := config.Load(cfgPath)
		if err == nil {
			err = c.Finalize(config.FinalizeOptions{})
		}
		select {
		case loaded <- err:
		default:
		}
		return c, err
	}
	write("/metrics")
	cfg, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
	shutdownGrace = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reload := make(chan os.Signal, 1)
	done := make(chan error, 1)
	quiet := func(*config.Config) *slog.Logger { return slog.New(slog.DiscardHandler) }
	go func() { done <- superviseDaemon(ctx, cfg, fakeNetwork(), reload, load, quiet) }()
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, out)) >= 2 }, "records")

	for _, p := range []string{"/metrics x", "/{bad", "/a{b}", "/ ", "/x/{a}/{a}", "/{x...}/y"} {
		write(p)
		reload <- syscall.SIGHUP
		select {
		case err := <-loaded:
			if err == nil || !strings.Contains(err.Error(), "outputs.metrics.path") {
				t.Errorf("reload to %q: %v", p, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("reload to %q: not loaded", p)
		}
		n := len(readRecords(t, out))
		waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, out)) > n }, "records after the reload to "+p)
		running(t, done)
	}
	write("/m2") // a valid one is taken
	reload <- syscall.SIGHUP
	waitFor(t, 5*time.Second, func() bool {
		resp, err := http.Get("http://" + addr + "/m2")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "metrics at the valid path")
	running(t, done)
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

// supervised runs superviseDaemon with cfg, reloading to whatever next
// returns, until the returned stop is called (it also is a cleanup).
func supervised(t *testing.T, cfg *config.Config, next func() *config.Config) (reload chan os.Signal, done chan error, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	reload = make(chan os.Signal, 1)
	done = make(chan error, 1)
	quiet := func(*config.Config) *slog.Logger { return slog.New(slog.DiscardHandler) }
	go func() {
		done <- superviseDaemon(ctx, cfg, fakeNetwork(), reload, func() (*config.Config, error) { return next(), nil }, quiet)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("supervisor: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("supervisor did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return reload, done, stop
}

func running(t *testing.T, done chan error) {
	t.Helper()
	select {
	case err := <-done:
		done <- err // for stop
		t.Fatalf("supervisor exited: %v", err)
	default:
	}
}

// A reload keeps the metrics listener when its address stays the same, so a
// local process polling bind() on the port finds no moment to take it.
// Before, the listener was closed for the reload: the squatter won, the new
// configuration and then the previous one failed to bind, and the agent
// exited (and served the squatter's numbers on its port).
func TestReloadKeepsTheMetricsPort(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)
	a := daemonConfigJSONL(t, dir+"/spool", dir+"/a.jsonl", addr)
	b := daemonConfigJSONL(t, dir+"/spool", dir+"/b.jsonl", addr)
	shutdownGrace = time.Second
	reload, done, stop := supervised(t, a, func() *config.Config { return b })
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/a.jsonl")) >= 2 }, "records")

	var squatter sync.WaitGroup
	quit := make(chan struct{})
	var squatted net.Listener
	squatter.Go(func() {
		for {
			select {
			case <-quit:
				return
			default:
			}
			if ln, err := net.Listen("tcp", addr); err == nil {
				squatted = ln
				return
			}
		}
	})
	reload <- syscall.SIGHUP
	waitFor(t, 5*time.Second, func() bool { return len(readRecords(t, dir+"/b.jsonl")) >= 2 }, "records after the reload")
	close(quit)
	squatter.Wait()
	if squatted != nil {
		_ = squatted.Close()
		t.Fatal("another process bound the metrics port during the reload")
	}
	if m := getMetrics(t, addr); !strings.Contains(m, `hopcorder_build_info{agent_id="r"`) {
		t.Errorf("the port does not serve the agent's metrics:\n%.300s", m)
	}
	running(t, done)
	stop()
}

// When a new configuration fails to start and the previous one then fails
// too (here its JSONL directory went away), the agent keeps running: it
// retries the previous configuration until it starts, holding the metrics
// port meanwhile and answering 503. Before, the supervisor returned and the
// agent exited.
func TestReloadRetriesWhenTheRevertFails(t *testing.T) {
	defer func(d time.Duration) { restartBackoffMin = d }(restartBackoffMin)
	restartBackoffMin = 20 * time.Millisecond
	dir := t.TempDir()
	if err := os.Mkdir(dir+"/a", 0o750); err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)
	good := daemonConfigJSONL(t, dir+"/spool", dir+"/a/out.jsonl", addr)
	bad := daemonConfigJSONL(t, dir+"/spool", dir+"/missing/out.jsonl", addr)
	shutdownGrace = time.Second
	reload, done, stop := supervised(t, good, func() *config.Config { return bad })
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/a/out.jsonl")) >= 2 }, "records")

	if err := os.Rename(dir+"/a", dir+"/a.old"); err != nil {
		t.Fatal(err)
	}
	reload <- syscall.SIGHUP
	time.Sleep(time.Second) // the reload, the revert and several retries fail
	running(t, done)
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("metrics port while retrying: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("metrics while no configuration runs: status %d, want 503", resp.StatusCode)
	}
	if ln, err := net.Listen("tcp", addr); err == nil {
		_ = ln.Close()
		t.Error("the metrics port was free while retrying")
	}

	if err := os.Mkdir(dir+"/a", 0o750); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(readRecords(t, dir+"/a/out.jsonl")) >= 2 },
		"records once the previous configuration can start again")
	if m := getMetrics(t, addr); !strings.Contains(m, `hopcorder_build_info{agent_id="r"`) {
		t.Errorf("metrics after the retry:\n%.300s", m)
	}
	running(t, done)
	stop()
}

// A reload to another metrics address binds the new one before releasing
// the old, which is free once the new configuration runs.
func TestReloadMovesTheMetricsAddress(t *testing.T) {
	dir := t.TempDir()
	addrA, addrB := freeAddr(t), freeAddr(t)
	a := daemonConfigJSONL(t, dir+"/spool", dir+"/out.jsonl", addrA)
	b := daemonConfigJSONL(t, dir+"/spool", dir+"/out.jsonl", addrB)
	b.AgentID = "moved"
	shutdownGrace = time.Second
	reload, done, stop := supervised(t, a, func() *config.Config { return b })
	waitFor(t, 3*time.Second, func() bool { return len(readRecords(t, dir+"/out.jsonl")) >= 2 }, "records")
	reload <- syscall.SIGHUP
	waitFor(t, 5*time.Second, func() bool {
		resp, err := http.Get("http://" + addrB + "/metrics")
		if err != nil {
			return false
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return strings.Contains(string(body), `hopcorder_build_info{agent_id="moved"`)
	}, "metrics on the new address")
	ln, err := net.Listen("tcp", addrA)
	if err != nil {
		t.Errorf("the old metrics address is still bound: %v", err)
	} else {
		_ = ln.Close()
	}
	running(t, done)
	stop()
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

// The daemon holds its targets' flows.
func TestDaemonHoldsFlows(t *testing.T) {
	dir := t.TempDir()
	cfg := daemonConfigJSONL(t, dir+"/spool", dir+"/out.jsonl", "")
	cfg.Targets = append(cfg.Targets, config.Target{Host: "192.0.2.2", Protocol: "tcp", FlowID: 999})
	if err := cfg.Finalize(config.FinalizeOptions{}); err != nil {
		t.Fatal(err)
	}
	f := fakeNetwork()
	stop := startDaemon(t, cfg, f)
	waitFor(t, 3*time.Second, func() bool { return len(f.Held()) == 2 }, "held flows")
	stop()
	want := []probe.Flow{{Protocol: probe.ICMP, ID: 33434}, {Protocol: probe.TCP, ID: 999}}
	if got := f.Held(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("held %v, want %v", got, want)
	}
}

// One process on the ICMP flow's identifier (any local user may open a ping
// socket where ping_group_range allows it) stops only the ICMP targets of
// that flow, visibly, and they come back once it is free, without a
// reload. It used to fail every start, UDP and TCP targets included.
func TestDaemonFlowInUseStopsOnlyItsTargets(t *testing.T) {
	dir := t.TempDir()
	metricsAddr := freeAddr(t)
	cfg := daemonConfigJSONL(t, dir+"/spool", dir+"/out.jsonl", metricsAddr)
	cfg.Targets = append(cfg.Targets, config.Target{Host: "192.0.2.2", Protocol: "tcp", FlowID: 999},
		config.Target{Host: "192.0.2.3", Protocol: "udp"}) // the UDP flow has the ICMP flow's id
	if err := cfg.Finalize(config.FinalizeOptions{}); err != nil {
		t.Fatal(err)
	}
	f := fakeNetwork()
	icmp := probe.Flow{Protocol: probe.ICMP, ID: 33434}
	f.SetHoldErr(icmp, fmt.Errorf("icmp flow_id 33434: ICMP echo identifier 33434 is %w", probe.ErrFlowInUse))
	stop := startDaemon(t, cfg, f)
	byTarget := func() map[string]int {
		n := map[string]int{}
		for _, r := range readRecords(t, dir+"/out.jsonl") {
			n[r.Target]++
		}
		return n
	}
	waitFor(t, 5*time.Second, func() bool { n := byTarget(); return n["192.0.2.2"] >= 3 && n["192.0.2.3"] >= 3 },
		"records of the TCP and UDP targets")
	m := getMetrics(t, metricsAddr)
	for _, want := range []string{
		`hopcorder_flow_in_use{agent_id="r",protocol="icmp",target="192.0.2.1"} 1`,
		`hopcorder_flow_in_use{agent_id="r",protocol="tcp",target="192.0.2.2"} 0`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s:\n%s", want, grep(m, "flow_in_use"))
		}
	}
	if strings.Contains(m, `hopcorder_cycle_errors_total{agent_id="r",protocol="icmp",target="192.0.2.1"} 0`) {
		t.Errorf("failed cycles not counted:\n%s", grep(m, "cycle_errors"))
	}
	if n := byTarget()["192.0.2.1"]; n != 0 {
		t.Errorf("%d records of the ICMP target although its identifier is not held", n)
	}
	for _, r := range f.Requests() {
		if r.Protocol == probe.ICMP {
			t.Fatalf("ICMP probes sent although the identifier is not held: %+v", r)
		}
	}

	f.SetHoldErr(icmp, nil) // the other process exits
	waitFor(t, 5*time.Second, func() bool { return byTarget()["192.0.2.1"] >= 2 }, "records of the ICMP target")
	if m := getMetrics(t, metricsAddr); !strings.Contains(m,
		`hopcorder_flow_in_use{agent_id="r",protocol="icmp",target="192.0.2.1"} 0`) {
		t.Errorf("flow still shown in use:\n%s", grep(m, "flow_in_use"))
	}
	stop()
}
