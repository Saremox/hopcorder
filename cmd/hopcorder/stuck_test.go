//go:build linux

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/config"
)

// A JSONL write that blocks for good (a FIFO for a log shipper that is not
// reading, stdout under a blocking log driver, a hung NFS mount) used to
// hang a reload and the shutdown: after the agent stopped probing, the
// supervisor waited for the drain without a bound, the metrics endpoint kept
// answering 200 with frozen numbers, and SIGTERM was ignored until systemd
// killed the agent (round four). These tests need no root: a FIFO in a
// temporary directory, and a pipe in place of stdout.

// fill writes to the non-blocking fd until its pipe is full.
func fill(t *testing.T, fd int) {
	t.Helper()
	buf := make([]byte, 4096)
	for {
		if _, err := syscall.Write(fd, buf); err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				return
			}
			t.Fatal(err)
		}
	}
}

// fullFIFO makes a FIFO at path whose buffer is full and which nobody
// reads: every write to it blocks. (It is held open for reading here, so
// the agent's open does not block.)
func fullFIFO(t *testing.T, path string) {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	fill(t, fd)
}

// blockedStdout replaces os.Stdout, for the test, with a full pipe in
// blocking mode, which takes no write deadlines: a write to it blocks until
// the cleanup drains the pipe.
func blockedStdout(t *testing.T) {
	t.Helper()
	fds := make([]int, 2)
	if err := syscall.Pipe2(fds, syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
		t.Fatal(err)
	}
	fill(t, fds[1])
	if err := syscall.SetNonblock(fds[1], false); err != nil {
		t.Fatal(err)
	}
	r, w := os.NewFile(uintptr(fds[0]), "stdout reader"), os.NewFile(uintptr(fds[1]), "blocked stdout")
	if w.SetWriteDeadline(time.Time{}) == nil {
		t.Fatal("the blocking pipe takes deadlines")
	}
	saved := os.Stdout
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = saved
		go func() { // let the writes left behind finish
			_, _ = io.Copy(io.Discard, r)
			_ = r.Close()
		}()
		_ = w.Close()
	})
}

// tryMetrics returns the metrics page, or "" while it cannot be had.
func tryMetrics(addr string) string {
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func metricsStatus(addr string) int {
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

var abandonedRE = regexp.MustCompile(`(?m)^hopcorder_output_abandoned_total\{agent_id="r",output="jsonl"\} (\d+)$`)

func abandonedJSONL(t *testing.T, addr string) int {
	t.Helper()
	m := abandonedRE.FindStringSubmatch(getMetrics(t, addr))
	if m == nil {
		t.Fatal("no hopcorder_output_abandoned_total for jsonl")
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// reloadWithBlockedJSONL runs a configuration whose JSONL output blocks,
// reloads to one writing to a file, and checks that the reload completes,
// that metrics answer 503 in between, and how many drains it left behind.
func reloadWithBlockedJSONL(t *testing.T, blocked string) (abandoned int) {
	t.Helper()
	dir := t.TempDir()
	addr := freeAddr(t)
	a := daemonConfigJSONL(t, dir+"/spool", blocked, addr)
	b := daemonConfigJSONL(t, dir+"/spool", dir+"/b.jsonl", addr)
	shutdownGrace = 500 * time.Millisecond
	reload, done, stop := supervised(t, a, func() *config.Config { return b })
	waitFor(t, 3*time.Second, func() bool { return cycles(tryMetrics(addr)) >= 3 }, "cycles")
	before := abandonedJSONL(t, addr)

	reload <- syscall.SIGHUP
	saw503 := false
	waitFor(t, 4*shutdownGrace+3*time.Second, func() bool {
		saw503 = saw503 || metricsStatus(addr) == http.StatusServiceUnavailable
		return len(readRecords(t, dir+"/b.jsonl")) >= 2
	}, "records of the new configuration")
	if !saw503 {
		t.Error("metrics never answered 503 while the old configuration stopped")
	}
	abandoned = abandonedJSONL(t, addr) - before
	running(t, done)
	stop()
	return abandoned
}

// A FIFO takes write deadlines: the write gives up when the configuration
// stops, and nothing is left behind.
func TestReloadWithJSONLOnAFullFIFO(t *testing.T) {
	fifo := t.TempDir() + "/fifo"
	fullFIFO(t, fifo)
	if n := reloadWithBlockedJSONL(t, fifo); n != 0 {
		t.Errorf("%d drains left behind; the FIFO's write deadline should have stopped it", n)
	}
}

// A blocking stdout takes no deadlines: its drain is left behind, counted.
func TestReloadWithJSONLOnABlockedStdout(t *testing.T) {
	blockedStdout(t)
	if n := reloadWithBlockedJSONL(t, "-"); n != 1 {
		t.Errorf("%d drains left behind, want 1", n)
	}
}

// shutdownWithin runs cfg, then stops it as SIGTERM does and fails unless
// the supervisor returns within d.
func shutdownWithin(t *testing.T, cfg *config.Config, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	quiet := func(*config.Config) *slog.Logger { return slog.New(slog.DiscardHandler) }
	go func() { done <- superviseDaemon(ctx, cfg, fakeNetwork(), make(chan os.Signal), nil, quiet) }()
	addr := cfg.Outputs.Metrics.Listen
	waitFor(t, 3*time.Second, func() bool { return cycles(tryMetrics(addr)) >= 3 }, "cycles")
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("supervisor: %v", err)
		}
		t.Logf("stopped in %v", time.Since(start).Round(time.Millisecond))
	case <-time.After(d):
		t.Fatalf("the agent did not stop within %v of SIGTERM", d)
	}
}

func TestShutdownWithJSONLOnAFullFIFO(t *testing.T) {
	dir := t.TempDir()
	fullFIFO(t, dir+"/fifo")
	shutdownGrace = 500 * time.Millisecond
	shutdownWithin(t, daemonConfigJSONL(t, dir+"/spool", dir+"/fifo", freeAddr(t)), 2*shutdownGrace+2*time.Second)
}

func TestShutdownWithJSONLOnABlockedStdout(t *testing.T) {
	blockedStdout(t)
	dir := t.TempDir()
	shutdownGrace = 500 * time.Millisecond
	shutdownWithin(t, daemonConfigJSONL(t, dir+"/spool", "-", freeAddr(t)), 2*shutdownGrace+2*time.Second)
}
