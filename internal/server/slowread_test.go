package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// smallSendBuffers gives accepted connections small kernel send buffers, so
// a response stalls after some KiB on a client that does not read, rather
// than after megabytes.
type smallSendBuffers struct{ net.Listener }

func (l smallSendBuffers) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetWriteBuffer(16 << 10)
	}
	return c, err
}

// bigRows answers queries matching match with n rows of about size bytes
// of JSON each, and other queries with one small row.
func bigRows(match string, n, size int) Store {
	pad := strings.Repeat("x", size)
	return funcStore{fakeStore: newFakeStore(), q: func(ctx context.Context, query string, row func(json.RawMessage) error) error {
		rows := 1
		if strings.Contains(query, match) {
			rows = n
		}
		for i := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := row(json.RawMessage(fmt.Sprintf(`{"ttl":%d,"pad":%q}`, i, pad))); err != nil {
				return err
			}
		}
		return nil
	}}
}

func slowReadServer(t *testing.T, st Store, progress time.Duration) (*Server, *httptest.Server) {
	t.Helper()
	s := New(Options{Store: st, Logger: quietLogger(), MaxConcurrentQueries: 1})
	s.writeProgress = progress
	s.SetReady()
	ts := httptest.NewUnstartedServer(s.Handler())
	ts.Listener = smallSendBuffers{ts.Listener}
	ts.Start()
	t.Cleanup(ts.Close)
	return s, ts
}

// dialSmall connects with a small receive buffer (which also stops the
// kernel from growing it), so what the client does not read stays on the
// server's side.
func dialSmall(t *testing.T, ts *httptest.Server) *net.TCPConn {
	t.Helper()
	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	tc := c.(*net.TCPConn)
	if err := tc.SetReadBuffer(16 << 10); err != nil {
		t.Fatal(err)
	}
	return tc
}

// A client that sends a read-API request and never reads the response
// gives its query slot up once a part of the response has waited
// writeProgress, instead of holding it for the server's WriteTimeout (with
// query.max_concurrent such clients kept the read API and the UI at 503).
func TestNonReadingClientLosesQuerySlot(t *testing.T) {
	s, ts := slowReadServer(t, bigRows("loop_events", 4000, 1000), 500*time.Millisecond)
	c := dialSmall(t, ts)
	if _, err := io.WriteString(c, "GET /api/v1/query/loops HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); len(s.querySem) == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the request never took its query slot")
		}
	}

	start := time.Now()
	resp, err := http.Get(ts.URL + "/api/v1/query/targets")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	// Without the deadline this waited queueWait (10 s) for the slot and got 503.
	if took := time.Since(start); resp.StatusCode != http.StatusOK || took > 5*time.Second {
		t.Errorf("a reader behind a client that does not read: %d after %s", resp.StatusCode, took.Round(time.Millisecond))
	}
}

// A client that reads slowly but steadily gets the whole response, even one
// that takes far longer than writeProgress, also when the server writes it
// at once (topology): each part has the deadline, not the response.
func TestSlowReaderGetsWholeResponse(t *testing.T) {
	_, ts := slowReadServer(t, bigRows("FROM hops", topologyMaxNodes, 1200), 500*time.Millisecond)
	c := dialSmall(t, ts)
	if _, err := io.WriteString(c, "GET /api/v1/query/topology?agent=lab&target=dc1.example HTTP/1.1\r\n"+
		"Host: localhost\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	// 16 KiB every 10 ms: about 1.6 MB/s, so the 2.4 MB response takes
	// 1.5 s and each 64 KiB part 40 ms.
	start := time.Now()
	resp, err := http.ReadResponse(bufio.NewReader(&throttled{r: c, n: 16 << 10, every: 10 * time.Millisecond}), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Nodes     []json.RawMessage `json:"nodes"`
		Truncated bool              `json:"truncated"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("after %s: %v", time.Since(start).Round(time.Millisecond), err)
	}
	if resp.StatusCode != http.StatusOK || len(body.Nodes) != topologyMaxNodes || body.Truncated {
		t.Errorf("%d: %d nodes, truncated %v", resp.StatusCode, len(body.Nodes), body.Truncated)
	}
	if took := time.Since(start); took < time.Second {
		t.Errorf("the response took %s, want more than writeProgress twice over (the test reads too fast)", took)
	}
}

// throttled reads at most n bytes per Read and sleeps every after each.
type throttled struct {
	r     io.Reader
	n     int
	every time.Duration
}

func (t *throttled) Read(b []byte) (int, error) {
	n, err := t.r.Read(b[:min(len(b), t.n)])
	time.Sleep(t.every)
	return n, err
}
