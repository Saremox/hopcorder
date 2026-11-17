package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/saremox/hopcorder/internal/config"
)

// socketPath is a unix socket path in a fresh directory, short enough for
// sun_path (t.TempDir names carry the test name).
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func listenFor(t *testing.T, listen string) (net.Listener, bool, error) {
	t.Helper()
	return listenServer(t.Context(), &config.ServerConfig{Listen: listen})
}

func TestListenUnixSocket(t *testing.T) {
	path := socketPath(t)
	ln, activated, err := listenFor(t, "unix:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if activated || ln.Addr().Network() != "unix" {
		t.Errorf("listener %s %s, activated %v", ln.Addr().Network(), ln.Addr(), activated)
	}
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Type() != os.ModeSocket || fi.Mode().Perm() != unixSocketMode {
		t.Errorf("socket mode %v, want socket %o", fi.Mode(), unixSocketMode)
	}

	// Something listens on it: not replaced.
	if _, _, err := listenFor(t, "unix:"+path); err == nil || !strings.Contains(err.Error(), "another process") {
		t.Errorf("second listener on a live socket: %v", err)
	}
	// A server that died left the file behind: replaced.
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = ln.Close()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket: %v", err)
	}
	ln, _, err = listenFor(t, "unix:"+path)
	if err != nil {
		t.Fatalf("stale socket not replaced: %v", err)
	}
	_ = ln.Close()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Errorf("socket left behind after a clean close: %v", err)
	}

	// Never removes what is not a socket.
	writeTestFile(t, path, "data")
	if _, _, err := listenFor(t, "unix:"+path); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Errorf("regular file at the socket path: %v", err)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "data" {
		t.Errorf("regular file changed: %q %v", b, err)
	}
}

// The server answers on a unix socket, health and metrics included, and
// checks Host headers there only when allowed_hosts lists names.
func TestServeOnUnixSocket(t *testing.T) {
	for _, tc := range []struct {
		allowed string
		refused bool // a query for a Host not listed gets 421
	}{
		{"", false},
		{"allowed_hosts: [hopcorder.example]", true},
	} {
		path := socketPath(t)
		cfg, err := config.ParseServer(fmt.Appendf(nil, `
listen: unix:%s
clickhouse: {url: "http://%s"}
enrich: {rdns: {enabled: false}, asn: {enabled: false}}
ui: false
%s
`, path, freeAddr(t), tc.allowed))
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.Finalize(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		ready := make(chan string, 1)
		served := make(chan error, 1)
		go func() { served <- serve(ctx, cfg, slog.New(slog.DiscardHandler), ready) }()
		select {
		case addr := <-ready:
			if addr != path {
				t.Errorf("ready on %q, want %q", addr, path)
			}
		case err := <-served:
			t.Fatalf("serve: %v", err)
		}
		client := &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		}}
		get := func(host, p string) (int, string) {
			t.Helper()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+host+p, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("GET %s: %v", p, err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			return resp.StatusCode, string(b)
		}
		if code, body := get("evil.example", "/healthz"); code != http.StatusOK || body != "ok\n" {
			t.Errorf("%s: /healthz %d %q", tc.allowed, code, body)
		}
		if code, body := get("evil.example", "/metrics"); code != http.StatusOK || !strings.Contains(body, "hopcorder_build_info") {
			t.Errorf("%s: /metrics %d", tc.allowed, code)
		}
		if code, _ := get("evil.example", "/api/v1/query/targets"); (code == http.StatusMisdirectedRequest) != tc.refused {
			t.Errorf("%q: query for an unlisted host: %d, refused %v", tc.allowed, code, tc.refused)
		}
		cancel()
		if err := <-served; err != nil {
			t.Errorf("shutdown: %v", err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("socket left behind after shutdown: %v", err)
		}
	}
}

// The socket from systemd: LISTEN_FDS sockets from fd 3, for the process in
// LISTEN_PID. The test passes a listening TCP socket as fd 3 to a child (a
// copy of this test binary), which serves one connection on it.
func TestSystemdSocketActivation(t *testing.T) {
	if os.Getenv("HOPCORDER_TEST_SYSTEMD_CHILD") == "1" {
		systemdChild(t)
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f, err := ln.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSystemdSocketActivation$", "-test.v")
	cmd.Env = append(os.Environ(), "HOPCORDER_TEST_SYSTEMD_CHILD=1", "LISTEN_FDS=1", "LISTEN_FDNAMES=hopcorder-server.socket")
	cmd.ExtraFiles = []*os.File{f} // fd 3
	var out strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	addr := ln.Addr().String()
	_ = ln.Close() // the child holds the socket now

	var d net.Dialer
	c, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial the activated socket: %v", err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	_ = c.Close()
	if werr := cmd.Wait(); werr != nil || err != nil {
		t.Fatalf("child: %v, read: %v\n%s", werr, err, out.String())
	}
	if want := "activated tcp " + addr + " hosts localhost,127.0.0.1,::1\n"; line != want {
		t.Errorf("child said %q, want %q\n%s", line, want, out.String())
	}
}

// systemdChild runs in the child of TestSystemdSocketActivation, where fd 3
// is the parent's socket. It sets LISTEN_PID itself, as systemd does after
// the fork.
func systemdChild(t *testing.T) {
	t.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
	cfg := &config.ServerConfig{Listen: config.ListenSystemd}
	if err := cfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	ln, activated, err := listenServer(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	for _, v := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		if os.Getenv(v) != "" {
			t.Errorf("%s still set", v)
		}
	}
	if !activated {
		t.Error("not reported as activated")
	}
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = fmt.Fprintf(c, "activated %s %s hosts %s\n", ln.Addr().Network(), ln.Addr(),
		strings.Join(listenerHosts(cfg, ln.Addr(), activated), ","))
}

// What systemd's variables make of each listen value, without touching a
// descriptor.
func TestSystemdListenRefusals(t *testing.T) {
	pid := strconv.Itoa(os.Getpid())
	for _, tc := range []struct {
		listen, pid, fds, want string
	}{
		// A socket unit is active, but the config listens elsewhere: its
		// connections would never be answered.
		{"127.0.0.1:0", pid, "1", "set listen: systemd"},
		{"systemd", pid, "2", "passed 2 sockets"},
		{"systemd", pid, "x", "not a count"},
		{"systemd", "", "", "passed no socket"},
		// Meant for another process (inherited).
		{"systemd", "1", "1", "passed no socket"},
	} {
		t.Setenv("LISTEN_PID", tc.pid)
		t.Setenv("LISTEN_FDS", tc.fds)
		ln, _, err := listenFor(t, tc.listen)
		if err == nil {
			_ = ln.Close()
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("listen %s, LISTEN_PID %q, LISTEN_FDS %q: %v, want %q", tc.listen, tc.pid, tc.fds, err, tc.want)
		}
	}
	// Inherited variables do not stop a TCP listener.
	t.Setenv("LISTEN_PID", "1")
	t.Setenv("LISTEN_FDS", "1")
	ln, activated, err := listenFor(t, "127.0.0.1:0")
	if err != nil || activated {
		t.Fatalf("tcp with another process's LISTEN_PID: %v, activated %v", err, activated)
	}
	_ = ln.Close()
}

func TestListenerHosts(t *testing.T) {
	local := []string{"localhost", "127.0.0.1", "::1"}
	sock := &net.UnixAddr{Name: "/run/hopcorder-server.sock", Net: "unix"}
	for _, tc := range []struct {
		listen    string
		allowed   []string
		addr      net.Addr
		activated bool
		want      []string
	}{
		// A unix socket: behind a proxy on the host, no check by default.
		{"unix:/run/hopcorder-server.sock", nil, sock, false, nil},
		{"systemd", nil, sock, true, nil},
		{"systemd", []string{"hopcorder.example"}, sock, true, []string{"hopcorder.example"}},
		{"unix:/run/hopcorder-server.sock", []string{"*"}, sock, false, nil},
		// A TCP socket from systemd: by its address, like listen.
		{"systemd", nil, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}, true, local},
		{"systemd", nil, &net.TCPAddr{IP: net.IPv6loopback, Port: 8080}, true, local},
		{"systemd", nil, &net.TCPAddr{IP: net.IPv4zero, Port: 8080}, true, nil},
		{"systemd", []string{"hopcorder.example"}, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}, true, []string{"hopcorder.example"}},
		// A configured TCP address: as before.
		{"127.0.0.1:8080", nil, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}, false, local},
		{"0.0.0.0:8080", nil, &net.TCPAddr{IP: net.IPv4zero, Port: 8080}, false, nil},
	} {
		cfg := &config.ServerConfig{Listen: tc.listen, AllowedHosts: tc.allowed}
		if err := cfg.Finalize(); err != nil {
			t.Fatal(err)
		}
		if got := listenerHosts(cfg, tc.addr, tc.activated); !slices.Equal(got, tc.want) {
			t.Errorf("listen %s on %s %s (activated %v), allowed_hosts %v: %v, want %v",
				tc.listen, tc.addr.Network(), tc.addr, tc.activated, tc.allowed, got, tc.want)
		}
	}
}
