package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/saremox/hopcorder/internal/config"
)

// sdListenFDsStart is the first file descriptor systemd passes
// (SD_LISTEN_FDS_START).
const sdListenFDsStart = 3

// unixSocketMode is the mode of a unix socket the server creates: its
// owner and group may connect (connecting needs write permission).
const unixSocketMode = 0o660

// listenServer opens the server's listener for cfg.Listen: a TCP address,
// a unix socket it creates, or the one socket systemd passed (socket
// activation). activated reports the last.
func listenServer(ctx context.Context, cfg *config.ServerConfig) (ln net.Listener, activated bool, err error) {
	network, addr, err := config.ListenAddress(cfg.Listen)
	if err != nil {
		return nil, false, fmt.Errorf("listen %q: %w", cfg.Listen, err)
	}
	n, err := sdListenFDs()
	if err != nil {
		return nil, false, err
	}
	if network != config.ListenSystemd {
		if n > 0 {
			// Otherwise the server would listen elsewhere and the socket's
			// connections would wait forever.
			return nil, false, fmt.Errorf("listen %s: systemd passed a socket (socket activation, e.g. hopcorder-server.socket): set listen: systemd, or stop the socket unit", cfg.Listen)
		}
		if network == "unix" {
			ln, err = listenUnix(ctx, addr)
		} else {
			var lc net.ListenConfig
			ln, err = lc.Listen(ctx, network, addr)
		}
		if err != nil {
			return nil, false, fmt.Errorf("listen %s: %w", cfg.Listen, err)
		}
		return ln, false, nil
	}
	switch {
	case n == 0:
		return nil, false, errors.New("listen systemd: systemd passed no socket (LISTEN_FDS, LISTEN_PID): start the server through its socket unit, e.g. hopcorder-server.socket")
	case n > 1:
		return nil, false, fmt.Errorf("listen systemd: systemd passed %d sockets, want one (one ListenStream= in the socket unit)", n)
	}
	name := "systemd socket"
	if names := os.Getenv("LISTEN_FDNAMES"); names != "" {
		name = names
	}
	// Taken once: nothing started later may think it was socket-activated.
	for _, v := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		_ = os.Unsetenv(v)
	}
	f := os.NewFile(sdListenFDsStart, name)
	// FileListener works on a duplicate (close-on-exec), so the original
	// descriptor is closed either way.
	ln, err = net.FileListener(f)
	_ = f.Close()
	if err != nil {
		return nil, false, fmt.Errorf("listen systemd: the passed descriptor is not a listening socket: %w", err)
	}
	return ln, true, nil
}

// sdListenFDs returns the number of sockets systemd passed to this process
// (sd_listen_fds): LISTEN_FDS, if LISTEN_PID is this process. Variables
// meant for another process (inherited from a socket-activated parent) are
// ignored.
func sdListenFDs() (int, error) {
	pid, fds := os.Getenv("LISTEN_PID"), os.Getenv("LISTEN_FDS")
	if pid == "" || fds == "" || pid != strconv.Itoa(os.Getpid()) {
		return 0, nil
	}
	n, err := strconv.Atoi(fds)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("listen: LISTEN_FDS %q from systemd: not a count", fds)
	}
	return n, nil
}

// listenUnix listens on a unix socket at path with mode 0660. A socket file
// left behind by a server that did not shut down cleanly is replaced; a
// socket something still listens on, and any other file, are not.
func listenUnix(ctx context.Context, path string) (net.Listener, error) {
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode().Type() != fs.ModeSocket {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		d := net.Dialer{Timeout: time.Second}
		c, err := d.DialContext(ctx, "unix", path)
		switch {
		case err == nil:
			_ = c.Close()
			return nil, fmt.Errorf("%s: another process is listening on it", path)
		case errors.Is(err, syscall.ECONNREFUSED):
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("remove stale socket: %w", err)
			}
		default:
			return nil, fmt.Errorf("check existing socket: %w", err)
		}
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	// Before the chmod the socket has the mode the umask leaves (0777 &^
	// umask): keep it in a directory only the server and the proxy can
	// enter if that matters.
	if err := os.Chmod(path, unixSocketMode); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("chmod %s: %w", path, err)
	}
	return ln, nil
}

// listenerHosts is the Host allowlist (config.ServerConfig.HostAllowlist)
// for the listener the server got. A unix socket is reached through a
// proxy on the same host, which authenticates and chooses the Host header
// it sends: there is no check unless allowed_hosts lists names. A socket
// from systemd is judged by its address, like a configured one.
func listenerHosts(cfg *config.ServerConfig, addr net.Addr, activated bool) []string {
	switch {
	case addr.Network() == "unix" && len(cfg.AllowedHosts) == 0:
		return nil
	case activated && strings.HasPrefix(addr.Network(), "tcp"):
		c := *cfg
		c.Listen = addr.String()
		return c.HostAllowlist()
	}
	return cfg.HostAllowlist()
}
