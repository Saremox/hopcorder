//go:build linux

package probe

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// heldID is one ICMP echo identifier held by a ping socket.
type heldID struct {
	v6 bool
	id uint16
}

func (h heldID) String() string {
	if h.v6 {
		return fmt.Sprintf("ICMPv6 echo identifier %d", h.id)
	}
	return fmt.Sprintf("ICMP echo identifier %d", h.id)
}

// HoldFlows implements FlowHolder. It holds the echo identifier of every
// ICMP flow with an unprivileged ICMP "ping" socket bound to it. The kernel
// hands such a socket every echo reply carrying its identifier and refuses
// the identifier to any other ping socket, so another process can neither
// watch this agent's echo replies (and learn probe ids) nor send echoes that
// pass for its probes (an echo reply quotes no IP ID). IPv4 and IPv6 ping
// sockets share one table of identifiers (the kernel's ping_get_port
// compares the identifier only), so one socket holds an identifier for both
// families: an IPv4 one, or an IPv6 one where IPv4 ping sockets cannot be
// opened. (A second socket for the other family would collide with the
// first.) Where ping sockets are not permitted for this process
// (net.ipv4.ping_group_range), nothing is held and that is logged once.
//
// UDP and TCP source ports are not held: a bound UDP port swallows the
// datagrams other tracers send to it, so the host would stop answering them
// with port unreachable, and a TCP port cannot be bound while any earlier
// connection on it is in TIME_WAIT. Forged ICMP errors for those probes are
// refused by the IP ID check instead, and their destination answers
// (SYN-ACK, RST) cannot be provoked without choosing the SYN's sequence
// number.
func (r *Raw) HoldFlows(flows []Flow) error {
	want := map[uint16]bool{}
	var order []uint16
	for _, f := range flows {
		if f.Protocol == ICMP && !want[f.ID] {
			want[f.ID] = true
			order = append(order, f.ID)
		}
	}

	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return errClosed
	}
	held := map[uint16]bool{}
	for h := range r.held {
		held[h.id] = true
	}
	opened := map[heldID]io.Closer{}
	fail := func(h heldID, err error) error {
		for _, c := range opened {
			_ = c.Close()
		}
		if errors.Is(err, syscall.EADDRINUSE) {
			return fmt.Errorf("icmp flow_id %d: %s is %w (another hopcorder, or a ping, with the same identifier?); "+
				"every hopcorder process on a host needs its own flow_id", h.id, h, ErrFlowInUse)
		}
		return fmt.Errorf("icmp flow_id %d: hold %s: %w", h.id, h, err)
	}
	for _, id := range order {
		if held[id] {
			continue
		}
		var first heldID
		var firstErr error
		for _, v6 := range []bool{false, true} {
			h := heldID{v6: v6, id: id}
			c, err := holdEchoID(h)
			if err == nil {
				opened[h] = c
				firstErr = nil
				break
			}
			if !errors.Is(err, errCannotHold) {
				return fail(h, err)
			}
			if firstErr == nil {
				first, firstErr = h, err
			}
		}
		if firstErr != nil { // neither family: report why IPv4 could not
			r.logCannotHold(first, firstErr)
		}
	}
	for h, c := range r.held {
		if !want[h.id] {
			_ = c.Close()
			delete(r.held, h)
		}
	}
	for h, c := range opened {
		r.held[h] = c
	}
	return nil
}

// errCannotHold marks identifiers this process cannot hold, for reasons
// that are no error of the configuration: no IPv6 in the kernel, or ping
// sockets not permitted for this process's group.
var errCannotHold = errors.New("cannot hold")

// holdEchoID opens a ping socket bound to h's echo identifier, with a filter
// that drops everything, so this agent's echo replies (which it also
// receives) never queue up on it.
func holdEchoID(h heldID) (io.Closer, error) {
	family, proto := syscall.AF_INET, syscall.IPPROTO_ICMP
	var sa syscall.Sockaddr = &syscall.SockaddrInet4{Port: int(h.id)}
	if h.v6 {
		family, proto = syscall.AF_INET6, syscall.IPPROTO_ICMPV6
		sa = &syscall.SockaddrInet6{Port: int(h.id)}
	}
	fd, err := syscall.Socket(family, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, proto)
	if err != nil {
		if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EAFNOSUPPORT) ||
			errors.Is(err, syscall.EPROTONOSUPPORT) || errors.Is(err, syscall.ESOCKTNOSUPPORT) {
			return nil, fmt.Errorf("%w: %w", errCannotHold, os.NewSyscallError("socket", err))
		}
		return nil, os.NewSyscallError("socket", err)
	}
	// Ping sockets start with SO_REUSEADDR set, and the kernel lets two of
	// them share an identifier unless one has it cleared.
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 0); err != nil {
		_ = syscall.Close(fd)
		return nil, os.NewSyscallError("setsockopt", err)
	}
	if err := syscall.Bind(fd, sa); err != nil {
		_ = syscall.Close(fd)
		return nil, os.NewSyscallError("bind", err)
	}
	f := os.NewFile(uintptr(fd), h.String())
	c, err := net.FilePacketConn(f) // the way x/net/icmp wraps ping sockets
	_ = f.Close()
	if err != nil {
		return nil, err
	}
	var fs bpfSetter = ipv4.NewPacketConn(c)
	if h.v6 {
		fs = ipv6.NewPacketConn(c)
	}
	if err := setFilter(fs, dropAll); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("drop filter: %w", err)
	}
	return c, nil
}

// logCannotHold logs, once per address family, that echo identifiers are
// not held. At info level only if other processes may open ping sockets.
func (r *Raw) logCannotHold(h heldID, err error) {
	if r.holdLogged[h.v6] {
		return
	}
	r.holdLogged[h.v6] = true
	if errors.Is(err, syscall.EAFNOSUPPORT) {
		r.log.Debug("no IPv6 in the kernel; no ICMPv6 echo identifier to hold")
		return
	}
	rng, open := pingGroupRange()
	if !open {
		r.log.Debug("ping sockets are disabled; no other process can claim the ICMP echo identifier",
			"ping_group_range", rng)
		return
	}
	r.log.Info("cannot hold the ICMP echo identifier: ping sockets are not permitted for this process's group. "+
		"Processes whose group is in net.ipv4.ping_group_range can see this agent's echo replies; "+
		"add the agent's group to the range to prevent that",
		"identifier", h.id, "ping_group_range", rng, "err", err)
}

// pingGroupRange returns net.ipv4.ping_group_range and whether it permits
// any group at all (the kernel's default, "1 0", permits none).
func pingGroupRange() (string, bool) {
	b, err := os.ReadFile("/proc/sys/net/ipv4/ping_group_range")
	if err != nil {
		return "", true
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return strings.TrimSpace(string(b)), true
	}
	lo, err1 := strconv.ParseUint(f[0], 10, 32)
	hi, err2 := strconv.ParseUint(f[1], 10, 32)
	if err1 != nil || err2 != nil {
		return f[0] + " " + f[1], true
	}
	return f[0] + " " + f[1], lo <= hi
}
