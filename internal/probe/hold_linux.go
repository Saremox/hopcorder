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

// holdKey is one local identifier a flow's probes need held: an ICMP echo
// identifier (one ping socket holds it for both address families, see
// holdEchoLocked), or a TCP source port of one address family.
type holdKey struct {
	proto Protocol // ICMP or TCP
	v6    bool     // TCP: the address family; always false for ICMP
	id    uint16   // the echo identifier or the port
}

func (k holdKey) String() string {
	switch {
	case k.proto == ICMP:
		return fmt.Sprintf("ICMP echo identifier %d", k.id)
	case k.v6:
		return fmt.Sprintf("IPv6 TCP port %d", k.id)
	default:
		return fmt.Sprintf("IPv4 TCP port %d", k.id)
	}
}

// holdKeys returns what req's probes need held: the echo identifier for
// ICMP, the source ports in req's address family for TCP, nothing for UDP.
func holdKeys(req TraceRequest) []holdKey {
	switch req.Protocol {
	case ICMP:
		return []holdKey{{proto: ICMP, id: req.FlowID}}
	case TCP:
		v6 := req.Target.Unmap().Is6()
		if req.Paris {
			return []holdKey{{proto: TCP, v6: v6, id: req.FlowID}}
		}
		var keys []holdKey
		for ttl := req.FirstTTL(); ttl <= req.MaxTTL; ttl++ {
			if p := tcpSourcePort(req.FlowID, false, ttl); p != 0 {
				keys = append(keys, holdKey{proto: TCP, v6: v6, id: p})
			}
		}
		return keys
	}
	return nil
}

// tcpPorts returns the TCP source ports of flow f (see Flow.Ports). Port 0,
// which a classic flow wraps through, cannot be held.
func (f Flow) tcpPorts() []uint16 {
	n := min(max(f.Ports, 1), 1<<16)
	ports := make([]uint16, 0, n)
	for ttl := 1; ttl <= n; ttl++ {
		if p := tcpSourcePort(f.ID, false, ttl); p != 0 {
			ports = append(ports, p)
		}
	}
	return ports
}

// heldID is one ICMP echo identifier held by a ping socket of one family.
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

// HoldFlows implements FlowHolder, for ICMP and TCP flows.
//
// It holds the echo identifier of every ICMP flow with an unprivileged ICMP
// "ping" socket bound to it. The kernel hands such a socket every echo
// reply carrying its identifier and refuses the identifier to any other
// ping socket, so another process can neither watch this agent's echo
// replies (and learn probe ids) nor send echoes that pass for its probes
// (an echo reply quotes no IP ID). IPv4 and IPv6 ping sockets share one
// table of identifiers (the kernel's ping_get_port compares the identifier
// only), so one socket holds an identifier for both families: an IPv4 one,
// or an IPv6 one where IPv4 ping sockets cannot be opened. (A second socket
// for the other family would collide with the first.) Where ping sockets
// are not permitted for this process (net.ipv4.ping_group_range), nothing
// is held and that is logged once.
//
// It holds every source port of every TCP flow (one, or max_ttl in classic
// mode) in both address families, each with a TCP socket bound to the
// wildcard address that never listens or connects (holdTCPPort). Another
// local process with a connection from the port to the target's service
// (`nc -p 33434 target 443`, or any connection the kernel happened to give
// that port) shares the probes' four-tuple, and the target answers every
// probe SYN with a challenge ACK for that connection (RFC 5961), not a
// SYN-ACK or RST: the cycles would be recorded as not reaching the target.
// With the port held, no other socket can bind it, whatever its options,
// and the kernel gives it to no connection.
//
// An identifier or port that cannot be held (another process has it, or
// had it a moment ago: a closed TCP connection stays in TIME_WAIT for a
// minute; or the socket fails) is left out; the error lists every such
// one, and everything else is held. Trace refuses the flows that use a
// left-out identifier or port with that error (TCP flows only for targets
// of the family left out), so another process on it stops only the targets
// of that flow, and tries to hold it again first: the targets resume once
// it is free.
//
// UDP source ports are not held: a bound UDP port swallows the datagrams
// other tracers send to it, so the host would stop answering them with port
// unreachable, and a UDP target's answer does not depend on the agent's
// sockets. Forged ICMP errors for UDP probes are refused by the IP ID check
// instead.
func (r *Raw) HoldFlows(flows []Flow) error {
	want := map[holdKey]uint16{} // -> the flow_id, for messages
	var order []holdKey
	add := func(k holdKey, flowID uint16) {
		if _, ok := want[k]; !ok {
			want[k] = flowID
			order = append(order, k)
		}
	}
	for _, f := range flows {
		switch f.Protocol {
		case ICMP:
			add(holdKey{proto: ICMP, id: f.ID}, f.ID)
		case TCP:
			for _, p := range f.tcpPorts() {
				add(holdKey{proto: TCP, id: p}, f.ID)
				add(holdKey{proto: TCP, v6: true, id: p}, f.ID)
			}
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
	for k, c := range r.held {
		if _, ok := want[k]; !ok {
			_ = c.Close()
			delete(r.held, k)
		}
	}
	r.blocked = map[holdKey]error{}
	var errs []error
	for _, k := range order {
		if err := r.holdLocked(k, want[k]); err != nil {
			r.blocked[k] = err
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// flowReady returns nil if req's probes may be sent: for each identifier or
// port they use, HoldFlows was not asked to hold it, or holds it, or this
// process cannot hold it at all. One HoldFlows could not hold is tried
// again; the error says why it still cannot.
func (r *Raw) flowReady(req TraceRequest) error {
	keys := holdKeys(req)
	if len(keys) == 0 {
		return nil
	}
	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	for _, k := range keys {
		if _, blocked := r.blocked[k]; !blocked {
			continue
		}
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return errClosed
		}
		if err := r.holdLocked(k, req.FlowID); err != nil {
			r.blocked[k] = err
			return err
		}
		delete(r.blocked, k)
		r.log.Info("now holding the flow's "+k.String()+"; probing its targets again",
			"protocol", string(req.Protocol), "flow_id", req.FlowID)
	}
	return nil
}

// holdLocked holds k (for a flow with flow_id flowID, named in errors)
// unless it is held already. Where this process cannot hold it
// (errCannotHold) that is logged and nil returned; other failures are
// returned.
func (r *Raw) holdLocked(k holdKey, flowID uint16) error {
	if _, ok := r.held[k]; ok {
		return nil
	}
	if k.proto == TCP {
		return r.holdTCPLocked(k, flowID)
	}
	return r.holdEchoLocked(k)
}

func (r *Raw) holdEchoLocked(k holdKey) error {
	var first heldID
	var firstErr error
	for _, v6 := range []bool{false, true} {
		h := heldID{v6: v6, id: k.id}
		c, err := holdEchoID(h)
		if err == nil {
			r.held[k] = c
			return nil
		}
		if errors.Is(err, syscall.EADDRINUSE) {
			return fmt.Errorf("icmp flow_id %d: %s is %w (another hopcorder, or a ping, with the same identifier?); "+
				"every hopcorder process on a host needs its own flow_id", h.id, h, ErrFlowInUse)
		}
		if !errors.Is(err, errCannotHold) {
			return fmt.Errorf("icmp flow_id %d: hold %s: %w", h.id, h, err)
		}
		if firstErr == nil {
			first, firstErr = h, err
		}
	}
	// Neither family: report why IPv4 could not.
	r.logCannotHold(first, firstErr)
	return nil
}

func (r *Raw) holdTCPLocked(k holdKey, flowID uint16) error {
	c, err := holdTCPPort(k)
	switch {
	case err == nil:
		r.held[k] = c
		return nil
	case errors.Is(err, syscall.EADDRINUSE):
		return fmt.Errorf("tcp flow_id %d: %s is %w (a connection from it, one closed less than a minute ago, "+
			"which the kernel keeps in TIME_WAIT, a server listening on it, or another hopcorder with the same "+
			"flow_id?); every hopcorder process on a host needs its own flow_id, and TCP ones a port nothing else uses",
			flowID, k, ErrFlowInUse)
	case errors.Is(err, errCannotHold):
		r.logCannotHoldTCP(k, err)
		return nil
	default:
		return fmt.Errorf("tcp flow_id %d: hold %s: %w", flowID, k, err)
	}
}

// errCannotHold marks identifiers this process cannot hold, for reasons
// that are no error of the configuration: no IPv6 in the kernel, ping
// sockets not permitted for this process's group, or a TCP port this
// process may not bind.
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

// holdTCPPort binds a TCP socket to TCP port k.id on the wildcard address of
// k's family, with SO_REUSEADDR cleared, and never listens or connects on
// it (checked on Linux 6.18, see TestInnerTCPHold):
//
//   - Every other bind() to the port fails with EADDRINUSE, with or
//     without SO_REUSEADDR or SO_REUSEPORT, to any local address, and the
//     kernel gives the port to no connect() (it skips ports someone bound).
//     The kernel shares a port only between sockets that all set
//     SO_REUSEADDR (or all SO_REUSEPORT, under one uid).
//   - A bound socket that neither listens nor is connected receives
//     nothing: the kernel matches segments only to listening and connected
//     sockets. So the target's SYN-ACKs and RSTs are handled as before:
//     the raw TCP socket gets them, and the kernel answers each SYN-ACK
//     with a RST, so no connection is established.
//   - The bind fails with EADDRINUSE while another socket has the port: a
//     connection from it (a listener's, too), or one in TIME_WAIT (a
//     minute after it was closed from this side), or another holder.
//
// The IPv6 socket is IPv6-only, so it does not collide with the IPv4 one
// (TCP's ports are one table for both families); without that it would
// hold both.
func holdTCPPort(k holdKey) (io.Closer, error) {
	family := syscall.AF_INET
	var sa syscall.Sockaddr = &syscall.SockaddrInet4{Port: int(k.id)}
	if k.v6 {
		family, sa = syscall.AF_INET6, &syscall.SockaddrInet6{Port: int(k.id)}
	}
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, syscall.IPPROTO_TCP)
	if err != nil {
		if errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
			return nil, fmt.Errorf("%w: %w", errCannotHold, os.NewSyscallError("socket", err))
		}
		return nil, os.NewSyscallError("socket", err)
	}
	opts := [][3]int{{syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 0}} // a new socket's default; this relies on it
	if k.v6 {
		opts = append(opts, [3]int{syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, 1})
	}
	for _, o := range opts {
		if err := syscall.SetsockoptInt(fd, o[0], o[1], o[2]); err != nil {
			_ = syscall.Close(fd)
			return nil, os.NewSyscallError("setsockopt", err)
		}
	}
	if err := syscall.Bind(fd, sa); err != nil {
		_ = syscall.Close(fd)
		if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
			// A port below net.ipv4.ip_unprivileged_port_start without
			// CAP_NET_BIND_SERVICE, or a security policy.
			return nil, fmt.Errorf("%w: %w", errCannotHold, os.NewSyscallError("bind", err))
		}
		return nil, os.NewSyscallError("bind", err)
	}
	return os.NewFile(uintptr(fd), k.String()), nil
}

// logCannotHold logs, once per address family, that echo identifiers are
// not held. At info level only if other processes may open ping sockets.
func (r *Raw) logCannotHold(h heldID, err error) {
	once := "icmp4"
	if h.v6 {
		once = "icmp6"
	}
	if r.holdLogged[once] {
		return
	}
	r.holdLogged[once] = true
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

// logCannotHoldTCP logs, once for each reason, that a TCP port is not held.
// A warning only if other unprivileged processes may bind it.
func (r *Raw) logCannotHoldTCP(k holdKey, err error) {
	once := "tcp"
	switch {
	case errors.Is(err, syscall.EAFNOSUPPORT):
		once = "tcp6"
	case int(k.id) < unprivilegedPortStart():
		once = "tcp-privileged"
	}
	if r.holdLogged[once] {
		return
	}
	r.holdLogged[once] = true
	switch once {
	case "tcp6":
		r.log.Debug("no IPv6 in the kernel; no IPv6 TCP port to hold")
	case "tcp-privileged":
		r.log.Debug("TCP source ports below net.ipv4.ip_unprivileged_port_start are not held: "+
			"this process may not bind them, and no other unprivileged one may", "port", k.id, "err", err)
	default:
		r.log.Warn("cannot hold the TCP source port: other local processes can connect from it, and the target "+
			"answers the probes for such a connection, so they look unanswered; allow this process to bind it",
			"port", k.id, "err", err)
	}
}

// unprivilegedPortStart returns net.ipv4.ip_unprivileged_port_start (the
// kernel's default, 1024, if it cannot be read).
func unprivilegedPortStart() int {
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_unprivileged_port_start")
	if err != nil {
		return 1024
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 1024
	}
	return n
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
