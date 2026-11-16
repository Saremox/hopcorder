package probe

import (
	"encoding/binary"
	"errors"
	"net/netip"
)

// IP protocol numbers.
const (
	protoICMP   = 1
	protoTCP    = 6
	protoUDP    = 17
	protoICMPv6 = 58
)

// TCP flags.
const (
	tcpSYN = 0x02
	tcpRST = 0x04
	tcpACK = 0x10
)

// ICMP types and codes used by the prober.
const (
	icmp4EchoReply    = 0
	icmp4Unreachable  = 3
	icmp4EchoRequest  = 8
	icmp4TimeExceeded = 11
	icmp4PortUnreach  = 3 // code of icmp4Unreachable

	icmp6Unreachable  = 1
	icmp6TimeExceeded = 3
	icmp6EchoRequest  = 128
	icmp6EchoReply    = 129
	icmp6PortUnreach  = 4 // code of icmp6Unreachable
)

const (
	ipv4HeaderLen = 20
	ipv6HeaderLen = 40
	// payloadMagic follows the 2-byte compensation word in every probe so
	// probes are recognisable in packet captures.
	payloadMagic = "hopcorder\x00"
	payloadLen   = 2 + len(payloadMagic)
)

// probeKey identifies one in-flight probe. The fields hold whatever the
// protocol lets us recover from a reply:
//
//	icmp: a = identifier, b = 0,     c = sequence
//	udp:  a = src port,   b = dst port, c = UDP checksum
//	tcp:  a = src port,   b = dst port, c = top 16 bits of the SYN's sequence number
type probeKey struct {
	dst     netip.Addr
	proto   uint8
	a, b, c uint16
}

// replyKind classifies an ICMP message we care about.
type replyKind uint8

const (
	kindEchoReply replyKind = iota + 1
	kindTimeExceeded
	kindUnreachable
	kindTCPReply // SYN-ACK or RST from the destination
)

// icmpReply is a parsed ICMP message that may answer one of our probes.
type icmpReply struct {
	kind replyKind
	code uint8
	key  probeKey
}

// Parse errors. errIgnored marks well-formed ICMP that is simply not a reply
// to a probe (echo requests, redirects, ...).
var (
	errTruncated = errors.New("truncated packet")
	errMalformed = errors.New("malformed packet")
	errIgnored   = errors.New("not a probe reply")
)

// onesSum adds b to sum as big-endian 16-bit words (RFC 1071), without
// folding or inverting.
func onesSum(sum uint32, b []byte) uint32 {
	for len(b) >= 2 {
		sum += uint32(binary.BigEndian.Uint16(b))
		b = b[2:]
	}
	if len(b) == 1 {
		sum += uint32(b[0]) << 8
	}
	return sum
}

func fold(sum uint32) uint16 {
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return uint16(sum)
}

// checksum is the Internet checksum of b with an initial partial sum.
func checksum(initial uint32, b []byte) uint16 {
	return ^fold(onesSum(initial, b))
}

// pseudoHeaderSum is the partial sum of the IPv4 or IPv6 pseudo-header for an
// upper-layer packet of length n.
func pseudoHeaderSum(src, dst netip.Addr, proto uint8, n int) uint32 {
	var sum uint32
	if src.Is4() {
		s, d := src.As4(), dst.As4()
		sum = onesSum(sum, s[:])
		sum = onesSum(sum, d[:])
		sum += uint32(proto) + uint32(n)
		return sum
	}
	s, d := src.As16(), dst.As16()
	sum = onesSum(sum, s[:])
	sum = onesSum(sum, d[:])
	sum += uint32(n>>16) + uint32(n&0xffff) + uint32(proto)
	return sum
}

// buildEcho returns an ICMPv4 or ICMPv6 echo request. In paris mode the
// payload's first word compensates for seq, so the checksum (which some ECMP
// routers hash on, as part of the first four L4 bytes) is the same for every
// probe of a flow. src and dst are only used for the ICMPv6 pseudo-header;
// the kernel recomputes that checksum anyway, and arrives at the same value.
func buildEcho(src, dst netip.Addr, id, seq uint16, paris bool) []byte {
	b := make([]byte, 8+payloadLen)
	if dst.Is4() {
		b[0] = icmp4EchoRequest
	} else {
		b[0] = icmp6EchoRequest
	}
	binary.BigEndian.PutUint16(b[4:], id)
	binary.BigEndian.PutUint16(b[6:], seq)
	if paris {
		// seq + ^seq == 0xffff, i.e. ones-complement zero: the sum, and
		// therefore the checksum, no longer depends on seq.
		binary.BigEndian.PutUint16(b[8:], ^seq)
	}
	copy(b[10:], payloadMagic)
	var initial uint32
	if dst.Is6() {
		initial = pseudoHeaderSum(src, dst, protoICMPv6, len(b))
	}
	binary.BigEndian.PutUint16(b[2:], checksum(initial, b))
	return b
}

// buildUDP returns a UDP header and payload whose checksum is exactly want,
// achieved by choosing the payload's first word. The checksum identifies the
// probe while ports — the fields ECMP hashes on — stay fixed. want must be in
// 1..0xfffe (0 means "no checksum" in IPv4 and 0xffff is its alias).
func buildUDP(src, dst netip.Addr, sport, dport, want uint16) []byte {
	b := make([]byte, 8+payloadLen)
	binary.BigEndian.PutUint16(b[0:], sport)
	binary.BigEndian.PutUint16(b[2:], dport)
	binary.BigEndian.PutUint16(b[4:], uint16(len(b)))
	copy(b[10:], payloadMagic)
	// With the checksum and compensation words zero, the folded sum is s.
	// We need fold(s + comp) == ^want, so comp = ^want - s in ones-complement
	// arithmetic, which is ^want + ^s.
	s := fold(onesSum(pseudoHeaderSum(src, dst, protoUDP, len(b)), b))
	comp := fold(uint32(^want) + uint32(^s))
	binary.BigEndian.PutUint16(b[8:], comp)
	binary.BigEndian.PutUint16(b[6:], want)
	return b
}

// buildTCPSyn returns a TCP SYN with an MSS option, like a real connection
// attempt, so middleboxes treat it as one. The probe id travels in the
// sequence number (seq = id<<16), which ECMP does not hash on; a SYN-ACK or
// RST acknowledges seq+1, and ICMP errors quote the sequence number in their
// first 8 bytes.
func buildTCPSyn(src, dst netip.Addr, sport, dport uint16, seq uint32) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint16(b[0:], sport)
	binary.BigEndian.PutUint16(b[2:], dport)
	binary.BigEndian.PutUint32(b[4:], seq)
	b[12] = 6 << 4 // data offset: 6 words
	b[13] = tcpSYN
	binary.BigEndian.PutUint16(b[14:], 64240) // window
	copy(b[20:], []byte{2, 4, 0x05, 0xb4})    // MSS 1460
	binary.BigEndian.PutUint16(b[16:], checksum(pseudoHeaderSum(src, dst, protoTCP, len(b)), b))
	return b
}

// tcpSeq is the sequence number that carries probe id.
func tcpSeq(id uint16) uint32 { return uint32(id) << 16 }

// parseTCP parses a TCP segment (no IP header) received from from. Only a
// SYN-ACK or an RST with ACK — the two answers to a SYN — is a reply.
func parseTCP(from netip.Addr, seg []byte) (icmpReply, error) {
	if len(seg) < 20 {
		return icmpReply{}, errTruncated
	}
	if off := int(seg[12]>>4) * 4; off < 20 || off > len(seg) {
		return icmpReply{}, errMalformed
	}
	flags := seg[13]
	if flags&tcpACK == 0 || flags&(tcpSYN|tcpRST) == 0 {
		return icmpReply{}, errIgnored
	}
	ack := binary.BigEndian.Uint32(seg[8:])
	return icmpReply{kind: kindTCPReply, key: probeKey{
		dst: from.Unmap().WithZone(""), proto: protoTCP,
		a: binary.BigEndian.Uint16(seg[2:]), // their destination port is our source port
		b: binary.BigEndian.Uint16(seg[0:]),
		c: uint16((ack - 1) >> 16),
	}}, nil
}

// buildIPv4 returns an IPv4 header for an IP_HDRINCL raw socket. Linux
// expects total length and fragment offset in network byte order and would
// fill in the checksum itself; we compute it so the bytes are complete.
func buildIPv4(src, dst netip.Addr, proto, ttl uint8, id uint16, payload int) []byte {
	h := make([]byte, ipv4HeaderLen)
	h[0] = 0x45
	binary.BigEndian.PutUint16(h[2:], uint16(ipv4HeaderLen+payload))
	binary.BigEndian.PutUint16(h[4:], id)
	h[8] = ttl
	h[9] = proto
	s, d := src.As4(), dst.As4()
	copy(h[12:16], s[:])
	copy(h[16:20], d[:])
	binary.BigEndian.PutUint16(h[10:], checksum(0, h))
	return h
}

// parseICMPv4 parses an ICMPv4 message (IP header already stripped) received
// from from.
func parseICMPv4(from netip.Addr, b []byte) (icmpReply, error) {
	if len(b) < 8 {
		return icmpReply{}, errTruncated
	}
	typ, code := b[0], b[1]
	switch typ {
	case icmp4EchoReply:
		return icmpReply{kind: kindEchoReply, key: probeKey{
			dst: from, proto: protoICMP,
			a: binary.BigEndian.Uint16(b[4:]), c: binary.BigEndian.Uint16(b[6:]),
		}}, nil
	case icmp4TimeExceeded:
		if code != 0 { // code 1 is fragment reassembly time exceeded
			return icmpReply{}, errIgnored
		}
		key, err := parseQuotedIPv4(b[8:])
		return icmpReply{kind: kindTimeExceeded, key: key}, err
	case icmp4Unreachable:
		key, err := parseQuotedIPv4(b[8:])
		return icmpReply{kind: kindUnreachable, code: code, key: key}, err
	default:
		return icmpReply{}, errIgnored
	}
}

func parseQuotedIPv4(q []byte) (probeKey, error) {
	if len(q) < ipv4HeaderLen {
		return probeKey{}, errTruncated
	}
	if q[0]>>4 != 4 {
		return probeKey{}, errMalformed
	}
	ihl := int(q[0]&0x0f) * 4
	if ihl < ipv4HeaderLen {
		return probeKey{}, errMalformed
	}
	if binary.BigEndian.Uint16(q[6:])&0x1fff != 0 {
		return probeKey{}, errIgnored // non-first fragment, no L4 header
	}
	if len(q) < ihl+8 {
		return probeKey{}, errTruncated
	}
	dst := netip.AddrFrom4([4]byte(q[16:20]))
	return quotedL4Key(dst, q[9], q[ihl:ihl+8])
}

// parseICMPv6 parses an ICMPv6 message received from from.
func parseICMPv6(from netip.Addr, b []byte) (icmpReply, error) {
	if len(b) < 8 {
		return icmpReply{}, errTruncated
	}
	typ, code := b[0], b[1]
	switch typ {
	case icmp6EchoReply:
		return icmpReply{kind: kindEchoReply, key: probeKey{
			dst: from.WithZone(""), proto: protoICMPv6,
			a: binary.BigEndian.Uint16(b[4:]), c: binary.BigEndian.Uint16(b[6:]),
		}}, nil
	case icmp6TimeExceeded:
		if code != 0 { // code 1 is fragment reassembly time exceeded
			return icmpReply{}, errIgnored
		}
		key, err := parseQuotedIPv6(b[8:])
		return icmpReply{kind: kindTimeExceeded, key: key}, err
	case icmp6Unreachable:
		key, err := parseQuotedIPv6(b[8:])
		return icmpReply{kind: kindUnreachable, code: code, key: key}, err
	default:
		return icmpReply{}, errIgnored
	}
}

func parseQuotedIPv6(q []byte) (probeKey, error) {
	if len(q) < ipv6HeaderLen {
		return probeKey{}, errTruncated
	}
	if q[0]>>4 != 6 {
		return probeKey{}, errMalformed
	}
	if len(q) < ipv6HeaderLen+8 {
		return probeKey{}, errTruncated
	}
	// Our probes carry no extension headers, so anything else in the
	// next-header field is not ours.
	dst := netip.AddrFrom16([16]byte(q[24:40]))
	return quotedL4Key(dst, q[6], q[ipv6HeaderLen:ipv6HeaderLen+8])
}

// quotedL4Key builds the probe key from the first 8 bytes of a quoted L4
// header.
func quotedL4Key(dst netip.Addr, proto uint8, l4 []byte) (probeKey, error) {
	switch proto {
	case protoICMP, protoICMPv6:
		if (proto == protoICMP && l4[0] != icmp4EchoRequest) || (proto == protoICMPv6 && l4[0] != icmp6EchoRequest) {
			return probeKey{}, errIgnored
		}
		return probeKey{dst: dst, proto: proto,
			a: binary.BigEndian.Uint16(l4[4:]), c: binary.BigEndian.Uint16(l4[6:])}, nil
	case protoUDP:
		return probeKey{dst: dst, proto: proto,
			a: binary.BigEndian.Uint16(l4[0:]), b: binary.BigEndian.Uint16(l4[2:]),
			c: binary.BigEndian.Uint16(l4[6:])}, nil
	case protoTCP:
		return probeKey{dst: dst, proto: proto,
			a: binary.BigEndian.Uint16(l4[0:]), b: binary.BigEndian.Uint16(l4[2:]),
			c: uint16(binary.BigEndian.Uint32(l4[4:]) >> 16)}, nil
	default:
		return probeKey{}, errIgnored
	}
}
