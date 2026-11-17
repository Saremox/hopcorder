package probe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/netip"
	"slices"

	"github.com/saremox/hopcorder/internal/record"
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
	// Every ICMP and UDP probe carries, after its 8-byte header, a 2-byte
	// checksum compensation word, a random 4-byte nonce and payloadMagic
	// (so probes are recognisable in packet captures).
	nonceLen     = 4
	payloadMagic = "hopcorder\x00"
	payloadLen   = 2 + nonceLen + len(payloadMagic)
	// nonceEnd is how much of the payload a quote must hold to include the
	// nonce.
	nonceEnd = 2 + nonceLen
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
	// ipID is the identification field of a quoted IPv4 header; quotedIPID
	// says whether there was one (ICMPv4 errors).
	ipID       uint16
	quotedIPID bool
	// seq is the full sequence number of the SYN that a TCP reply
	// acknowledges (its ack minus one) or an ICMP error quotes; set when
	// key.proto is TCP.
	seq uint32
	// l4Off and l4Len locate the quoted L4 packet of an ICMP error in the
	// message: its header and as much of its payload as was quoted, cut at
	// the length its IP header states, so that RFC 4884 padding and
	// extensions are left out. Both are 0 for echo and TCP replies.
	l4Off, l4Len int
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

// buildEcho returns an ICMPv4 or ICMPv6 echo request whose data carries
// nonce, which the destination echoes back. In paris mode the payload's
// first word compensates for seq and nonce, so the checksum (which some ECMP
// routers hash on, as part of the first four L4 bytes) is the same for every
// probe of a flow. src and dst are only used for the ICMPv6 pseudo-header;
// the kernel recomputes that checksum anyway, and arrives at the same value.
func buildEcho(src, dst netip.Addr, id, seq uint16, nonce uint32, paris bool) []byte {
	b := make([]byte, 8+payloadLen)
	if dst.Is4() {
		b[0] = icmp4EchoRequest
	} else {
		b[0] = icmp6EchoRequest
	}
	binary.BigEndian.PutUint16(b[4:], id)
	binary.BigEndian.PutUint16(b[6:], seq)
	binary.BigEndian.PutUint32(b[10:], nonce)
	if paris {
		// x + ^x == 0xffff, i.e. ones-complement zero: with x the sum of
		// seq and the nonce's two words, the packet's sum, and therefore
		// its checksum, no longer depends on either.
		binary.BigEndian.PutUint16(b[8:], ^fold(uint32(seq)+nonce>>16+nonce&0xffff))
	}
	copy(b[8+nonceEnd:], payloadMagic)
	var initial uint32
	if dst.Is6() {
		initial = pseudoHeaderSum(src, dst, protoICMPv6, len(b))
	}
	binary.BigEndian.PutUint16(b[2:], checksum(initial, b))
	return b
}

// buildUDP returns a UDP header and a payload carrying nonce, with a
// checksum of exactly want, achieved by choosing the payload's first word.
// The checksum identifies the probe while ports — the fields ECMP hashes
// on — stay fixed. want must be in 1..0xfffe (0 means "no checksum" in IPv4
// and 0xffff is its alias).
func buildUDP(src, dst netip.Addr, sport, dport, want uint16, nonce uint32) []byte {
	b := make([]byte, 8+payloadLen)
	binary.BigEndian.PutUint16(b[0:], sport)
	binary.BigEndian.PutUint16(b[2:], dport)
	binary.BigEndian.PutUint16(b[4:], uint16(len(b)))
	binary.BigEndian.PutUint32(b[10:], nonce)
	copy(b[8+nonceEnd:], payloadMagic)
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
// sequence number (see tcpSeq), which ECMP does not hash on; a SYN-ACK or
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

// tcpSeq is a SYN's sequence number: the probe id in the top 16 bits (the
// key) and random bits in the low 16. A reply must acknowledge exactly
// seq+1, and an ICMP error must quote all 32 bits.
func tcpSeq(id, random uint16) uint32 { return uint32(id)<<16 | uint32(random) }

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
	seq := binary.BigEndian.Uint32(seg[8:]) - 1 // the ack acknowledges the SYN's seq + 1
	return icmpReply{kind: kindTCPReply, seq: seq, key: probeKey{
		dst: from.Unmap().WithZone(""), proto: protoTCP,
		a: binary.BigEndian.Uint16(seg[2:]), // their destination port is our source port
		b: binary.BigEndian.Uint16(seg[0:]),
		c: uint16(seq >> 16),
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
		return quotedReply(kindTimeExceeded, 0, b, parseQuotedIPv4)
	case icmp4Unreachable:
		return quotedReply(kindUnreachable, code, b, parseQuotedIPv4)
	default:
		return icmpReply{}, errIgnored
	}
}

// quotedReply parses the packet quoted by ICMP error b with parse.
func quotedReply(kind replyKind, code uint8, b []byte, parse func([]byte) (icmpReply, error)) (icmpReply, error) {
	r, err := parse(b[8:])
	if err != nil {
		return icmpReply{}, err
	}
	r.kind, r.code = kind, code
	r.l4Off += 8
	return r, nil
}

// parseQuotedIPv4 returns the probe key of a quoted IPv4 packet, its
// identification field and where its L4 packet lies in q.
func parseQuotedIPv4(q []byte) (icmpReply, error) {
	if len(q) < ipv4HeaderLen {
		return icmpReply{}, errTruncated
	}
	if q[0]>>4 != 4 {
		return icmpReply{}, errMalformed
	}
	ihl := int(q[0]&0x0f) * 4
	if ihl < ipv4HeaderLen {
		return icmpReply{}, errMalformed
	}
	if binary.BigEndian.Uint16(q[6:])&0x1fff != 0 {
		return icmpReply{}, errIgnored // non-first fragment, no L4 header
	}
	if len(q) < ihl+8 {
		return icmpReply{}, errTruncated
	}
	dst := netip.AddrFrom4([4]byte(q[16:20]))
	r, err := quotedL4(dst, q[9], q[ihl:], int(binary.BigEndian.Uint16(q[2:]))-ihl)
	r.ipID, r.quotedIPID = binary.BigEndian.Uint16(q[4:]), true
	r.l4Off = ihl
	return r, err
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
		return quotedReply(kindTimeExceeded, 0, b, parseQuotedIPv6)
	case icmp6Unreachable:
		return quotedReply(kindUnreachable, code, b, parseQuotedIPv6)
	default:
		return icmpReply{}, errIgnored
	}
}

func parseQuotedIPv6(q []byte) (icmpReply, error) {
	if len(q) < ipv6HeaderLen {
		return icmpReply{}, errTruncated
	}
	if q[0]>>4 != 6 {
		return icmpReply{}, errMalformed
	}
	if len(q) < ipv6HeaderLen+8 {
		return icmpReply{}, errTruncated
	}
	// Our probes carry no extension headers, so anything else in the
	// next-header field is not ours.
	dst := netip.AddrFrom16([16]byte(q[24:40]))
	r, err := quotedL4(dst, q[6], q[ipv6HeaderLen:], int(binary.BigEndian.Uint16(q[4:])))
	r.l4Off = ipv6HeaderLen
	return r, err
}

// quotedL4 builds the probe key from the first 8 bytes of a quoted L4
// packet l4 (at least 8 bytes, possibly followed by padding) whose IP header
// states its length as n. l4Len is set to the bytes of l4 that belong to
// the packet; a length shorter than the L4 header is ignored.
func quotedL4(dst netip.Addr, proto uint8, l4 []byte, n int) (icmpReply, error) {
	r := icmpReply{l4Len: len(l4)}
	if n >= 8 && n < len(l4) {
		r.l4Len = n
	}
	switch proto {
	case protoICMP, protoICMPv6:
		if (proto == protoICMP && l4[0] != icmp4EchoRequest) || (proto == protoICMPv6 && l4[0] != icmp6EchoRequest) {
			return icmpReply{}, errIgnored
		}
		r.key = probeKey{dst: dst, proto: proto,
			a: binary.BigEndian.Uint16(l4[4:]), c: binary.BigEndian.Uint16(l4[6:])}
	case protoUDP:
		r.key = probeKey{dst: dst, proto: proto,
			a: binary.BigEndian.Uint16(l4[0:]), b: binary.BigEndian.Uint16(l4[2:]),
			c: binary.BigEndian.Uint16(l4[6:])}
	case protoTCP:
		r.seq = binary.BigEndian.Uint32(l4[4:])
		r.key = probeKey{dst: dst, proto: proto,
			a: binary.BigEndian.Uint16(l4[0:]), b: binary.BigEndian.Uint16(l4[2:]),
			c: uint16(r.seq >> 16)}
	default:
		return icmpReply{}, errIgnored
	}
	return r, nil
}

// quoteMatches reports whether the payload an ICMP error quotes (l4 is the
// quoted L4 packet, header included) agrees with data, the payload of the
// probe it names. Only what was quoted is compared, since routers may quote
// just the first 8 L4 bytes (RFC 792). With requireNonce (IPv6, where RFC
// 4443 requires as much of the packet as fits) a quote too short to include
// the nonce is refused. Without it, a quoted payload of zeros counts as not
// quoted: RFC 4884 pads a quote to 128 bytes, also one that stopped after 8.
func quoteMatches(l4, data []byte, requireNonce bool) bool {
	var q []byte
	if len(l4) > 8 {
		q = l4[8:]
	}
	n := min(len(q), len(data))
	if requireNonce && n < nonceEnd {
		return false
	}
	q = q[:n]
	if bytes.Equal(q, data[:n]) {
		return true
	}
	return !requireNonce && !slices.ContainsFunc(q, func(c byte) bool { return c != 0 })
}

// RFC 4884 extension structure, RFC 4950 MPLS label stack object.
const (
	extVersion         = 2
	extMinOriginal     = 128 // original datagram is padded to at least this
	extClassMPLS       = 1
	extCTypeLabelStack = 1
)

// parseMPLS returns the MPLS label stack from the RFC 4884 extension of an
// ICMP time-exceeded or destination-unreachable message, or nil.
//
// Compliant routers state the original datagram's length (in 32-bit words
// for ICMPv4, 64-bit words for ICMPv6) and the extension follows it. Many
// routers instead leave the length at 0 and append the extension after a
// 128-byte original datagram (RFC 4884 §5); that layout is accepted only
// with a valid extension checksum, so payload bytes that merely look like an
// extension are not misread.
func parseMPLS(msg []byte, v6 bool) []record.MPLSLabel {
	if len(msg) < 8 {
		return nil
	}
	words, unit := int(msg[5]), 4
	if v6 {
		words, unit = int(msg[4]), 8
	}
	off, compat := 8+words*unit, words == 0
	if compat {
		off = 8 + extMinOriginal
	}
	if len(msg) < off+4 {
		return nil
	}
	ext := msg[off:]
	if ext[0]>>4 != extVersion {
		return nil
	}
	if sum := binary.BigEndian.Uint16(ext[2:]); (compat || sum != 0) && fold(onesSum(0, ext)) != 0xffff {
		return nil
	}
	var labels []record.MPLSLabel
	for obj := ext[4:]; len(obj) >= 4; {
		n := int(binary.BigEndian.Uint16(obj))
		if n < 4 || n > len(obj) {
			break
		}
		if obj[2] == extClassMPLS && obj[3] == extCTypeLabelStack {
			// Only the first label stack, and at most MaxMPLSLabels of it:
			// the packet comes from any router on the path (or a spoofer),
			// and real stacks are a few labels deep.
			for e := obj[4:n]; len(e) >= 4 && len(labels) < record.MaxMPLSLabels; e = e[4:] {
				v := binary.BigEndian.Uint32(e)
				labels = append(labels, record.MPLSLabel{
					Label: v >> 12,
					TC:    uint8(v>>9) & 7,
					S:     v&0x100 != 0,
					TTL:   uint8(v),
				})
			}
			return labels
		}
		obj = obj[n:]
	}
	return labels
}

// sourceMatchesSocket reports whether a reply's source address belongs to
// the family of the socket it arrived on. Some kernels deliver packets with
// IPv4-mapped sources (::ffff:a.b.c.d) on IPv6 sockets; parsed by their
// source, such a packet would be taken for an IPv4 reply.
func sourceMatchesSocket(addr netip.Addr, ipv4Socket bool) bool {
	if ipv4Socket {
		return addr.Unmap().Is4()
	}
	return addr.Is6() && !addr.Is4In6()
}
