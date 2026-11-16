package probe

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/netip"
	"strings"
	"testing"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var (
	src4 = netip.MustParseAddr("192.0.2.1")
	dst4 = netip.MustParseAddr("198.51.100.7")
	src6 = netip.MustParseAddr("2001:db8::1")
	dst6 = netip.MustParseAddr("2001:db8::2")
)

// Golden bytes below were produced by an independent Python implementation
// (struct + RFC 1071 checksum; the UDP compensation word was found by brute
// force rather than with the algebra used in buildUDP).
func TestBuildGolden(t *testing.T) {
	tests := []struct {
		name string
		got  []byte
		want string
	}{
		{"echo4 paris seq 1", buildEcho(src4, dst4, 33434, 1, true), "080056ba829a0001fffe686f70636f7264657200"},
		{"echo4 paris seq 2", buildEcho(src4, dst4, 33434, 2, true), "080056ba829a0002fffd686f70636f7264657200"},
		{"echo4 classic seq 1", buildEcho(src4, dst4, 33434, 1, false), "080056b9829a00010000686f70636f7264657200"},
		{"echo6 paris seq 7", buildEcho(src6, dst6, 33434, 7, true), "800082f6829a0007fff8686f70636f7264657200"},
		{"udp4 checksum 0x1234", buildUDP(src4, dst4, 33434, 33434, 0x1234), "829a829a00141234dd75686f70636f7264657200"},
		{"udp6 checksum 0xabcd", buildUDP(src6, dst6, 33434, 33435, 0xabcd), "829a829b0014abcdd4a2686f70636f7264657200"},
		{"ipv4 header", buildIPv4(src4, dst4, protoUDP, 5, 0x1234, 20), "45000028123400000511b755c0000201c6336407"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hex.EncodeToString(tt.got); got != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// verify returns the ones-complement sum over pseudo-header and packet,
// which is 0xffff for a packet with a correct checksum.
func verifySum(src, dst netip.Addr, proto uint8, b []byte) uint16 {
	var initial uint32
	if proto != protoICMP {
		initial = pseudoHeaderSum(src, dst, proto, len(b))
	}
	return fold(onesSum(initial, b))
}

func TestParisEchoChecksumConstant(t *testing.T) {
	for _, pair := range [][2]netip.Addr{{src4, dst4}, {src6, dst6}} {
		src, dst := pair[0], pair[1]
		proto := uint8(protoICMP)
		if dst.Is6() {
			proto = protoICMPv6
		}
		var first uint16
		for seq := uint16(1); seq < 0xffff; seq += 97 {
			b := buildEcho(src, dst, 4242, seq, true)
			if s := verifySum(src, dst, proto, b); s != 0xffff {
				t.Fatalf("%s seq %d: bad checksum (sum %#x)", dst, seq, s)
			}
			c := binary.BigEndian.Uint16(b[2:])
			if seq == 1 {
				first = c
			} else if c != first {
				t.Fatalf("%s seq %d: checksum %#x differs from %#x", dst, seq, c, first)
			}
		}
	}
}

func TestUDPChecksumIsIdentifier(t *testing.T) {
	for _, pair := range [][2]netip.Addr{{src4, dst4}, {src6, dst6}} {
		src, dst := pair[0], pair[1]
		for _, want := range []uint16{1, 2, 0x7fff, 0x8000, 0xfffd, 0xfffe} {
			b := buildUDP(src, dst, 40000, 33434, want)
			if got := binary.BigEndian.Uint16(b[6:]); got != want {
				t.Errorf("%s: checksum %#x, want %#x", dst, got, want)
			}
			if s := verifySum(src, dst, protoUDP, b); s != 0xffff {
				t.Errorf("%s want %#x: packet does not verify (sum %#x)", dst, want, s)
			}
		}
	}
}

// Messages as a router or destination would send them (after the kernel has
// stripped the outer IPv4 header).
const (
	// ICMPv4 time exceeded quoting an echo request to 198.51.100.7,
	// id 33434 seq 1. Quoted IP header: TTL 1, proto 1.
	timeExceeded4Echo = "0b00 0000 00000000" +
		"45000028 00010000 0101 0000 c0000201 c6336407" +
		"0800 56ba 829a 0001"
	// ICMPv4 port unreachable quoting a UDP probe 33434 -> 33434, csum 0x1234.
	portUnreach4UDP = "0303 0000 00000000" +
		"45000028 12340000 0111 0000 c0000201 c6336407" +
		"829a 829a 0014 1234"
	// Same, quoted IP header carries 4 bytes of options (IHL 6).
	timeExceeded4Options = "0b00 0000 00000000" +
		"4600002c 00010000 0111 0000 c0000201 c6336407 01010100" +
		"829a 829a 0014 1234"
	echoReply4 = "0000 0000 829a 0001 fffe686f70636f7264657200"

	// ICMPv6 time exceeded quoting an echo request to 2001:db8::2.
	timeExceeded6Echo = "0300 0000 00000000" +
		"60000000 0014 3a 01 20010db8000000000000000000000001 20010db8000000000000000000000002" +
		"8000 82f6 829a 0007"
	// ICMPv6 port unreachable (code 4) quoting UDP 33434 -> 33435 csum 0xabcd.
	portUnreach6UDP = "0104 0000 00000000" +
		"60000000 0014 11 01 20010db8000000000000000000000001 20010db8000000000000000000000002" +
		"829a 829b 0014 abcd"
	echoReply6 = "8100 0000 829a 0007 fff8686f70636f7264657200"
)

func TestParseICMPv4(t *testing.T) {
	router := netip.MustParseAddr("203.0.113.9")
	tests := []struct {
		name string
		from netip.Addr
		msg  string
		want icmpReply
		err  error
	}{
		{"time exceeded / echo", router, timeExceeded4Echo,
			icmpReply{kind: kindTimeExceeded, key: probeKey{dst: dst4, proto: protoICMP, a: 33434, c: 1}}, nil},
		{"port unreachable / udp", dst4, portUnreach4UDP,
			icmpReply{kind: kindUnreachable, code: 3, key: probeKey{dst: dst4, proto: protoUDP, a: 33434, b: 33434, c: 0x1234}}, nil},
		{"quoted header with options", router, timeExceeded4Options,
			icmpReply{kind: kindTimeExceeded, key: probeKey{dst: dst4, proto: protoUDP, a: 33434, b: 33434, c: 0x1234}}, nil},
		{"echo reply", dst4, echoReply4,
			icmpReply{kind: kindEchoReply, key: probeKey{dst: dst4, proto: protoICMP, a: 33434, c: 1}}, nil},
		{"echo request ignored", dst4, "0800 0000 829a 0001", icmpReply{}, errIgnored},
		{"redirect ignored", router, "0501 0000 c0000201" + "45000028 00010000 0101 0000 c0000201 c6336407 0800 56ba 829a 0001", icmpReply{}, errIgnored},
		{"reassembly timeout ignored", router, "0b01 0000 00000000" + "45000028 00010000 0101 0000 c0000201 c6336407 0800 56ba 829a 0001", icmpReply{}, errIgnored},
		{"quoted echo reply is not ours", router, "0b00 0000 00000000" + "45000028 00010000 0101 0000 c0000201 c6336407 0000 56ba 829a 0001", icmpReply{}, errIgnored},
		{"quoted tcp unsupported", router, "0b00 0000 00000000" + "45000028 00010000 0106 0000 c0000201 c6336407 829a 0050 00000001", icmpReply{}, errIgnored},
		{"quoted non-first fragment", router, "0b00 0000 00000000" + "45000028 00010010 0111 0000 c0000201 c6336407 829a 829a 0014 1234", icmpReply{}, errIgnored},
		{"quoted ipv6 in icmpv4", router, "0b00 0000 00000000" + "65000028 00010000 0111 0000 c0000201 c6336407 829a 829a 0014 1234", icmpReply{}, errMalformed},
		{"quoted ihl too small", router, "0b00 0000 00000000" + "44000028 00010000 0111 0000 c0000201 c6336407 829a 829a 0014 1234", icmpReply{}, errMalformed},
		{"quoted l4 truncated", router, "0b00 0000 00000000" + "45000028 00010000 0101 0000 c0000201 c6336407 0800 56ba", icmpReply{}, errTruncated},
		{"options push l4 past end", router, "0b00 0000 00000000" + "4f00002c 00010000 0111 0000 c0000201 c6336407 829a 829a 0014 1234", icmpReply{}, errTruncated},
		{"short header", router, "0b00 0000", icmpReply{}, errTruncated},
		{"empty", router, "", icmpReply{}, errTruncated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseICMPv4(tt.from, unhex(t, tt.msg))
			if !errors.Is(err, tt.err) {
				t.Fatalf("err = %v, want %v", err, tt.err)
			}
			if tt.err == nil && got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseICMPv6(t *testing.T) {
	router := netip.MustParseAddr("2001:db8:ffff::9")
	tests := []struct {
		name string
		from netip.Addr
		msg  string
		want icmpReply
		err  error
	}{
		{"time exceeded / echo", router, timeExceeded6Echo,
			icmpReply{kind: kindTimeExceeded, key: probeKey{dst: dst6, proto: protoICMPv6, a: 33434, c: 7}}, nil},
		{"port unreachable / udp", dst6, portUnreach6UDP,
			icmpReply{kind: kindUnreachable, code: 4, key: probeKey{dst: dst6, proto: protoUDP, a: 33434, b: 33435, c: 0xabcd}}, nil},
		{"echo reply", dst6, echoReply6,
			icmpReply{kind: kindEchoReply, key: probeKey{dst: dst6, proto: protoICMPv6, a: 33434, c: 7}}, nil},
		{"echo reply from zoned link-local", netip.MustParseAddr("fe80::1%eth0"), echoReply6,
			icmpReply{kind: kindEchoReply, key: probeKey{dst: netip.MustParseAddr("fe80::1"), proto: protoICMPv6, a: 33434, c: 7}}, nil},
		{"echo request ignored", dst6, "8000 0000 829a 0007", icmpReply{}, errIgnored},
		{"packet too big ignored", router, "0200 0000 000005dc" + strings.Repeat("00", 48), icmpReply{}, errIgnored},
		{"extension header not ours", router, "0300 0000 00000000" +
			"60000000 0014 00 01 20010db8000000000000000000000001 20010db8000000000000000000000002 3a00 0000 0000 0000", icmpReply{}, errIgnored},
		{"quoted v4 in icmpv6", router, "0300 0000 00000000" +
			"40000000 0014 3a 01 20010db8000000000000000000000001 20010db8000000000000000000000002 8000 82f6 829a 0007", icmpReply{}, errMalformed},
		{"quoted header truncated", router, "0300 0000 00000000" + "60000000 0014 3a 01 20010db8", icmpReply{}, errTruncated},
		{"quoted l4 truncated", router, "0300 0000 00000000" +
			"60000000 0014 3a 01 20010db8000000000000000000000001 20010db8000000000000000000000002 8000", icmpReply{}, errTruncated},
		{"short header", router, "03", icmpReply{}, errTruncated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseICMPv6(tt.from, unhex(t, tt.msg))
			if !errors.Is(err, tt.err) {
				t.Fatalf("err = %v, want %v", err, tt.err)
			}
			if tt.err == nil && got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// Every truncation of a valid message must fail cleanly, never panic or
// produce a key.
func TestParseTruncations(t *testing.T) {
	for _, msg := range []string{timeExceeded4Echo, portUnreach4UDP, timeExceeded4Options} {
		full := unhex(t, msg)
		for n := range len(full) {
			if _, err := parseICMPv4(dst4, full[:n]); err == nil {
				t.Errorf("v4 prefix %d of %d bytes parsed without error", n, len(full))
			}
		}
	}
	for _, msg := range []string{timeExceeded6Echo, portUnreach6UDP} {
		full := unhex(t, msg)
		for n := range len(full) {
			if _, err := parseICMPv6(dst6, full[:n]); err == nil {
				t.Errorf("v6 prefix %d of %d bytes parsed without error", n, len(full))
			}
		}
	}
}

// The quoted headers in the golden messages are what our builders produce,
// so a probe and the reply quoting it yield the same key.
func TestBuiltProbesMatchQuotedKeys(t *testing.T) {
	echo := buildEcho(src4, dst4, 33434, 1, true)
	q := append(buildIPv4(src4, dst4, protoICMP, 1, 1, len(echo)), echo[:8]...)
	msg := append(unhex(t, "0b00 0000 00000000"), q...)
	r, err := parseICMPv4(netip.MustParseAddr("203.0.113.9"), msg)
	if err != nil {
		t.Fatal(err)
	}
	if want := (probeKey{dst: dst4, proto: protoICMP, a: 33434, c: 1}); r.key != want {
		t.Errorf("key %+v, want %+v", r.key, want)
	}
}

func FuzzParseICMP(f *testing.F) {
	for _, s := range []string{timeExceeded4Echo, portUnreach4UDP, timeExceeded4Options, echoReply4,
		timeExceeded6Echo, portUnreach6UDP, echoReply6} {
		f.Add(unhex(f, s))
	}
	f.Fuzz(func(_ *testing.T, b []byte) {
		_, _ = parseICMPv4(dst4, b)
		_, _ = parseICMPv6(dst6, b)
	})
}
