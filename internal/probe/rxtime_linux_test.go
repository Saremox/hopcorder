package probe

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// cmsg builds one control message as the kernel lays it out on 64-bit
// Linux: cmsghdr{len uint64, level int32, type int32} followed by data.
func cmsg(level, typ int, data []byte) []byte {
	b := make([]byte, syscall.CmsgSpace(len(data)))
	binary.NativeEndian.PutUint64(b[0:], uint64(syscall.CmsgLen(len(data))))
	binary.NativeEndian.PutUint32(b[8:], uint32(level))
	binary.NativeEndian.PutUint32(b[12:], uint32(typ))
	copy(b[syscall.CmsgLen(0):], data)
	return b
}

func TestKernelRxTime(t *testing.T) {
	if strconv.IntSize != 64 || syscall.SizeofCmsghdr != 16 {
		t.Skip("control-message layout below is for 64-bit Linux")
	}
	ts := make([]byte, 16)
	binary.NativeEndian.PutUint64(ts[0:], 1_800_000_000)
	binary.NativeEndian.PutUint64(ts[8:], 123_456_789)
	want := time.Unix(1_800_000_000, 123_456_789)

	other := cmsg(syscall.IPPROTO_IPV6, 52, []byte{1, 2, 3, 4}) // e.g. IPV6_HOPLIMIT
	oob := slices.Concat(other, cmsg(syscall.SOL_SOCKET, syscall.SCM_TIMESTAMPNS, ts))
	got, ok := kernelRxTime(oob)
	if !ok || !got.Equal(want) {
		t.Errorf("kernelRxTime = %v, %v; want %v", got, ok, want)
	}
	if _, ok := kernelRxTime(other); ok {
		t.Error("timestamp found where there is none")
	}
	if _, ok := kernelRxTime(nil); ok {
		t.Error("timestamp found in empty ancillary data")
	}
	if _, ok := kernelRxTime(oob[:7]); ok {
		t.Error("timestamp found in truncated ancillary data")
	}
}

func TestStripIPv4Header(t *testing.T) {
	icmp := []byte{11, 0, 0, 0, 0, 0, 0, 0}
	hdr := buildIPv4(src4, dst4, protoICMP, 64, 1, len(icmp))
	if got := stripIPv4Header(append(hdr, icmp...)); !bytes.Equal(got, icmp) {
		t.Errorf("got % x", got)
	}
	withOpts := append([]byte{0x46}, hdr[1:]...)
	withOpts = append(withOpts, 1, 1, 1, 0) // 4 bytes of options
	if got := stripIPv4Header(append(withOpts, icmp...)); !bytes.Equal(got, icmp) {
		t.Errorf("with options: got % x", got)
	}
	for _, b := range [][]byte{icmp, hdr[:10], append([]byte{0x4f}, hdr[1:]...)} {
		if got := stripIPv4Header(b); !bytes.Equal(got, b) {
			t.Errorf("implausible header % x was stripped", b)
		}
	}
}
