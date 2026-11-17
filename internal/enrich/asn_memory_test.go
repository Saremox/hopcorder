package enrich

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/netip"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// asnFile writes an iptoasn file of ranges ranges (IPv6 unless v4) and
// infos distinct ASes, gzipped: the first infos-1 ranges each have an AS of
// their own, with a country of 8 and a name of nameLen bytes; the others
// share one AS with short fields, so the file stays within the
// uncompressed limit.
func asnFile(t testing.TB, ranges, infos, nameLen int, v4 bool) []byte {
	t.Helper()
	var raw bytes.Buffer
	gw, _ := gzip.NewWriterLevel(&raw, gzip.BestSpeed)
	var line []byte
	for i := range ranges {
		var a string
		if v4 {
			n := uint32(0x01000000 + 2*i) //nolint:gosec // small
			a = netip.AddrFrom4([4]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}).String()
		} else {
			a = fmt.Sprintf("2001:%x:%x::", i>>16, i&0xffff)
		}
		if i < infos-1 {
			line = fmt.Appendf(line[:0], "%s\t%s\t%d\tC%07d\t%0*d\n", a, a, 1+i, i%10000000, nameLen, i)
		} else {
			line = fmt.Appendf(line[:0], "%s\t%s\t%d\tZZ\tX\n", a, a, infos)
		}
		_, _ = gw.Write(line)
	}
	_ = gw.Close()
	return raw.Bytes()
}

// peakHeap runs f and returns the largest HeapInuse sampled meanwhile.
func peakHeap(f func()) uint64 {
	var peak atomic.Uint64
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peak.Load() {
				peak.Store(m.HeapInuse)
			}
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	f()
	close(done)
	<-sampled
	return peak.Load()
}

// The bounds that keep a database's memory in check: ranges, distinct ASes
// and the bytes of their names (lowered for the test).
func TestParseMemoryBounds(t *testing.T) {
	defer func(r, i, x int) { maxASNRanges, maxASNInfos, maxASNText = r, i, x }(maxASNRanges, maxASNInfos, maxASNText)
	maxASNRanges, maxASNInfos, maxASNText = 1000, 100, 100*(maxCountryLen+20)
	for _, tc := range []struct {
		name                   string
		ranges, infos, nameLen int
		v4                     bool
		err                    string
	}{
		{"at the limits", 1000, 100, 20, true, ""},
		{"at the limits, IPv6", 1000, 100, 20, false, ""},
		{"ranges", 1001, 100, 20, true, "more than 1000 ranges"},
		{"ASes", 1000, 101, 10, true, "more than 100 distinct ASes"},
		{"names", 1000, 100, 21, true, "of AS names"},
	} {
		db, err := ParseIPToASN(bytes.NewReader(asnFile(t, tc.ranges, tc.infos, tc.nameLen, tc.v4)))
		switch {
		case tc.err == "" && (err != nil || db.Len() != tc.ranges):
			t.Errorf("%s: %v", tc.name, err)
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s: %v, want %q", tc.name, err, tc.err)
		}
	}
}

// A database is compact: about 12 bytes per IPv4 range, 40 per IPv6 range
// and the AS names in one string. Before, a range took 56 bytes and every
// AS 40 bytes and two strings, which with the old bounds let a 45 MiB
// download peak at 1.35 GB. Every range is still found.
func TestASNDatabaseIsCompact(t *testing.T) {
	const ranges, infos = 100_000, 20_000
	raw4, raw6 := asnFile(t, ranges, infos, 30, true), asnFile(t, ranges, infos, 30, false)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	db4, err4 := ParseIPToASN(bytes.NewReader(raw4))
	db6, err6 := ParseIPToASN(bytes.NewReader(raw6))
	if err4 != nil || err6 != nil {
		t.Fatal(err4, err6)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	kept := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	// 12 + 40 bytes per range pair, 2 x (12 + 38) per AS, and slack for
	// slices grown by appending.
	want := int64(ranges*(12+40)+2*infos*(12+38)) * 3 / 2
	t.Logf("%d IPv4 and %d IPv6 ranges, %d ASes each: %d bytes (%d per range)", ranges, ranges, infos, kept, kept/(2*ranges))
	if kept > want {
		t.Errorf("the databases keep %d bytes, want at most %d", kept, want)
	}
	for i := 0; i < ranges; i += 997 {
		n := uint32(0x01000000 + 2*i) //nolint:gosec // small
		a4 := netip.AddrFrom4([4]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
		a6 := netip.MustParseAddr(fmt.Sprintf("2001:%x:%x::", i>>16, i&0xffff))
		wantASN := uint32(min(i+1, infos)) //nolint:gosec // small
		for _, c := range []struct {
			db *ASNDB
			a  netip.Addr
		}{{db4, a4}, {db6, a6}} {
			if info, ok := c.db.Lookup(c.a); !ok || info.ASN != wantASN || (i < infos-1) != (len(info.Name) == 30) {
				t.Fatalf("Lookup(%s) = %+v %v, want AS%d", c.a, info, ok, wantASN)
			}
			if _, ok := c.db.Lookup(c.a.Next()); ok { // the gap after each range
				t.Fatalf("Lookup(%s) found a range", c.a.Next())
			}
		}
	}
	runtime.KeepAlive(db4)
	runtime.KeepAlive(db6)
}

// The highest addresses compare as unsigned numbers.
func TestLookupTopOfAddressSpace(t *testing.T) {
	db, err := ParseIPToASN(strings.NewReader("255.255.255.0\t255.255.255.255\t64496\tZZ\tTOP4\n" +
		"128.0.0.0\t128.0.0.255\t64497\tZZ\tMID4\n" +
		"ffff::\tffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff\t64498\tZZ\tTOP6\n" +
		"8000::\t8000::ff\t64499\tZZ\tMID6\n"))
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]string{"255.255.255.255": "TOP4", "255.255.255.0": "TOP4", "128.0.0.7": "MID4",
		"127.255.255.255": "", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff": "TOP6", "ffff::1": "TOP6",
		"8000::7": "MID6", "7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff": "", "8000::100": ""} {
		if info, _ := db.Lookup(netip.MustParseAddr(ip)); info.Name != want {
			t.Errorf("Lookup(%s) = %+v, want %q", ip, info, want)
		}
	}
}

// The worst database the bounds let through, parsed while another one is
// in use, as a refresh does: the heap must stay well inside the Helm
// chart's 1 GiB container limit (a 5-million-range file used to peak at
// 1.35 GB and crash-loop the server). Slow, so only with
// HOPCORDER_ASN_MEMTEST=1; HOPCORDER_ASN_GOGC sets GOGC (default 100; 5
// shows the live heap).
func TestASNWorstCaseMemory(t *testing.T) {
	if os.Getenv("HOPCORDER_ASN_MEMTEST") == "" {
		t.Skip("set HOPCORDER_ASN_MEMTEST=1")
	}
	gc := 100
	if v := os.Getenv("HOPCORDER_ASN_GOGC"); v != "" {
		_, _ = fmt.Sscan(v, &gc)
	}
	defer debug.SetGCPercent(debug.SetGCPercent(gc))
	// IPv6 ranges are the larger ones; the names fill maxASNText.
	ranges, infos := maxASNRanges, maxASNInfos
	if v := os.Getenv("HOPCORDER_ASN_RANGES"); v != "" {
		_, _ = fmt.Sscan(v, &ranges, &infos)
	}
	nameLen := min(maxASNText/infos-maxCountryLen, maxASNameLen)
	raw := asnFile(t, ranges, infos, nameLen, false)
	t.Logf("%d ranges, %d ASes with %d-byte names: %d MiB gzipped", ranges, infos, nameLen, len(raw)>>20)
	if len(raw) > maxASNDownload {
		t.Fatalf("the file is larger than a download may be")
	}
	old, err := ParseIPToASN(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	// A download is held in memory while it is parsed: account for one of
	// the largest size.
	download := make([]byte, maxASNDownload-len(raw))
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	t.Logf("one database in use, and a %d MiB download: heap %d MiB", maxASNDownload>>20, base.HeapInuse>>20)

	var db *ASNDB
	start := time.Now()
	peak := peakHeap(func() {
		if db, err = ParseIPToASN(bytes.NewReader(raw)); err != nil {
			t.Error(err)
		}
	})
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("parsed %d ranges in %s while the old one was in use: peak heap %d MiB, then %d MiB",
		db.Len(), time.Since(start).Round(time.Millisecond), peak>>20, after.HeapInuse>>20)
	if peak > 512<<20 {
		t.Errorf("peak heap %d MiB, want well inside 1 GiB", peak>>20)
	}
	if info, ok := db.Lookup(netip.MustParseAddr("2001:0:7::")); !ok || info.ASN != 8 || !strings.HasSuffix(info.Name, "7") {
		t.Errorf("lookup: %+v %v", info, ok)
	}
	runtime.KeepAlive(old)
	runtime.KeepAlive(raw)
	runtime.KeepAlive(download)
}
