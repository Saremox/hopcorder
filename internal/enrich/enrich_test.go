package enrich

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A slice of the real ip2asn-combined.tsv layout.
const asnFixture = "1.0.0.0\t1.0.0.255\t13335\tUS\tCLOUDFLARENET\n" +
	"1.0.1.0\t1.0.3.255\t0\tNone\tNot routed\n" +
	"62.115.0.0\t62.115.255.255\t1299\tSE\tTWELVE99 Arelion, fka Telia Carrier\n" +
	"79.99.80.0\t79.99.87.255\t64500\tDE\tEXAMPLE-DC\n" +
	"2001:2000::\t2001:2003:ffff:ffff:ffff:ffff:ffff:ffff\t1299\tSE\tTWELVE99 Arelion, fka Telia Carrier\n" +
	"2606:4700::\t2606:4700:ffff:ffff:ffff:ffff:ffff:ffff\t13335\tUS\tCLOUDFLARENET\n"

func gz(t *testing.T, s string) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, _ = w.Write([]byte(s))
	_ = w.Close()
	return b.Bytes()
}

func TestParseAndLookup(t *testing.T) {
	for name, input := range map[string][]byte{"plain": []byte(asnFixture), "gzip": gz(t, asnFixture)} {
		t.Run(name, func(t *testing.T) {
			db, err := ParseIPToASN(bytes.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			if db.Len() != 5 {
				t.Errorf("%d ranges, want 5 (the unrouted one is skipped)", db.Len())
			}
			tests := []struct {
				ip   string
				asn  uint32
				name string
			}{
				{"1.0.0.0", 13335, "CLOUDFLARENET"},
				{"1.0.0.255", 13335, "CLOUDFLARENET"},
				{"1.0.2.1", 0, ""}, // not routed
				{"62.115.120.1", 1299, "TWELVE99 Arelion, fka Telia Carrier"},
				{"79.99.80.226", 64500, "EXAMPLE-DC"},
				{"79.99.88.0", 0, ""}, // just past a range
				{"0.0.0.1", 0, ""},    // before the first
				{"255.1.1.1", 0, ""},  // after the last
				{"::ffff:62.115.1.1", 1299, "TWELVE99 Arelion, fka Telia Carrier"},
				{"2001:2002::1", 1299, "TWELVE99 Arelion, fka Telia Carrier"},
				{"2606:4700:4700::1111", 13335, "CLOUDFLARENET"},
				{"2a00::1", 0, ""},
			}
			for _, tt := range tests {
				got, ok := db.Lookup(netip.MustParseAddr(tt.ip))
				if got.ASN != tt.asn || got.Name != tt.name || ok != (tt.asn != 0) {
					t.Errorf("Lookup(%s) = %+v %v, want AS%d %q", tt.ip, got, ok, tt.asn, tt.name)
				}
			}
		})
	}
	var nilDB *ASNDB
	if _, ok := nilDB.Lookup(netip.MustParseAddr("1.0.0.1")); ok {
		t.Error("nil database found something")
	}
}

func TestParseRejects(t *testing.T) {
	for _, in := range []string{
		"1.0.0.0\t1.0.0.255\t13335\tUS\n",
		"x\t1.0.0.255\t13335\tUS\tX\n",
		"1.0.0.9\t1.0.0.1\t13335\tUS\tX\n",
		"1.0.0.0\t::1\t13335\tUS\tX\n",
		"1.0.1.0\t1.0.3.255\t0\tNone\tNot routed\n",
		"",
	} {
		if _, err := ParseIPToASN(strings.NewReader(in)); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestASNSourceDownloadAndCache(t *testing.T) {
	var fail atomic.Bool
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if fail.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write(gz(t, asnFixture))
	}))
	defer srv.Close()
	cache := filepath.Join(t.TempDir(), "sub", "ip2asn.tsv.gz")
	s := &ASNSource{URL: srv.URL, File: cache, Refresh: time.Hour}
	s.refresh(t.Context())
	if info, ok := s.Lookup(netip.MustParseAddr("62.115.1.1")); !ok || info.ASN != 1299 {
		t.Fatalf("after download: %+v %v", info, ok)
	}
	if _, err := os.Stat(cache); err != nil {
		t.Fatalf("not cached: %v", err)
	}
	s.refresh(t.Context()) // fresh: no new download
	if hits.Load() != 1 {
		t.Errorf("%d downloads, want 1", hits.Load())
	}

	// A new process starts from the cache even if the source is down.
	fail.Store(true)
	s2 := &ASNSource{URL: srv.URL, File: cache, Refresh: time.Hour}
	s2.refresh(t.Context())
	if _, ok := s2.Lookup(netip.MustParseAddr("1.0.0.1")); !ok {
		t.Error("cached database not used")
	}
	// A stale cache plus a failing download keeps the stale database.
	old := time.Now().Add(-48 * time.Hour)
	_ = os.Chtimes(cache, old, old)
	s3 := &ASNSource{URL: srv.URL, File: cache, Refresh: time.Hour}
	s3.refresh(t.Context())
	if _, ok := s3.Lookup(netip.MustParseAddr("1.0.0.1")); !ok || s3.Age() < 47*time.Hour {
		t.Errorf("stale database dropped (age %v)", s3.Age())
	}
}

func TestASNSourceFileOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ip2asn.tsv")
	if err := os.WriteFile(path, []byte("1.0.0.0\t1.0.0.255\t13335\tUS\tCLOUDFLARENET\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &ASNSource{File: path}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.Refresh = time.Hour
	s.refresh(ctx)
	if info, _ := s.Lookup(netip.MustParseAddr("1.0.0.1")); info.ASN != 13335 {
		t.Fatalf("got %+v", info)
	}
	// The operator replaces the file; the next check picks it up.
	if err := os.WriteFile(path, []byte("1.0.0.0\t1.0.0.255\t64496\tZZ\tREPLACED\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Minute)
	_ = os.Chtimes(path, future, future)
	s.refresh(ctx)
	if info, _ := s.Lookup(netip.MustParseAddr("1.0.0.1")); info.ASN != 64496 {
		t.Errorf("replaced file not reloaded: %+v", info)
	}
}

type fakePTR struct {
	mu    sync.Mutex
	names map[string][]string
	calls map[string]int
	block chan struct{}
}

func (f *fakePTR) LookupAddr(ctx context.Context, addr string) ([]string, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[addr]++
	if n, ok := f.names[addr]; ok {
		return n, nil
	}
	return nil, errors.New("NXDOMAIN")
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRDNS(t *testing.T) {
	res := &fakePTR{names: map[string][]string{"62.115.120.1": {"ffm-bb1-link.ip.twelve99.net."}}, calls: map[string]int{}}
	r := &RDNS{Resolver: res, NegativeTTL: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)

	hop := netip.MustParseAddr("62.115.120.1")
	if n := r.Name(hop); n != "" {
		t.Errorf("first sight returned %q; lookups must not block", n)
	}
	eventually(t, func() bool { return r.Name(hop) == "ffm-bb1-link.ip.twelve99.net" })
	for range 10 {
		r.Name(hop)
	}
	res.mu.Lock()
	calls := res.calls["62.115.120.1"]
	res.mu.Unlock()
	if calls != 1 {
		t.Errorf("%d lookups for a cached name", calls)
	}

	// No PTR: cached negatively, retried after the negative TTL.
	none := netip.MustParseAddr("10.9.9.9")
	r.Name(none)
	eventually(t, func() bool { return r.Stats().Lookups >= 2 })
	r.Name(none)
	time.Sleep(80 * time.Millisecond)
	r.Name(none)
	eventually(t, func() bool { res.mu.Lock(); defer res.mu.Unlock(); return res.calls["10.9.9.9"] == 2 })
	if st := r.Stats(); st.Entries != 2 || st.Hits == 0 || st.Misses == 0 {
		t.Errorf("stats %+v", st)
	}
	if r.Name(netip.Addr{}) != "" {
		t.Error("invalid address resolved")
	}
}

func TestRDNSQueueFullDrops(t *testing.T) {
	res := &fakePTR{names: map[string][]string{}, calls: map[string]int{}, block: make(chan struct{})}
	r := &RDNS{Resolver: res, Workers: 1}
	r.init()
	for i := range 5000 {
		r.Name(netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}))
	}
	if r.Stats().Dropped == 0 {
		t.Error("a full queue must drop rather than block")
	}
	close(res.block)
}

func TestRDNSEviction(t *testing.T) {
	res := &fakePTR{names: map[string][]string{}, calls: map[string]int{}}
	r := &RDNS{Resolver: res, MaxEntries: 20}
	r.init()
	for i := range 50 {
		r.resolve(t.Context(), netip.AddrFrom4([4]byte{10, 0, 0, byte(i)}))
	}
	if n := r.Stats().Entries; n > 20 {
		t.Errorf("%d entries with MaxEntries 20", n)
	}
}

// The database is a download: its size, range count and fields are bounded.
func TestParseBounds(t *testing.T) {
	// A small gzip that inflates past the uncompressed limit (lowered for
	// the test).
	defer func(n int64) { maxASNDecoded = n }(maxASNDecoded)
	maxASNDecoded = 4 << 20
	var bomb bytes.Buffer
	gw, _ := gzip.NewWriterLevel(&bomb, gzip.BestCompression)
	// Blank lines of 32 KiB: skipped by the parser, cheap to scan.
	chunk := bytes.Repeat(append(bytes.Repeat([]byte(" "), 32<<10-1), '\n'), 32)
	for range maxASNDecoded>>20 + 1 {
		_, _ = gw.Write(chunk)
	}
	_ = gw.Close()
	if bomb.Len() > 64<<10 {
		t.Fatalf("bomb is %d bytes; the test wants a small one", bomb.Len())
	}
	if _, err := ParseIPToASN(bytes.NewReader(bomb.Bytes())); err == nil || !strings.Contains(err.Error(), "uncompressed") {
		t.Errorf("gzip bomb: %v", err)
	}

	// Control characters are dropped and names truncated.
	hostile := "198.51.100.0\t198.51.100.255\t64496\tZZ\t<img src=x>\x1b[31mEVIL\x00" + strings.Repeat("A", 500) + "\n"
	db, err := ParseIPToASN(strings.NewReader(hostile))
	if err != nil {
		t.Fatal(err)
	}
	info, ok := db.Lookup(netip.MustParseAddr("198.51.100.7"))
	if !ok || len(info.Name) > maxASNameLen || strings.ContainsAny(info.Name, "\x1b\x00") || !strings.HasPrefix(info.Name, "<img src=x>[31mEVIL") {
		t.Errorf("name %q (%d bytes)", info.Name, len(info.Name))
	}
}

// A redirect from https to plain http is not followed.
func TestASNDownloadRefusesDowngrade(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(asnFixture))
	}))
	defer plain.Close()
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/db.tsv", http.StatusFound)
	}))
	defer tlsSrv.Close()
	hc := tlsSrv.Client()
	hc.CheckRedirect = noDowngrade
	s := &ASNSource{URL: tlsSrv.URL, HTTP: hc}
	s.defaults()
	if err := s.download(t.Context()); err == nil || !strings.Contains(err.Error(), "refusing redirect") {
		t.Errorf("downgrade followed: %v", err)
	}
	// Redirects that stay on https are fine.
	if err := noDowngrade(httptest.NewRequest(http.MethodGet, "https://b.example/x", nil),
		[]*http.Request{httptest.NewRequest(http.MethodGet, "https://a.example/", nil)}); err != nil {
		t.Errorf("https to https: %v", err)
	}
}
