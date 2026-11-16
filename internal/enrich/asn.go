// Package enrich adds what the agent deliberately does not: the AS number
// and name of every hop address (from a local iptoasn database refreshed
// daily) and its reverse DNS name (cached, resolved in the background).
package enrich

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// ASNInfo is the origin AS of an address.
type ASNInfo struct {
	ASN     uint32
	Name    string
	Country string
}

type asnRange struct {
	start, end netip.Addr
	info       int32 // index into ASNDB.infos
}

// ASNDB maps address ranges to origin ASes.
type ASNDB struct {
	v4, v6 []asnRange
	infos  []ASNInfo
}

// Limits on an ASN database, whose source is a download: the real file is
// about 7 MB gzipped, 30 MB and 600,000 ranges uncompressed.
const (
	maxASNDownload = 64 << 20 // compressed
	maxASNRanges   = 5_000_000
	maxASNameLen   = 128
	maxCountryLen  = 8
)

var maxASNDecoded int64 = 256 << 20 // uncompressed; a variable for tests

// ParseIPToASN reads the iptoasn.com TSV format (ip2asn-combined.tsv,
// optionally gzipped): range_start, range_end, AS number, country, AS
// description. Ranges with AS 0 ("Not routed") are skipped. Its size,
// range count and field lengths are bounded, and control characters are
// removed from names.
func ParseIPToASN(r io.Reader) (*ASNDB, error) {
	br := bufio.NewReader(r)
	if magic, err := br.Peek(2); err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("asn db: %w", err)
		}
		defer gz.Close()
		br = bufio.NewReader(gz)
	}
	limited := &io.LimitedReader{R: br, N: maxASNDecoded + 1}
	br = bufio.NewReader(limited)
	db := &ASNDB{}
	index := map[ASNInfo]int32{}
	sc := bufio.NewScanner(br)
	sc.Buffer(make([]byte, 64<<10), 64<<10)
	line := 0
	for sc.Scan() {
		line++
		if len(db.v4)+len(db.v6) >= maxASNRanges {
			return nil, fmt.Errorf("asn db: more than %d ranges", maxASNRanges)
		}
		f := strings.Split(sc.Text(), "\t")
		if len(f) < 5 {
			if strings.TrimSpace(sc.Text()) == "" {
				continue
			}
			return nil, fmt.Errorf("asn db line %d: want 5 tab-separated fields", line)
		}
		start, err1 := netip.ParseAddr(f[0])
		end, err2 := netip.ParseAddr(f[1])
		asn, err3 := strconv.ParseUint(f[2], 10, 32)
		if err := errors.Join(err1, err2, err3); err != nil {
			return nil, fmt.Errorf("asn db line %d: %w", line, err)
		}
		start, end = start.Unmap(), end.Unmap()
		if start.Is4() != end.Is4() || end.Less(start) {
			return nil, fmt.Errorf("asn db line %d: bad range %s-%s", line, start, end)
		}
		if asn == 0 {
			continue
		}
		info := ASNInfo{ASN: uint32(asn), Country: cleanField(f[3], maxCountryLen), Name: cleanField(f[4], maxASNameLen)}
		idx, ok := index[info]
		if !ok {
			idx = int32(len(db.infos)) //nolint:gosec // far fewer than 2^31 ASes
			db.infos = append(db.infos, info)
			index[info] = idx
		}
		rg := asnRange{start: start, end: end, info: idx}
		if start.Is4() {
			db.v4 = append(db.v4, rg)
		} else {
			db.v6 = append(db.v6, rg)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("asn db: %w", err)
	}
	if limited.N <= 0 {
		return nil, fmt.Errorf("asn db: larger than %d MiB uncompressed", maxASNDecoded>>20)
	}
	if len(db.v4)+len(db.v6) == 0 {
		return nil, errors.New("asn db: no routed ranges")
	}
	byStart := func(a, b asnRange) int { return a.start.Compare(b.start) }
	slices.SortFunc(db.v4, byStart)
	slices.SortFunc(db.v6, byStart)
	return db, nil
}

// Lookup returns the origin AS of ip.
func (db *ASNDB) Lookup(ip netip.Addr) (ASNInfo, bool) {
	if db == nil || !ip.IsValid() {
		return ASNInfo{}, false
	}
	ip = ip.Unmap().WithZone("")
	ranges := db.v6
	if ip.Is4() {
		ranges = db.v4
	}
	// The last range starting at or before ip.
	i, found := slices.BinarySearchFunc(ranges, ip, func(r asnRange, ip netip.Addr) int { return r.start.Compare(ip) })
	if !found {
		i--
	}
	if i < 0 || ranges[i].end.Less(ip) {
		return ASNInfo{}, false
	}
	return db.infos[ranges[i].info], true
}

// Len returns the number of ranges.
func (db *ASNDB) Len() int { return len(db.v4) + len(db.v6) }

// ASNSource keeps an ASNDB current. With URL set it downloads the database
// every Refresh and keeps a copy in File, which is used at startup and
// whenever a download fails. With only File set, the file is reloaded when
// it changes (the operator updates it).
type ASNSource struct {
	URL     string
	File    string
	Refresh time.Duration
	HTTP    *http.Client
	Logger  *slog.Logger

	db       atomic.Pointer[ASNDB]
	loadedAt atomic.Int64 // unix seconds of the database in use
	modTime  time.Time
}

// Lookup returns the origin AS of ip from the current database.
func (s *ASNSource) Lookup(ip netip.Addr) (ASNInfo, bool) { return s.db.Load().Lookup(ip) }

// Age returns how old the database in use is, or -1 if none is loaded.
func (s *ASNSource) Age() time.Duration {
	at := s.loadedAt.Load()
	if at == 0 {
		return -1
	}
	return time.Since(time.Unix(at, 0))
}

// cleanField removes control characters and invalid UTF-8 from a field of
// the downloaded database and truncates it to limit bytes. The result is a
// copy: a substring would keep the whole line (up to 64 KiB) in memory for
// as long as the database is in use.
func cleanField(s string, limit int) string {
	s = strings.Map(func(r rune) rune {
		if r < ' ' || r == 0x7f || r == utf8.RuneError {
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, ""))
	if len(s) > limit {
		s = strings.ToValidUTF8(s[:limit], "")
	}
	return strings.Clone(strings.TrimSpace(s))
}

func (s *ASNSource) defaults() {
	if s.Refresh <= 0 {
		s.Refresh = 24 * time.Hour
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	if s.HTTP == nil {
		s.HTTP = &http.Client{Timeout: 5 * time.Minute, CheckRedirect: noDowngrade}
	}
}

// Run loads the database and keeps it fresh until ctx ends.
func (s *ASNSource) Run(ctx context.Context) {
	s.defaults()
	s.refresh(ctx)
	check := min(s.Refresh, time.Hour)
	t := time.NewTicker(check)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.refresh(ctx)
		}
	}
}

func (s *ASNSource) refresh(ctx context.Context) {
	s.defaults()
	log := s.Logger.With("component", "asn")
	// The cached or operator-managed file first: fast, and works offline.
	if s.File != "" {
		if st, err := os.Stat(s.File); err == nil && !st.ModTime().Equal(s.modTime) {
			if err := s.loadFile(); err != nil {
				log.Warn("cannot load ASN database file", "file", s.File, "err", err)
			} else {
				s.modTime = st.ModTime()
				s.loadedAt.Store(st.ModTime().Unix())
				log.Info("ASN database loaded", "file", s.File, "ranges", s.db.Load().Len(), "age", s.Age().Round(time.Minute))
			}
		}
	}
	if s.URL == "" || (s.db.Load() != nil && s.Age() < s.Refresh) {
		return
	}
	if err := s.download(ctx); err != nil {
		log.Warn("ASN database download failed; keeping the current one", "url", s.URL, "err", err)
		return
	}
	log.Info("ASN database downloaded", "url", s.URL, "ranges", s.db.Load().Len())
}

func (s *ASNSource) loadFile() error {
	f, err := os.Open(s.File)
	if err != nil {
		return err
	}
	defer f.Close()
	db, err := ParseIPToASN(f)
	if err != nil {
		return err
	}
	s.db.Store(db)
	return nil
}

func (s *ASNSource) download(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return err
	}
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxASNDownload+1))
	if err != nil {
		return err
	}
	if len(raw) > maxASNDownload {
		return fmt.Errorf("download larger than %d MiB", maxASNDownload>>20)
	}
	db, err := ParseIPToASN(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	s.db.Store(db)
	s.loadedAt.Store(time.Now().Unix())
	if s.File != "" {
		if err := writeFileAtomic(s.File, raw); err != nil {
			s.Logger.Warn("cannot cache ASN database", "file", s.File, "err", err)
		} else if st, err := os.Stat(s.File); err == nil {
			s.modTime = st.ModTime()
		}
	}
	return nil
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".asn-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// noDowngrade follows redirects, but not from https to plain http.
func noDowngrade(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if via[0].URL.Scheme == "https" && req.URL.Scheme != "https" {
		return fmt.Errorf("refusing redirect from https to %s", req.URL.Redacted())
	}
	return nil
}
