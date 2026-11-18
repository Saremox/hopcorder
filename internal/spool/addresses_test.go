package spool

import (
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// The agent's target addresses survive a restart; saving one a key has
// already commits nothing; KeepAddresses deletes the others; records, seq
// and cursors are not touched.
func TestAddressesPersist(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "c")
	if m, err := s.Addresses(); err != nil || len(m) != 0 {
		t.Fatalf("a new spool's addresses: %v, %v", m, err)
	}
	appendN(t, s, 3)
	before := s.Stats()
	want := map[string]netip.Addr{
		"ip a.example":  netip.MustParseAddr("192.0.2.1"),
		"ip6 a.example": netip.MustParseAddr("fe80::1%eth0"),
		"ip4 b.example": netip.MustParseAddr("192.0.2.2"),
	}
	for k, a := range want {
		if err := s.SaveAddress(k, a); err != nil {
			t.Fatal(err)
		}
	}
	gen := s.txGen
	if err := s.SaveAddress("ip a.example", want["ip a.example"]); err != nil {
		t.Fatal(err)
	}
	if s.txGen != gen {
		t.Error("saving an address the key has already committed a transaction")
	}
	if err := s.SaveAddress("ip4 b.example", netip.Addr{}); err == nil {
		t.Error("the zero address was saved")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, dir, "c")
	defer s.Close()
	got, err := s.Addresses()
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("after reopening: %v, %v; want %v", got, err, want)
	}
	if st := s.Stats(); st.LastSeq != before.LastSeq || st.Records != before.Records || s.Pending("c") != 3 {
		t.Errorf("stats %+v, pending %d; before the addresses %+v, 3 pending", st, s.Pending("c"), before)
	}

	gen = s.txGen
	if err := s.KeepAddresses([]string{"ip a.example", "ip6 a.example", "ip x.example"}); err != nil {
		t.Fatal(err)
	}
	if s.txGen == gen {
		t.Error("KeepAddresses committed nothing")
	}
	gen = s.txGen
	if err := s.KeepAddresses([]string{"ip a.example", "ip6 a.example"}); err != nil {
		t.Fatal(err)
	}
	if s.txGen != gen {
		t.Error("KeepAddresses with nothing to delete committed a transaction")
	}
	if err := s.SaveAddress("ip a.example", netip.MustParseAddr("192.0.2.9")); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Addresses()
	if keys := slices.Sorted(maps.Keys(got)); !slices.Equal(keys, []string{"ip a.example", "ip6 a.example"}) ||
		got["ip a.example"] != netip.MustParseAddr("192.0.2.9") {
		t.Errorf("after KeepAddresses and a change: %v", got)
	}
	if err := s.KeepAddresses(nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Addresses(); len(got) != 0 {
		t.Errorf("after KeepAddresses(nil): %v", got)
	}
}

// A spool that has never saved an address has no bucket for them (so the
// files of earlier versions, and the damage corpus, are as they were), and
// a value that does not decode is left out.
func TestAddressesBucket(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "c")
	appendN(t, s, 2)
	if err := s.KeepAddresses([]string{"x"}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketAddresses) != nil {
			t.Error("a spool without addresses has the bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAddress("ip a.example", netip.MustParseAddr("192.0.2.1")); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAddresses).Put([]byte("ip b.example"), []byte{1, 2, 3})
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Addresses(); err != nil || len(got) != 1 || got["ip a.example"] != netip.MustParseAddr("192.0.2.1") {
		t.Errorf("addresses %v, %v; want only a.example's", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Addresses(); err == nil {
		t.Error("Addresses of a closed spool")
	}
	if err := s.SaveAddress("ip a.example", netip.MustParseAddr("192.0.2.2")); err == nil {
		t.Error("SaveAddress to a closed spool")
	}
}

// At the size bound, saving an address makes room as any operation does,
// and the file keeps to the bound.
func TestAddressAtTheSizeBound(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: MinMaxBytes, Consumers: []string{"c"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := s.Stats().LastSeq
	appendNoise(t, s, 1500, 1300)
	for i := range 200 {
		ip := netip.AddrFrom4([4]byte{192, 0, 2, byte(i)})
		if err := s.SaveAddress("ip "+ip.String()+".example", ip); err != nil {
			t.Fatalf("save %d at the bound: %v", i, err)
		}
	}
	appendNoise(t, s, 100, 1300)
	checkNewestKept(t, s, "c", first)
	if got, err := s.Addresses(); err != nil || len(got) != 200 {
		t.Errorf("%d addresses (%v), want 200", len(got), err)
	}
	fi, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > MinMaxBytes {
		t.Errorf("file %d bytes, over the bound of %d", fi.Size(), MinMaxBytes)
	}
}
