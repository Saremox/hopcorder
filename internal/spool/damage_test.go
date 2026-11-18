package spool

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// These tests damage spool files and open them in a child process
// (TestDamagedChild), so that a regression (a fatal fault, a stack overflow,
// a hang, memory without bound) fails the test instead of killing or
// stalling the test binary. The child bounds its own run time and heap.

const childTimeout = 20 * time.Second

// pageView reads a bbolt file's structure the way checkPages does, to aim
// damage at particular pages.
type pageView struct {
	b  []byte
	ps int
}

func (v pageView) page(id uint64) []byte  { return v.b[int(id)*v.ps:] }
func (v pageView) flags(id uint64) uint16 { return ne.Uint16(v.page(id)[8:]) }
func (v pageView) count(id uint64) int    { return int(ne.Uint16(v.page(id)[10:])) }
func (v pageView) elem(id uint64, i int) []byte {
	return v.page(id)[pageHeaderSize+i*elementSize:]
}

// key returns element i's key on page id, within v.b.
func (v pageView) key(id uint64, i int) []byte {
	e := v.elem(id, i)
	pos, ksize := ne.Uint32(e[4:]), ne.Uint32(e[8:])
	if v.flags(id) == flagBranch {
		pos, ksize = ne.Uint32(e[0:]), ne.Uint32(e[4:])
	}
	off := int(id)*v.ps + pageHeaderSize + i*elementSize + int(pos)
	return v.b[off : off+int(ksize)]
}

// leafValue returns the offset in v.b of element i's value on leaf page id.
func (v pageView) leafValue(id uint64, i int) int {
	e := v.elem(id, i)
	return int(id)*v.ps + pageHeaderSize + i*elementSize + int(ne.Uint32(e[4:])) + int(ne.Uint32(e[8:]))
}

// meta returns the meta page with the higher txid.
func (v pageView) meta() boltMeta {
	if m0, m1 := v.metaAt(0), v.metaAt(1); m1.txid > m0.txid {
		return m1
	}
	return v.metaAt(0)
}

func (v pageView) metaAt(id uint64) boltMeta {
	p := v.page(id)[pageHeaderSize:]
	return boltMeta{root: ne.Uint64(p[16:]), freelist: ne.Uint64(p[32:]), hwm: ne.Uint64(p[40:]), txid: ne.Uint64(p[48:])}
}

// bucketRoot returns the root page of the named top-level bucket.
func (v pageView) bucketRoot(t *testing.T, name string) uint64 {
	t.Helper()
	root := v.meta().root
	if v.flags(root) != flagLeaf {
		t.Fatalf("root bucket page %d is not a leaf", root)
	}
	for i := range v.count(root) {
		e := v.elem(root, i)
		off := int(root)*v.ps + pageHeaderSize + i*elementSize + int(ne.Uint32(e[4:]))
		if string(v.b[off:off+int(ne.Uint32(e[8:]))]) == name {
			r := ne.Uint64(v.b[v.leafValue(root, i):])
			if r == 0 {
				t.Fatalf("bucket %s is inline", name)
			}
			return r
		}
	}
	t.Fatalf("no bucket %s", name)
	return 0
}

// freePages returns the free pages: those on the freelist or, in a file
// without one (as the spool writes them), those the tree does not reach.
func (v pageView) freePages() map[uint64]bool {
	m := v.meta()
	free := map[uint64]bool{}
	if m.freelist != noFreelist {
		p := v.page(m.freelist)
		n, off := uint64(ne.Uint16(p[10:])), pageHeaderSize
		if n == 0xffff {
			n, off = ne.Uint64(p[16:]), off+8
		}
		for i := range n {
			free[ne.Uint64(p[off+int(i)*8:])] = true
		}
		return free
	}
	reached := map[uint64]bool{}
	var walk func(id uint64, top bool)
	walk = func(id uint64, top bool) {
		for p := id; p <= id+uint64(ne.Uint32(v.page(id)[12:])); p++ {
			reached[p] = true
		}
		for i := range v.count(id) {
			e := v.elem(id, i)
			switch {
			case v.flags(id) == flagBranch:
				walk(ne.Uint64(e[8:]), top)
			case top && ne.Uint32(e[0:])&flagBucket != 0:
				if r := ne.Uint64(v.b[v.leafValue(id, i):]); r != 0 {
					walk(r, false)
				}
			}
		}
	}
	walk(m.root, true)
	for p := uint64(2); p < m.hwm; p++ {
		if !reached[p] {
			free[p] = true
		}
	}
	return free
}

// buildSpool writes a spool with consumers a and b the way the review's
// corruption campaign did (records added, some delivered and deleted, more
// added: branch pages, overflow-free leaves, free pages) and returns its
// bytes. It has no freelist (see boltOptions); withFreelist gives one.
func buildSpool(t *testing.T) pageView {
	t.Helper()
	dir := t.TempDir()
	s := open(t, dir, "a", "b")
	appendN(t, s, 600)
	if err := s.Ack("a", s.Stats().LastSeq-300); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack("b", s.Stats().LastSeq-450); err != nil {
		t.Fatal(err)
	}
	appendN(t, s, 100)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	return bboltGrown(b)
}

// bboltGrown returns the file b as bbolt alone grew it (the least power of
// two from 32 KiB that holds the high water mark and one page more),
// without the space the spool allocates ahead since round six: the corpus
// damages the bytes it did before.
func bboltGrown(b []byte) pageView {
	v := view(b)
	n := 32 << 10
	for n < int(v.meta().hwm+1)*v.ps {
		n *= 2
	}
	return view(b[:n])
}

// withFreelist returns base with a freelist, as bbolt's default options
// write it (and the spool did until round seven), committed once more.
func withFreelist(t *testing.T, base pageView) pageView {
	t.Helper()
	path := filepath.Join(t.TempDir(), fileName)
	if err := os.WriteFile(path, base.b, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil) // commits a freelist as it opens
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	v := bboltGrown(b)
	if v.meta().freelist == noFreelist {
		t.Fatal("bbolt wrote no freelist")
	}
	return v
}

// view returns b's pages, with the page size its first meta page states.
func view(b []byte) pageView {
	return pageView{b: b, ps: int(ne.Uint32(b[pageHeaderSize+8:]))}
}

// runChild runs TestDamagedChild in mode on a copy of b. It returns the
// child's output, whether the file was moved aside, and an error if the
// child did not end well.
func runChild(ctx context.Context, t *testing.T, b []byte, mode string) (string, bool, error) {
	dir := t.TempDir()
	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return "", false, err
	}
	ctx, cancel := context.WithTimeout(ctx, childTimeout+10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDamagedChild$")
	cmd.Env = append(os.Environ(), "HOPCORDER_SPOOL_CHILD="+mode+" "+dir)
	out, err := cmd.CombinedOutput()
	o := string(out)
	if i := strings.Index(o, "\ngoroutine "); i > 0 {
		o = o[:i] // a crash: keep the message, not every stack
	}
	moved, _ := filepath.Glob(path + ".corrupt-*")
	if err != nil || !strings.Contains(o, "CHILD OK") {
		return o, len(moved) > 0, fmt.Errorf("child: %w", err)
	}
	return o, len(moved) > 0, nil
}

// TestDamagedChild opens HOPCORDER_SPOOL_CHILD="<mode> <dir>"'s spool with
// consumers a and b and uses it like the agent does, in goroutines of their
// own: drains read and acknowledge everything, the schedulers append.
// Modes: "use" does just that; "fault" first damages the open file so that
// reading it faults; "panic" appends with an encoder that panics. It prints
// "CHILD OK" if every step succeeded or failed with an error, and the spool
// works afterwards.
func TestDamagedChild(t *testing.T) {
	spec := strings.Fields(os.Getenv("HOPCORDER_SPOOL_CHILD"))
	if len(spec) != 2 {
		t.Skip("helper process for the damaged spool tests")
	}
	mode, dir := spec[0], spec[1]
	go func() { // bound time and heap: a regression must end the child
		deadline := time.Now().Add(childTimeout)
		sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		for time.Now().Before(deadline) {
			metrics.Read(sample)
			if sample[0].Value.Uint64() > 512<<20 {
				fmt.Println("CHILD heap above 512 MiB")
				os.Exit(4)
			}
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Println("CHILD timed out")
		buf := make([]byte, 1<<20)
		fmt.Println(string(buf[:runtime.Stack(buf, true)]))
		os.Exit(3)
	}()
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a", "b"}})
	if err != nil {
		fmt.Println("CHILD open failed:", err)
		return
	}
	defer s.Close()
	inGoroutine := func(fn func() error) error {
		done := make(chan error, 1)
		go func() { done <- fn() }()
		return <-done
	}
	drain := func() error {
		for _, c := range []string{"a", "b"} {
			for {
				es, err := s.Read(c, 50, 1<<30)
				if err != nil || len(es) == 0 {
					return err
				}
				if err := s.Ack(c, es[len(es)-1].Seq); err != nil {
					return err
				}
			}
		}
		return nil
	}
	appendSome := func() error {
		for range 20 {
			if _, err := s.Append(time.Now(), func(seq uint64) ([]byte, error) {
				return []byte(fmt.Sprintf(`{"seq":%d}`, seq)), nil
			}); err != nil {
				return err
			}
		}
		return nil
	}
	switch mode {
	case "fault":
		// Damage under the open database (bit rot, another writer): the
		// first leaf with many elements gets a key outside the mapping.
		path := filepath.Join(dir, fileName)
		b, err := os.ReadFile(path)
		if err != nil {
			fmt.Println("CHILD", err)
			return
		}
		v := view(b)
		for id := uint64(2); int(id+1)*v.ps <= len(b); id++ {
			if v.flags(id) == flagLeaf && v.count(id) > 10 {
				f, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err == nil {
					_, err = f.WriteAt(binary.NativeEndian.AppendUint32(nil, 0x7ff00000), int64(int(id)*v.ps+pageHeaderSize+4))
					_ = f.Close()
				}
				if err != nil {
					fmt.Println("CHILD", err)
					return
				}
				break
			}
		}
	case "panic":
		_, err := s.Append(time.Now(), func(uint64) ([]byte, error) { panic("encoder bug") })
		if !errors.Is(err, errCorrupt) {
			fmt.Println("CHILD panicking append:", err)
			return
		}
	}
	for i, step := range []func() error{drain, appendSome, drain} {
		if err := inGoroutine(step); err != nil {
			if !errors.Is(err, errCorrupt) {
				fmt.Printf("CHILD step %d failed: %v\n", i, err)
				return
			}
			fmt.Printf("step %d: %v\n", i, err) // the damage, found while in use
		}
	}
	// Whatever happened, the spool works now.
	if err := inGoroutine(appendSome); err != nil {
		fmt.Println("CHILD append afterwards:", err)
		return
	}
	es, err := s.Read("a", 1000, 1<<30)
	if err != nil || len(es) < 20 {
		fmt.Printf("CHILD read afterwards: %d records, %v\n", len(es), err)
		return
	}
	fmt.Println("CHILD OK")
}

// Directed damage the round-two checks missed, each of which crashed, hung
// or panicked the agent, on every start or on the first acknowledgement,
// and keys out of order, which bbolt's open of a file without a freelist
// panics on. Each on the file as the spool writes it, without a freelist,
// and with one.
func TestDirectedDamageIsQuarantined(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	base := buildSpool(t)
	t.Run("nofreelist", func(t *testing.T) { directedDamage(t, base) })
	t.Run("freelist", func(t *testing.T) { directedDamage(t, withFreelist(t, base)) })
}

// directedDamage runs TestDirectedDamageIsQuarantined's cases on base.
func directedDamage(t *testing.T, base pageView) {
	records := base.bucketRoot(t, string(bucketRecords))
	if base.flags(records) != flagBranch || base.count(records) < 3 {
		t.Fatalf("records bucket root %d: flags %#x, %d elements; want a branch", records, base.flags(records), base.count(records))
	}
	firstLeaf := ne.Uint64(base.elem(records, 0)[8:])
	free, synced := base.freePages(), base.meta().freelist != noFreelist
	// Without a freelist this damage is none: the file is kept, and works.
	harmless := map[string]bool{"overflow over a free page": !synced}
	cases := []struct {
		name   string
		damage func(v pageView) bool
	}{
		// walkBucket recursed into it without end: a stack overflow.
		{"nested bucket rooted at its own page", func(v pageView) bool {
			e := v.elem(firstLeaf, 0)
			binary.NativeEndian.PutUint32(e[0:], flagBucket)
			val := v.leafValue(firstLeaf, 0)
			binary.NativeEndian.PutUint64(v.b[val:], firstLeaf)
			binary.NativeEndian.PutUint64(v.b[val+8:], 0)
			return true
		}},
		// tx.Check looped over 2^31 pages, filling a map: a hang and
		// gigabytes of memory.
		{"overflow count of a leaf", func(v pageView) bool {
			binary.NativeEndian.PutUint32(v.page(firstLeaf)[12:], 0x7fffffff)
			return true
		}},
		{"overflow count of the freelist", func(v pageView) bool {
			if v.meta().freelist == noFreelist {
				return false
			}
			binary.NativeEndian.PutUint32(v.page(v.meta().freelist)[12:], 0x7fffffff)
			return true
		}},
		// tx.Check reads branch keys, which the cursor walk does not: a
		// fatal fault in the checker's goroutine.
		{"branch key outside the mapping", func(v pageView) bool {
			binary.NativeEndian.PutUint32(v.elem(records, 1)[0:], 0x7ff00000)
			return true
		}},
		// The cursor walk descended into its own page for ever.
		{"branch element pointing at its own page", func(v pageView) bool {
			binary.NativeEndian.PutUint64(v.elem(records, 0)[8:], records)
			return true
		}},
		// Passed tx.Check (it checks only a span's first page against the
		// freelist); freeing the leaf later panicked with "page already
		// freed". Without a freelist the page is the leaf's now.
		{"overflow over a free page", func(v pageView) bool {
			for i := range v.count(records) {
				leaf := ne.Uint64(v.elem(records, i)[8:])
				if free[leaf+1] && ne.Uint32(v.page(leaf)[12:]) == 0 {
					binary.NativeEndian.PutUint32(v.page(leaf)[12:], 1)
					return true
				}
			}
			return false
		}},
		// Keys out of order (review, round five): bbolt opens a file without
		// a freelist with a walk for its free pages that panics on them, in
		// a goroutine of bbolt's own or in the caller's, with that goroutine
		// still running.
		{"leaf keys out of order", func(v pageView) bool {
			k0, k1 := v.key(firstLeaf, 0), v.key(firstLeaf, 1)
			t0 := append([]byte(nil), k0...)
			copy(k0, k1)
			copy(k1, t0)
			return true
		}},
		{"branch key not above the one before", func(v pageView) bool {
			copy(v.key(records, 1), v.key(records, 0))
			return true
		}},
		{"branch key above its child's keys", func(v pageView) bool {
			copy(v.key(records, 1), v.key(records, 2))
			return true
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := pageView{b: append([]byte(nil), base.b...), ps: base.ps}
			if !c.damage(v) {
				t.Skip("no page to damage this way in this spool")
			}
			out, moved, err := runChild(t.Context(), t, v.b, "use")
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			if moved == harmless[c.name] {
				t.Errorf("damaged file moved aside: %v, want %v\n%s", moved, !moved, out)
			}
			// Found by the checks, not by bbolt's walk for the free pages:
			// that panics, and crashes the agent now and then (a goroutine
			// of bbolt's own goes on after the panic, on the closed
			// transaction).
			if strings.Contains(out, "freepages") {
				t.Errorf("bbolt's open found the damage\n%s", out)
			}
		})
	}
}

// craftedMeta returns the review's crafted spool (round four): meta page 1
// rewritten into one with the higher txid, a valid checksum, twice the page
// size, and as root a free page made into the given branch. bbolt checks
// only a meta's magic, version and checksum, so it uses that meta and its
// tree. checkPages refused the meta for its page size and checked the
// other's tree instead, then bbolt's own check ran on the crafted one.
func craftedMeta(t *testing.T, branch func(v pageView, root, child uint64)) []byte {
	t.Helper()
	base := buildSpool(t)
	if base.meta() != base.metaAt(0) {
		// Make meta page 0 the newer one, so that bbolt takes its page
		// size from page 0, the meta this leaves alone.
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, fileName), base.b, 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := bolt.Open(filepath.Join(dir, fileName), 0o600, boltOptions())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(*bolt.Tx) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, fileName))
		if err != nil {
			t.Fatal(err)
		}
		if base = view(b); base.meta() != base.metaAt(0) {
			t.Fatal("meta page 0 is still the older one")
		}
	}
	v := pageView{b: append([]byte(nil), base.b...), ps: base.ps}
	m := v.metaAt(0)
	var f uint64 // the lowest free page
	for p := range v.freePages() {
		if f == 0 || p < f {
			f = p
		}
	}
	if f == 0 {
		t.Fatal("no free page")
	}
	pg := v.page(f)
	clear(pg[:v.ps])
	binary.NativeEndian.PutUint64(pg[0:], f)
	binary.NativeEndian.PutUint16(pg[8:], flagBranch)
	branch(v, f, m.root)
	meta := v.page(1)[pageHeaderSize:]
	copy(meta, v.page(0)[pageHeaderSize:pageHeaderSize+64])
	binary.NativeEndian.PutUint32(meta[8:], uint32(2*v.ps)) //nolint:gosec // a page size
	binary.NativeEndian.PutUint64(meta[16:], f)
	binary.NativeEndian.PutUint64(meta[48:], m.txid+1)
	h := fnv.New64a()
	_, _ = h.Write(meta[:56])
	binary.NativeEndian.PutUint64(meta[56:], h.Sum64())
	return v.b
}

// The review's crafted spools got past checkPages, which chose another
// meta page than bbolt does, and then crashed or hung every start: a fault
// in tx.Check's goroutine, or a cursor walk descending for ever. They are
// quarantined now.
func TestCraftedMetaIsQuarantined(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	// key adds a branch element pointing at child whose key is at offset
	// pos from the element (and, unless pos is far off, is "k").
	key := func(v pageView, id uint64, i int, pos uint32, k string, child uint64) {
		e := v.elem(id, i)
		binary.NativeEndian.PutUint32(e[0:], pos)
		binary.NativeEndian.PutUint32(e[4:], uint32(len(k))) //nolint:gosec // short
		binary.NativeEndian.PutUint64(e[8:], child)
		if int(pos) < v.ps {
			copy(e[pos:], k)
		}
	}
	cases := []struct {
		name   string
		branch func(v pageView, root, child uint64)
	}{
		{"branch key outside the mapping", func(v pageView, root, child uint64) {
			binary.NativeEndian.PutUint16(v.page(root)[10:], 1)
			key(v, root, 0, 0x7ff00000, "meta", child)
		}},
		{"branch pointing at itself", func(v pageView, root, _ uint64) {
			binary.NativeEndian.PutUint16(v.page(root)[10:], 1)
			key(v, root, 0, elementSize, "meta", root)
		}},
		// A seek reads keys 1 and 2, the cursor walk none, tx.Check (in a
		// goroutine of its own) all three.
		{"key only tx.Check reads outside the mapping", func(v pageView, root, child uint64) {
			binary.NativeEndian.PutUint16(v.page(root)[10:], 3)
			key(v, root, 0, 0x7ff00000, "0000", child)
			key(v, root, 1, 2*elementSize+8, "a", child)
			key(v, root, 2, elementSize+16, "z", child)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := craftedMeta(t, c.branch)
			path := filepath.Join(t.TempDir(), fileName)
			if err := os.WriteFile(path, b, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := checkPages(path, int(ne.Uint32(b[pageHeaderSize+8:]))); err == nil {
				t.Error("checkPages accepts the crafted file")
			}
			for start := 1; start <= 2; start++ {
				out, moved, err := runChild(t.Context(), t, b, "use")
				if err != nil {
					t.Fatalf("start %d: %v\n%s", start, err, out)
				}
				if !moved {
					t.Errorf("start %d: crafted file kept\n%s", start, out)
				}
			}
		})
	}
}

// rleBomb is a zstd frame of n RLE blocks of 128 KiB of '{' each: 4 bytes
// of input per 128 KiB of output, and no content size in its header.
func rleBomb(n int) []byte {
	b := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38} // magic; no checksum or size; window 128 KiB
	for i := range n {
		h := uint32(128<<10)<<3 | 1<<1 // block size, type RLE
		if i == n-1 {
			h |= 1 // last block
		}
		b = append(b, byte(h), byte(h>>8), byte(h>>16), '{')
	}
	return b
}

// replaceRecord stores val as the record seq of the closed spool in dir.
func replaceRecord(t *testing.T, dir string, seq uint64, val []byte) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, fileName), 0o600, boltOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRecords).Put(seqKey(seq), append(binary.BigEndian.AppendUint64(nil, uint64(time.Now().UnixNano())), val...)) //nolint:gosec // now
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// The review's decompression bomb (round four): a 48 KiB stored value that
// decodes to 1.5 GiB. With the decoder's default bound (64 GiB) every start
// took the heap past 512 MiB here, and the agent was OOM-killed in the
// chart's 256 MiB on every start; it is not damage bbolt or the page check
// can see, so it was never quarantined. Now it decodes to at most
// maxRecordSize, fails, and is skipped like any undecodable record.
func TestDecompressionBombIsSkipped(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	dir := t.TempDir()
	s := open(t, dir, "a", "b")
	seqs := appendN(t, s, 3)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	replaceRecord(t, dir, seqs[1], rleBomb(12<<10)) // 12,288 blocks: 1.5 GiB
	b, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	for start := 1; start <= 2; start++ {
		out, _, err := runChild(t.Context(), t, b, "use")
		if err != nil {
			t.Fatalf("start %d: %v\n%.600s", start, err, out)
		}
		if !strings.Contains(out, "skipping undecodable record") {
			t.Errorf("start %d: the bomb was not skipped\n%.600s", start, out)
		}
	}
}

// Damage found while the spool is in use (bbolt faults, or panics) used to
// kill the agent. Now the operation fails, the file is moved aside at once,
// and the spool goes on empty.
func TestDamageInUseIsQuarantined(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	base := buildSpool(t)
	for _, mode := range []string{"fault", "panic"} {
		t.Run(mode, func(t *testing.T) {
			out, moved, err := runChild(t.Context(), t, base.b, mode)
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			if !moved {
				t.Errorf("damaged file kept\n%s", out)
			}
		})
	}
}

// A sample of the review's corruption campaign: random damage of four
// kinds (16 random bytes, bit flips, a random word in a page, a page
// header field). In the review's 3000 cases, 54 hung, one faulted inside
// tx.Check on every start, and one panicked on its first acknowledgement.
// Every case must now be kept or quarantined, and the spool work. The
// sample is cases 0..63 and six that made tx.Check grow without bound
// before (overflow fields, in the layout the spool wrote then; on a host
// with 4 KiB pages the spool built here has the same layout every time).
// HOPCORDER_SPOOL_CORPUS=3000 runs cases 0..2999 instead. Each runs on the
// file as the spool writes it, without a freelist (since round seven: bbolt
// walks the tree for the free pages as it opens it), and with one, as
// bbolt's default options write it.
func TestRandomDamageCorpus(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	var sample []int
	for i := range 64 {
		sample = append(sample, i)
	}
	sample = append(sample, 99, 171, 223, 227, 271, 283)
	if n, err := strconv.Atoi(os.Getenv("HOPCORDER_SPOOL_CORPUS")); err == nil {
		sample = sample[:0]
		for i := range n {
			sample = append(sample, i)
		}
	}
	base := buildSpool(t)
	t.Run("nofreelist", func(t *testing.T) { damageCorpus(t, base, sample) })
	t.Run("freelist", func(t *testing.T) { damageCorpus(t, withFreelist(t, base), sample) })
}

// damageCorpus runs the corpus's cases in sample on base.
func damageCorpus(t *testing.T, base pageView, sample []int) {
	var (
		mu       sync.Mutex
		failures []string
		kinds    [2]int // kept, quarantined
	)
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range max(2, runtime.GOMAXPROCS(0)) {
		workers.Go(func() {
			for i := range jobs {
				b, kind := corrupt(base, i)
				out, moved, err := runChild(t.Context(), t, b, "use")
				mu.Lock()
				if err == nil && strings.Contains(out, "freepages") {
					err = errors.New("bbolt's open found the damage (see TestDirectedDamageIsQuarantined)")
				}
				switch {
				case err != nil:
					if len(out) > 400 {
						out = out[:400]
					}
					failures = append(failures, fmt.Sprintf("case %d (kind %d): %v\n%s", i, kind, err, out))
				case moved:
					kinds[1]++
				default:
					kinds[0]++
				}
				mu.Unlock()
			}
		})
	}
	for _, i := range sample {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	t.Logf("%d cases: %d kept, %d quarantined, %d failed", len(sample), kinds[0], kinds[1], len(failures))
	for _, f := range failures {
		t.Error(f)
	}
}

// corrupt damages a copy of base the way the review's campaign did, case i
// of kind i%4.
func corrupt(base pageView, i int) ([]byte, int) {
	r := rand.New(rand.NewPCG(uint64(i), 7)) //nolint:gosec // reproducible damage
	b := append([]byte(nil), base.b...)
	ps := base.ps
	kind := i % 4
	switch kind {
	case 0: // 16 random bytes
		off := r.IntN(len(b) - 16)
		for j := range 16 {
			b[off+j] = byte(r.Uint32())
		}
	case 1: // a few bit flips
		for range 1 + r.IntN(4) {
			b[r.IntN(len(b))] ^= 1 << r.IntN(8)
		}
	case 2: // a random 32-bit value at a 4-aligned offset after the meta pages
		off := (2*ps + r.IntN(len(b)-2*ps-4)) &^ 3
		binary.LittleEndian.PutUint32(b[off:], r.Uint32())
	case 3: // a page header field: id, flags, count or overflow
		off := (2+r.IntN(len(b)/ps-2))*ps + []int{0, 8, 10, 12}[r.IntN(4)]
		v := r.Uint32()
		b[off], b[off+1] = byte(v), byte(v>>8)
		if r.IntN(2) == 0 {
			b[off+2], b[off+3] = byte(v>>16), byte(v>>24)
		}
	}
	return b, kind
}
