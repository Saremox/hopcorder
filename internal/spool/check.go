package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

// bbolt's on-disk format (version 2, unchanged since boltdb), as far as the
// structural check below needs it. Fields are in the host's byte order.
const (
	boltMagic   = 0xED0CDAED
	boltVersion = 2

	pageHeaderSize  = 16 // id u64, flags u16, count u16, overflow u32
	elementSize     = 16 // branch: pos, ksize u32, pgid u64; leaf: flags, pos, ksize, vsize u32
	bucketValueSize = 16 // root pgid u64, sequence u64; an inline bucket's page follows

	flagBranch   = 0x01
	flagLeaf     = 0x02
	flagFreelist = 0x10
	flagBucket   = 0x01 // of a leaf element: the value is a bucket

	noFreelist = ^uint64(0)

	// maxTreeDepth bounds the B+tree depth (a real spool's is a few).
	maxTreeDepth = 64
	// readChunk bounds one read of the freelist's ids.
	readChunk = 64 << 10
)

var ne = binary.NativeEndian

// boltMeta is the part of a meta page the check uses.
type boltMeta struct {
	pageSize, root, freelist, hwm, txid uint64
}

// checkPages verifies the page structure of the bbolt file at path, with
// page size ps, by reading it (not through bbolt's memory map), before bbolt
// or its consistency check reads anything beyond the meta pages: bbolt
// pages carry no checksums, and bbolt trusts them. A damaged page can then
// make bbolt fault on the mapping (a fatal error in a goroutine that has not
// asked for panics), loop about 2^32 times (an overflow count), recurse
// without end (a page that is its own child) or free a page twice. The
// check returns an error for any of these and costs one read of each page's
// header and element array:
//
//   - the meta page bbolt uses, chosen as bbolt chooses it (see meta): the
//     page size ps, the high water mark within the file, root and freelist
//     below it;
//   - every page reachable from the root, and the freelist page: its id,
//     type and overflow within the high water mark, and no page reached
//     twice (which also rules out cycles), counted, or freed;
//   - every element within its page's span, branch pages not empty, the
//     tree at most maxTreeDepth deep;
//   - the root bucket holds only buckets, and those hold none (the spool
//     has no nested buckets), inline ones included;
//   - the freelist: ids within the file, none twice, none reachable, and
//     every page below the high water mark reachable or free.
//
// The caller holds a lock on the file, so nobody writes it meanwhile.
func checkPages(path string, ps int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	c := &pageCheck{f: f, ps: uint64(ps), size: uint64(st.Size())} //nolint:gosec // sizes are positive
	return c.run()
}

type pageCheck struct {
	f        *os.File
	ps, size uint64
	hwm      uint64
	seen     []uint64 // bitset: pages reached (meta, freelist, tree)
	free     []uint64 // bitset: pages on the freelist
}

func (c *pageCheck) read(off uint64, n uint64) ([]byte, error) {
	if off+n > c.size || off+n < off {
		return nil, fmt.Errorf("read of %d bytes at %d beyond the file (%d bytes)", n, off, c.size)
	}
	b := make([]byte, n)
	if _, err := c.f.ReadAt(b, int64(off)); err != nil && !errors.Is(err, io.EOF) { //nolint:gosec // within the file
		return nil, err
	}
	return b, nil
}

func bit(set []uint64, i uint64) bool { return set[i/64]&(1<<(i%64)) != 0 }
func setBit(set []uint64, i uint64)   { set[i/64] |= 1 << (i % 64) }

func (c *pageCheck) run() error {
	m, err := c.meta()
	if err != nil {
		return err
	}
	if m.hwm < 2 || m.hwm > c.size/c.ps {
		return fmt.Errorf("high water mark %d outside the file's %d pages", m.hwm, c.size/c.ps)
	}
	c.hwm = m.hwm
	c.seen = make([]uint64, (c.hwm+63)/64)
	c.free = make([]uint64, (c.hwm+63)/64)
	setBit(c.seen, 0)
	setBit(c.seen, 1)
	if m.freelist == noFreelist {
		return errors.New("no freelist (the spool always writes one)")
	}
	if err := c.freelist(m.freelist); err != nil {
		return fmt.Errorf("freelist page %d: %w", m.freelist, err)
	}
	if err := c.tree(m.root); err != nil {
		return err
	}
	for id := range c.hwm {
		if !bit(c.seen, id) && !bit(c.free, id) {
			return fmt.Errorf("page %d neither reachable nor free", id)
		}
	}
	return nil
}

// meta returns the meta page bbolt uses, chosen exactly as bbolt's DB.meta
// chooses it: the one with the higher txid (page 0 on a tie) if it is
// valid, else the other, where valid means only magic, version and
// checksum (common.Meta.Validate). What else the check needs of that page,
// its page size, is required of it afterwards, and the file refused if it
// differs. Passing it over for the other meta page instead, as this did
// until round four, checked a tree bbolt does not use: a crafted meta with
// the higher txid and another page size took bbolt to a tree nothing had
// checked.
func (c *pageCheck) meta() (boltMeta, error) {
	var ms [2]boltMeta
	var errs [2]error
	for i := range ms {
		ms[i], errs[i] = c.readMeta(uint64(i))
	}
	a, b := 0, 1
	if ms[1].txid > ms[0].txid {
		a, b = 1, 0
	}
	if errs[a] != nil {
		a = b
	}
	if errs[a] != nil {
		return boltMeta{}, fmt.Errorf("no valid meta page: %w", errors.Join(errs[0], errs[1]))
	}
	if m := ms[a]; m.pageSize != c.ps {
		return boltMeta{}, fmt.Errorf("meta page %d, the one bbolt uses: page size %d, not the file's %d", a, m.pageSize, c.ps)
	}
	return ms[a], nil
}

// readMeta reads meta page id and checks it as bbolt does. The fields are
// returned even if it is invalid: bbolt orders the pages by txid first.
func (c *pageCheck) readMeta(id uint64) (boltMeta, error) {
	b, err := c.read(id*c.ps, pageHeaderSize+64)
	if err != nil {
		return boltMeta{}, err
	}
	m := b[pageHeaderSize:]
	meta := boltMeta{
		pageSize: uint64(ne.Uint32(m[8:])), root: ne.Uint64(m[16:]), freelist: ne.Uint64(m[32:]),
		hwm: ne.Uint64(m[40:]), txid: ne.Uint64(m[48:]),
	}
	h := fnv.New64a()
	_, _ = h.Write(m[:56])
	switch {
	case ne.Uint32(m[0:]) != boltMagic:
		return meta, fmt.Errorf("meta page %d: bad magic", id)
	case ne.Uint32(m[4:]) != boltVersion:
		return meta, fmt.Errorf("meta page %d: version %d", id, ne.Uint32(m[4:]))
	case ne.Uint64(m[56:]) != h.Sum64():
		return meta, fmt.Errorf("meta page %d: bad checksum", id)
	}
	return meta, nil
}

// span reads the header of page id, checks its id, type and extent (marking
// the pages seen) and returns its flags, element count and size in bytes.
func (c *pageCheck) span(id uint64) (flags, count uint16, bytes uint64, err error) {
	if id < 2 || id >= c.hwm {
		return 0, 0, 0, fmt.Errorf("page %d outside 2..%d", id, c.hwm-1)
	}
	h, err := c.read(id*c.ps, pageHeaderSize)
	if err != nil {
		return 0, 0, 0, err
	}
	if got := ne.Uint64(h[0:]); got != id {
		return 0, 0, 0, fmt.Errorf("page %d says it is page %d", id, got)
	}
	flags, count, overflow := ne.Uint16(h[8:]), ne.Uint16(h[10:]), uint64(ne.Uint32(h[12:]))
	if overflow >= c.hwm-id {
		return 0, 0, 0, fmt.Errorf("page %d: %d overflow pages beyond the high water mark %d", id, overflow, c.hwm)
	}
	for p := id; p <= id+overflow; p++ {
		if bit(c.seen, p) {
			return 0, 0, 0, fmt.Errorf("page %d (of page %d) reached twice", p, id)
		}
		setBit(c.seen, p)
	}
	return flags, count, (overflow + 1) * c.ps, nil
}

func (c *pageCheck) freelist(id uint64) error {
	flags, count, bytes, err := c.span(id)
	if err != nil {
		return err
	}
	if flags != flagFreelist {
		return fmt.Errorf("type %#x, not a freelist", flags)
	}
	off, n := uint64(pageHeaderSize), uint64(count)
	if count == 0xffff { // the real count is the first id slot
		b, err := c.read(id*c.ps+off, 8)
		if err != nil {
			return err
		}
		off, n = off+8, ne.Uint64(b)
	}
	if n > (bytes-off)/8 {
		return fmt.Errorf("%d ids do not fit its %d bytes", n, bytes)
	}
	for done := uint64(0); done < n; {
		k := min(n-done, readChunk/8)
		b, err := c.read(id*c.ps+off+done*8, k*8)
		if err != nil {
			return err
		}
		for i := range k {
			p := ne.Uint64(b[i*8:])
			switch {
			case p < 2 || p >= c.hwm:
				return fmt.Errorf("free page %d outside 2..%d", p, c.hwm-1)
			case bit(c.free, p):
				return fmt.Errorf("page %d free twice", p)
			}
			setBit(c.free, p)
		}
		done += k
	}
	for p := id; p < id+bytes/c.ps; p++ {
		if bit(c.free, p) {
			return fmt.Errorf("its own page %d is free", p)
		}
	}
	return nil
}

// tree checks every page reachable from the root bucket's root page.
func (c *pageCheck) tree(root uint64) error {
	type visit struct {
		id, depth uint64
		top       bool // a page of the root bucket, whose values are buckets
	}
	stack := []visit{{id: root, depth: 1, top: true}}
	for len(stack) > 0 {
		v := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if v.depth > maxTreeDepth {
			return fmt.Errorf("page %d: tree deeper than %d", v.id, maxTreeDepth)
		}
		flags, count, bytes, err := c.span(v.id)
		if err != nil {
			return err
		}
		if bit(c.free, v.id) {
			return fmt.Errorf("page %d reachable and free", v.id)
		}
		for p := v.id + 1; p < v.id+bytes/c.ps; p++ {
			if bit(c.free, p) {
				return fmt.Errorf("page %d (overflow of page %d) reachable and free", p, v.id)
			}
		}
		if flags != flagBranch && flags != flagLeaf {
			return fmt.Errorf("page %d: type %#x, not a branch or leaf", v.id, flags)
		}
		if flags == flagBranch && count == 0 {
			return fmt.Errorf("page %d: empty branch", v.id)
		}
		elems := pageHeaderSize + uint64(count)*elementSize
		if elems > bytes {
			return fmt.Errorf("page %d: %d elements do not fit its %d bytes", v.id, count, bytes)
		}
		page, err := c.read(v.id*c.ps, elems)
		if err != nil {
			return err
		}
		for i := range uint64(count) {
			e := page[pageHeaderSize+i*elementSize:]
			at := pageHeaderSize + i*elementSize // the element's offset; pos counts from there
			if flags == flagBranch {
				pos, ksize := uint64(ne.Uint32(e[0:])), uint64(ne.Uint32(e[4:]))
				if at+pos < elems || at+pos+ksize > bytes {
					return fmt.Errorf("page %d: branch element %d outside the page", v.id, i)
				}
				stack = append(stack, visit{id: ne.Uint64(e[8:]), depth: v.depth + 1, top: v.top})
				continue
			}
			eflags := ne.Uint32(e[0:])
			pos, ksize, vsize := uint64(ne.Uint32(e[4:])), uint64(ne.Uint32(e[8:])), uint64(ne.Uint32(e[12:]))
			if at+pos < elems || at+pos+ksize+vsize > bytes {
				return fmt.Errorf("page %d: leaf element %d outside the page", v.id, i)
			}
			switch {
			case !v.top && eflags&flagBucket != 0:
				return fmt.Errorf("page %d: element %d is a nested bucket", v.id, i)
			case v.top && eflags&flagBucket == 0:
				return fmt.Errorf("page %d: element %d of the root bucket is not a bucket", v.id, i)
			case !v.top:
				continue
			}
			if vsize < bucketValueSize {
				return fmt.Errorf("page %d: bucket %d has a %d-byte header", v.id, i, vsize)
			}
			val, err := c.read(v.id*c.ps+at+pos+ksize, vsize)
			if err != nil {
				return err
			}
			if r := ne.Uint64(val); r != 0 {
				stack = append(stack, visit{id: r, depth: v.depth + 1})
			} else if err := checkInline(val[bucketValueSize:]); err != nil {
				return fmt.Errorf("page %d: inline bucket %d: %w", v.id, i, err)
			}
		}
	}
	return nil
}

// checkInline checks an inline bucket's page, the bytes after its bucket
// header: a leaf whose elements lie within it, none a bucket.
func checkInline(p []byte) error {
	if len(p) < pageHeaderSize {
		return errors.New("no page header")
	}
	if flags := ne.Uint16(p[8:]); flags != flagLeaf {
		return fmt.Errorf("type %#x, not a leaf", flags)
	}
	n, count := uint64(len(p)), uint64(ne.Uint16(p[10:]))
	elems := pageHeaderSize + count*elementSize
	if elems > n {
		return fmt.Errorf("%d elements do not fit %d bytes", count, n)
	}
	for i := range count {
		e := p[pageHeaderSize+i*elementSize:]
		at := pageHeaderSize + i*elementSize
		pos, ksize, vsize := uint64(ne.Uint32(e[4:])), uint64(ne.Uint32(e[8:])), uint64(ne.Uint32(e[12:]))
		if at+pos < elems || at+pos+ksize+vsize > n {
			return fmt.Errorf("element %d outside the page", i)
		}
		if ne.Uint32(e[0:])&flagBucket != 0 {
			return fmt.Errorf("element %d is a nested bucket", i)
		}
	}
	return nil
}
