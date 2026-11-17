package server

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// chunkPool hands out the read buffer's chunks (readChunk bytes each) to the
// request bodies being read. A body reserves every chunk it may need at
// once, and reservations are granted first come, first served: a reader
// never holds part of what it needs while it waits for the rest, so no set
// of readers can hold the whole pool between them with each waiting for
// more (seven max-size bodies taking chunks one at a time did, stalling all
// ingest until their read timeout). It is a weighted semaphore.
type chunkPool struct {
	mu      sync.Mutex
	size    int
	held    int
	waiters list.List // of *chunkWaiter, in arrival order
}

type chunkWaiter struct {
	n     int
	ready chan struct{} // closed once the chunks are granted
}

var errPoolTooSmall = errors.New("request needs more read buffer than there is")

func newChunkPool(size int) *chunkPool { return &chunkPool{size: size} }

// acquire reserves n chunks, waiting behind earlier requests until they
// are free or ctx is done.
func (p *chunkPool) acquire(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	p.mu.Lock()
	if n > p.size {
		p.mu.Unlock()
		return errPoolTooSmall
	}
	if p.size-p.held >= n && p.waiters.Len() == 0 {
		p.held += n
		p.mu.Unlock()
		return nil
	}
	w := &chunkWaiter{n: n, ready: make(chan struct{})}
	e := p.waiters.PushBack(w)
	p.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-w.ready: // granted while giving up: hand them on
		p.held -= n
	default:
		p.waiters.Remove(e)
	}
	// Either way the requests behind may fit now.
	p.grant()
	return ctx.Err()
}

// release returns n chunks to the pool.
func (p *chunkPool) release(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.held -= n
	if p.held < 0 {
		panic("chunkPool: released more chunks than were held")
	}
	p.grant()
}

// grant hands free chunks to the waiting requests in order, stopping at the
// first that does not fit (so a large body is not passed over for ever).
// p.mu must be held.
func (p *chunkPool) grant() {
	for e := p.waiters.Front(); e != nil; e = p.waiters.Front() {
		w := e.Value.(*chunkWaiter)
		if p.size-p.held < w.n {
			return
		}
		p.held += w.n
		p.waiters.Remove(e)
		close(w.ready)
	}
}

// inUse returns how many chunks are reserved.
func (p *chunkPool) inUse() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held
}
