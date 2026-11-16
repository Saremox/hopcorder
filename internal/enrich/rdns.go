package enrich

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PTRResolver looks up reverse DNS; *net.Resolver implements it.
type PTRResolver interface {
	LookupAddr(ctx context.Context, addr string) ([]string, error)
}

// RDNS is a cache of reverse DNS names that never blocks its callers: Name
// returns what is cached right now and queues a lookup for addresses it has
// not resolved (or whose entry has expired, in which case the stale name is
// still returned until the refresh completes). Hop addresses repeat every
// cycle, so a new hop has its name within a cycle or two.
type RDNS struct {
	Resolver    PTRResolver
	TTL         time.Duration // how long a found name is kept; default 24h
	NegativeTTL time.Duration // how long "no name" is kept; default 1h
	Timeout     time.Duration // per lookup; default 2s
	Workers     int           // concurrent lookups; default 4
	MaxEntries  int           // cache bound; default 100000
	Logger      *slog.Logger

	once     sync.Once
	mu       sync.Mutex
	cache    map[netip.Addr]rdnsEntry
	inflight map[netip.Addr]bool
	queue    chan netip.Addr

	hits, misses, dropped, lookups atomic.Uint64
}

type rdnsEntry struct {
	name    string
	expires time.Time
}

// RDNSStats are cumulative counters.
type RDNSStats struct {
	Hits, Misses, Dropped, Lookups uint64
	Entries                        int
}

func (r *RDNS) init() {
	r.once.Do(func() {
		if r.Resolver == nil {
			r.Resolver = net.DefaultResolver
		}
		if r.TTL <= 0 {
			r.TTL = 24 * time.Hour
		}
		if r.NegativeTTL <= 0 {
			r.NegativeTTL = time.Hour
		}
		if r.Timeout <= 0 {
			r.Timeout = 2 * time.Second
		}
		if r.Workers <= 0 {
			r.Workers = 4
		}
		if r.MaxEntries <= 0 {
			r.MaxEntries = 100000
		}
		if r.Logger == nil {
			r.Logger = slog.Default()
		}
		r.cache = map[netip.Addr]rdnsEntry{}
		r.inflight = map[netip.Addr]bool{}
		r.queue = make(chan netip.Addr, 4096)
	})
}

// Name returns the cached name of ip ("" if unknown or none) and schedules
// a lookup if the cache has nothing current.
func (r *RDNS) Name(ip netip.Addr) string {
	r.init()
	if !ip.IsValid() {
		return ""
	}
	ip = ip.Unmap().WithZone("")
	now := time.Now()
	r.mu.Lock()
	e, ok := r.cache[ip]
	fresh := ok && now.Before(e.expires)
	if !fresh && !r.inflight[ip] {
		select {
		case r.queue <- ip:
			r.inflight[ip] = true
		default:
			r.dropped.Add(1) // retried on a later sighting
		}
	}
	r.mu.Unlock()
	if ok {
		r.hits.Add(1)
	} else {
		r.misses.Add(1)
	}
	return e.name
}

// Run resolves queued addresses until ctx ends.
func (r *RDNS) Run(ctx context.Context) {
	r.init()
	var wg sync.WaitGroup
	for range r.Workers {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case ip := <-r.queue:
					r.resolve(ctx, ip)
				}
			}
		})
	}
	wg.Wait()
}

func (r *RDNS) resolve(ctx context.Context, ip netip.Addr) {
	r.lookups.Add(1)
	lctx, cancel := context.WithTimeout(ctx, r.Timeout)
	names, err := r.Resolver.LookupAddr(lctx, ip.String())
	cancel()
	if ctx.Err() != nil {
		r.mu.Lock()
		delete(r.inflight, ip)
		r.mu.Unlock()
		return
	}
	e := rdnsEntry{expires: time.Now().Add(r.NegativeTTL)}
	if err == nil && len(names) > 0 {
		e = rdnsEntry{name: strings.TrimSuffix(names[0], "."), expires: time.Now().Add(r.TTL)}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inflight, ip)
	if _, ok := r.cache[ip]; !ok && len(r.cache) >= r.MaxEntries {
		r.evictLocked()
	}
	r.cache[ip] = e
}

// evictLocked drops expired entries, or else roughly a tenth of the cache.
func (r *RDNS) evictLocked() {
	now := time.Now()
	n := 0
	for ip, e := range r.cache {
		if now.After(e.expires) {
			delete(r.cache, ip)
			n++
		}
	}
	for ip := range r.cache {
		if n >= r.MaxEntries/10+1 {
			break
		}
		delete(r.cache, ip)
		n++
	}
}

// Stats returns counters for metrics.
func (r *RDNS) Stats() RDNSStats {
	r.init()
	r.mu.Lock()
	n := len(r.cache)
	r.mu.Unlock()
	return RDNSStats{Hits: r.hits.Load(), Misses: r.misses.Load(), Dropped: r.dropped.Load(),
		Lookups: r.lookups.Load(), Entries: n}
}
