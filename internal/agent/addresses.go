package agent

import (
	"net/netip"
	"strings"
	"sync"

	"github.com/saremox/hopcorder/internal/record"
)

// Addresses remembers the address each name target uses, so that the next
// Agent, the configuration a reload starts, begins with it instead of a
// lookup. Before, every configuration's runners started without one: a
// reload while DNS was down with the network left each name target without
// records for the rest of the outage, while targets given as addresses
// recorded it (review, round six). An address is kept by its name and the
// address family asked for (the lookup's network), and each Agent forgets,
// as it runs, those of names none of its targets resolves, so the set is as
// large as the configuration. Safe for concurrent use; one Agent runs with
// it at a time.
type Addresses struct {
	mu sync.Mutex
	m  map[string]addrEntry
}

type addrEntry struct {
	ip netip.Addr
	// saved: read from an AddressStore (an earlier process's) and not used
	// by this one. It may be old, so it is only a fallback for a first
	// lookup that fails.
	saved bool
}

// NewAddresses returns an empty set of addresses.
func NewAddresses() *Addresses { return &Addresses{m: map[string]addrEntry{}} }

// AddressStore keeps the name targets' addresses across restarts, by the
// keys an Agent gives them. *spool.Spool implements it.
type AddressStore interface {
	// Addresses returns the saved addresses by key.
	Addresses() (map[string]netip.Addr, error)
	// SaveAddress saves addr under key.
	SaveAddress(key string, addr netip.Addr) error
	// KeepAddresses deletes the saved addresses of every key not in keys.
	KeepAddresses(keys []string) error
}

func (a *Addresses) get(key string) (addrEntry, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.m[key]
	return e, ok
}

// set records that key's targets use ip.
func (a *Addresses) set(key string, ip netip.Addr) {
	a.mu.Lock()
	a.m[key] = addrEntry{ip: ip}
	a.mu.Unlock()
}

// prepare readies the set for an Agent whose targets' names have keys: the
// others are forgotten, and a saved address fills in for each key the set
// has none for (after a restart, every key).
func (a *Addresses) prepare(keys []string, saved map[string]netip.Addr) {
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for k := range a.m {
		if !want[k] {
			delete(a.m, k)
		}
	}
	for _, k := range keys {
		if _, ok := a.m[k]; ok {
			continue
		}
		if ip, ok := saved[k]; ok && usable(k, ip) {
			a.m[k] = addrEntry{ip: ip, saved: true}
		}
	}
}

// lookupNetwork is the network a target's name is looked up in.
func lookupNetwork(af int) string {
	switch af {
	case 4:
		return "ip4"
	case 6:
		return "ip6"
	}
	return "ip"
}

// addressKey is what a name target's address is kept by: the network of
// its lookup and the name. A target given as an address has none ("").
func addressKey(t *Target) string {
	if _, err := netip.ParseAddr(t.Host); err == nil {
		return ""
	}
	return lookupNetwork(t.AF) + " " + t.Host
}

// addressKeys returns the keys of the targets' names, each once.
func addressKeys(targets []Target) []string {
	var keys []string
	seen := map[string]bool{}
	for i := range targets {
		if k := addressKey(&targets[i]); k != "" && !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// usable reports whether ip, read from a store, can be the address of key's
// targets: of the family the lookup asks for, as pickAddr returns them, and
// with a zone a record may carry.
func usable(key string, ip netip.Addr) bool {
	network, _, _ := strings.Cut(key, " ")
	switch {
	case !ip.IsValid() || ip.Is4In6():
		return false
	case ip.Zone() != "" && !record.ValidZone(ip.Zone()):
		return false
	case network == "ip4":
		return ip.Is4()
	case network == "ip6":
		return ip.Is6()
	}
	return true
}
