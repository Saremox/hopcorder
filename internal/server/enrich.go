package server

import (
	"net/netip"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/enrich"
)

// Enrichment adapts the enrich package to Enricher. Either part may be nil
// (disabled).
type Enrichment struct {
	ASNs  *enrich.ASNSource
	Names *enrich.RDNS
}

// ASN implements Enricher.
func (e Enrichment) ASN(ip netip.Addr) (uint32, string) {
	if e.ASNs == nil {
		return 0, ""
	}
	info, _ := e.ASNs.Lookup(ip)
	return info.ASN, info.Name
}

// Name implements Enricher.
func (e Enrichment) Name(ip netip.Addr) string {
	if e.Names == nil {
		return ""
	}
	return e.Names.Name(ip)
}

// RegisterEnrichmentMetrics exposes enrichment health on reg.
func RegisterEnrichmentMetrics(reg prometheus.Registerer, e Enrichment) {
	if e.ASNs != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hopcorder_server_asn_db_age_seconds", Help: "Age of the ASN database in use (-1: none loaded).",
		}, func() float64 {
			if a := e.ASNs.Age(); a >= 0 {
				return a.Seconds()
			}
			return -1
		}))
	}
	if r := e.Names; r != nil {
		stat := func(f func(enrich.RDNSStats) uint64) func() float64 {
			return func() float64 { return float64(f(r.Stats())) }
		}
		reg.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "hopcorder_server_rdns_cache_hits_total",
				Help: "rDNS cache hits."}, stat(func(s enrich.RDNSStats) uint64 { return s.Hits })),
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "hopcorder_server_rdns_cache_misses_total",
				Help: "rDNS cache misses."}, stat(func(s enrich.RDNSStats) uint64 { return s.Misses })),
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "hopcorder_server_rdns_lookups_total",
				Help: "rDNS lookups performed."}, stat(func(s enrich.RDNSStats) uint64 { return s.Lookups })),
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "hopcorder_server_rdns_queue_dropped_total",
				Help: "rDNS lookups not queued because the queue was full."},
				stat(func(s enrich.RDNSStats) uint64 { return s.Dropped })),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "hopcorder_server_rdns_cache_entries",
				Help: "Entries in the rDNS cache."}, func() float64 { return float64(r.Stats().Entries) }),
		)
	}
}
