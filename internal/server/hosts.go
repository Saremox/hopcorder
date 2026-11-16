package server

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// hostCheck answers only requests whose Host header names one of the
// allowed hosts, so a web page cannot reach a server on localhost or an
// internal address through DNS rebinding (its own name resolving to the
// server's address): the browser still sends the page's host name. Health
// and metrics endpoints answer any host (probes and scrapers use addresses).
type hostCheck struct {
	allowed map[string]bool
	next    http.Handler
	s       *Server
	warned  atomic.Int64 // unix seconds of the last warning
}

// hostExempt are the paths served whatever the Host header says.
var hostExempt = map[string]bool{"/healthz": true, "/readyz": true, "/metrics": true}

func newHostCheck(s *Server, hosts []string, next http.Handler) http.Handler {
	if len(hosts) == 0 {
		return next
	}
	h := &hostCheck{allowed: map[string]bool{}, next: next, s: s}
	for _, name := range hosts {
		h.allowed[hostName(name)] = true
	}
	return h
}

func (h *hostCheck) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hostExempt[r.URL.Path] || h.allowed[hostName(r.Host)] {
		h.next.ServeHTTP(w, r)
		return
	}
	h.s.metrics.hostRefused.Inc()
	// At most one warning a minute: a misconfigured proxy refuses every
	// request, an attacker's page may send many.
	if now := time.Now().Unix(); h.warned.Load() < now-60 {
		h.warned.Store(now)
		h.s.log.Warn("refused a request for a host not in allowed_hosts (DNS rebinding protection)",
			"host", r.Host, "path", r.URL.Path)
	}
	http.Error(w, fmt.Sprintf("host %q is not in the server's allowed_hosts", r.Host), http.StatusMisdirectedRequest)
}

// hostName normalizes a Host header or an allowed_hosts entry for
// comparison: without port and IPv6 brackets, lower case, without a
// trailing dot.
func hostName(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		h = host
	}
	h = strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")
	return strings.TrimSuffix(strings.ToLower(h), ".")
}
