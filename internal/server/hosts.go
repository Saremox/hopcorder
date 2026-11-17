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
//
// An entry "*.example.net" allows the names one label below example.net,
// as an Ingress host of that form routes them.
type hostCheck struct {
	allowed  map[string]bool
	wildcard []string // ".example.net" for "*.example.net"
	next     http.Handler
	s        *Server
	warned   atomic.Int64 // unix seconds of the last warning
}

// hostExempt are the paths served whatever the Host header says.
var hostExempt = map[string]bool{"/healthz": true, "/readyz": true, "/metrics": true}

func newHostCheck(s *Server, hosts []string, next http.Handler) http.Handler {
	if len(hosts) == 0 {
		return next
	}
	h := &hostCheck{allowed: map[string]bool{}, next: next, s: s}
	for _, name := range hosts {
		name = hostName(name)
		if domain, ok := strings.CutPrefix(name, "*."); ok {
			h.wildcard = append(h.wildcard, "."+domain)
			continue
		}
		h.allowed[name] = true
	}
	return h
}

// allows reports whether host (a Host header) is allowed.
func (h *hostCheck) allows(host string) bool {
	name := hostName(host)
	if h.allowed[name] {
		return true
	}
	for _, domain := range h.wildcard {
		if label, ok := strings.CutSuffix(name, domain); ok && label != "" && !strings.Contains(label, ".") {
			return true
		}
	}
	return false
}

func (h *hostCheck) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hostExempt[r.URL.Path] || h.allows(r.Host) {
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
