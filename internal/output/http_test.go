package output

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

type capturedRequest struct {
	path, auth, ctype, cenc, ua, custom, host string
	body                                      string
	user, pass                                string
	hasBasic                                  bool
	clientCN                                  string
}

type ingestServer struct {
	mu     sync.Mutex
	reqs   []capturedRequest
	status int
}

func (s *ingestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dec, _ := zstd.NewReader(r.Body)
	body, err := io.ReadAll(dec)
	dec.Close()
	if err != nil {
		http.Error(w, "bad zstd: "+err.Error(), http.StatusBadRequest)
		return
	}
	c := capturedRequest{path: r.URL.Path, auth: r.Header.Get("Authorization"), ctype: r.Header.Get("Content-Type"),
		cenc: r.Header.Get("Content-Encoding"), ua: r.Header.Get("User-Agent"), custom: r.Header.Get("X-Tenant"),
		host: r.Host, body: string(body)}
	c.user, c.pass, c.hasBasic = r.BasicAuth()
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		c.clientCN = r.TLS.PeerCertificates[0].Subject.CommonName
	}
	s.mu.Lock()
	s.reqs = append(s.reqs, c)
	status := s.status
	s.mu.Unlock()
	if status != 0 {
		http.Error(w, "ingest unavailable: maintenance", status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *ingestServer) last(t *testing.T) capturedRequest {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reqs) == 0 {
		t.Fatal("no request received")
	}
	return s.reqs[len(s.reqs)-1]
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestHTTPDeliver(t *testing.T) {
	srv := &ingestServer{}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	h, err := NewHTTP(HTTPConfig{URL: ts.URL + "/base/", Headers: map[string]string{"X-Tenant": "lab", "Host": "ingest.example"},
		BearerToken: "s3cret", UserAgent: "hopcorder/test"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := h.Deliver(t.Context(), [][]byte{[]byte(`{"seq":1}`), []byte(`{"seq":2}`)}); err != nil {
		t.Fatal(err)
	}
	r := srv.last(t)
	want := capturedRequest{path: "/base/api/v1/ingest", auth: "Bearer s3cret", ctype: "application/x-ndjson",
		cenc: "zstd", ua: "hopcorder/test", custom: "lab", host: "ingest.example", body: "{\"seq\":1}\n{\"seq\":2}\n"}
	if r != want {
		t.Errorf("request\n got %+v\nwant %+v", r, want)
	}
}

func TestHTTPURLAlreadyHasIngestPath(t *testing.T) {
	h, err := NewHTTP(HTTPConfig{URL: "https://x.example/api/v1/ingest"})
	if err != nil {
		t.Fatal(err)
	}
	if h.URL() != "https://x.example/api/v1/ingest" {
		t.Errorf("url %s", h.URL())
	}
}

func TestHTTPSecretFilesAreReread(t *testing.T) {
	srv := &ingestServer{}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	dir := t.TempDir()
	tok := writeFile(t, dir, "token", "first\n")
	h, err := NewHTTP(HTTPConfig{URL: ts.URL, BearerTokenFile: tok})
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Deliver(t.Context(), [][]byte{[]byte(`{}`)})
	if a := srv.last(t).auth; a != "Bearer first" {
		t.Errorf("auth %q", a)
	}
	writeFile(t, dir, "token", "second")
	_ = h.Deliver(t.Context(), [][]byte{[]byte(`{}`)})
	if a := srv.last(t).auth; a != "Bearer second" {
		t.Errorf("rotated token not used: %q", a)
	}

	pw := writeFile(t, dir, "pw", "hunter2\n")
	h, err = NewHTTP(HTTPConfig{URL: ts.URL, BasicUser: "agent", BasicPasswordFile: pw})
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Deliver(t.Context(), [][]byte{[]byte(`{}`)})
	if r := srv.last(t); !r.hasBasic || r.user != "agent" || r.pass != "hunter2" {
		t.Errorf("basic auth %+v", r)
	}

	if _, err := NewHTTP(HTTPConfig{URL: ts.URL, BearerTokenFile: filepath.Join(dir, "missing")}); err == nil {
		t.Error("missing token file accepted at startup")
	}
	empty := writeFile(t, dir, "empty", "\n")
	if _, err := NewHTTP(HTTPConfig{URL: ts.URL, BearerTokenFile: empty}); err == nil {
		t.Error("empty token file accepted")
	}
}

func TestHTTPErrorStatus(t *testing.T) {
	srv := &ingestServer{status: http.StatusServiceUnavailable}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	h, _ := NewHTTP(HTTPConfig{URL: ts.URL})
	err := h.Deliver(t.Context(), [][]byte{[]byte(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "maintenance") {
		t.Errorf("err = %v, want status and body excerpt", err)
	}
	ts.Close()
	if err := h.Deliver(t.Context(), [][]byte{[]byte(`{}`)}); err == nil {
		t.Error("delivery to a closed server succeeded")
	}
}

func TestHTTPTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer ts.Close()
	h, _ := NewHTTP(HTTPConfig{URL: ts.URL, Timeout: 100 * time.Millisecond})
	start := time.Now()
	if err := h.Deliver(t.Context(), [][]byte{[]byte(`{}`)}); err == nil {
		t.Error("hung server did not time out")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("timeout took %v", d)
	}
}

// genCert returns PEM certificate and key for a self-signed certificate.
func genCert(t *testing.T, cn string, client bool) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if client {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
}

func TestHTTPMutualTLS(t *testing.T) {
	dir := t.TempDir()
	clientCert, clientKey := genCert(t, "agent-lab", true)
	clientPool := x509.NewCertPool()
	clientPool.AppendCertsFromPEM(clientCert)

	srv := &ingestServer{}
	ts := httptest.NewUnstartedServer(srv)
	ts.Config.ErrorLog = log.New(io.Discard, "", 0) // the rejected handshakes below are expected
	ts.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientPool, MinVersion: tls.VersionTLS12}
	ts.StartTLS()
	defer ts.Close()
	caFile := writeFile(t, dir, "ca.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw})))
	certFile := writeFile(t, dir, "client.pem", string(clientCert))
	keyFile := writeFile(t, dir, "client.key", string(clientKey))

	h, err := NewHTTP(HTTPConfig{URL: ts.URL, CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Deliver(t.Context(), [][]byte{[]byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if cn := srv.last(t).clientCN; cn != "agent-lab" {
		t.Errorf("client cert CN %q", cn)
	}

	// Without the custom CA the server's certificate is not trusted.
	h, _ = NewHTTP(HTTPConfig{URL: ts.URL, CertFile: certFile, KeyFile: keyFile})
	if err := h.Deliver(t.Context(), [][]byte{[]byte(`{}`)}); err == nil {
		t.Error("untrusted server certificate accepted")
	}
	// Without a client certificate the server refuses the handshake.
	h, _ = NewHTTP(HTTPConfig{URL: ts.URL, CAFile: caFile, ServerName: "example.com"})
	if err := h.Deliver(t.Context(), [][]byte{[]byte(`{}`)}); err == nil {
		t.Error("connection without client certificate accepted")
	}

	bad := writeFile(t, dir, "bad.pem", "not a cert")
	if _, err := NewHTTP(HTTPConfig{URL: ts.URL, CAFile: bad}); err == nil {
		t.Error("garbage CA file accepted")
	}
	if _, err := NewHTTP(HTTPConfig{URL: ts.URL, CertFile: certFile, KeyFile: bad}); err == nil {
		t.Error("garbage key accepted")
	}
}
