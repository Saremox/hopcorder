package ui

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"
)

type response struct {
	code   int
	header http.Header
	body   string
}

func get(t *testing.T, h http.Handler, path string, hdr map[string]string) response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return response{rec.Code, rec.Header(), rec.Body.String()}
}

func TestHandler(t *testing.T) {
	big := strings.Repeat("console.log('hopcorder');\n", 200)
	h, err := newHandler(fstest.MapFS{
		"index.html":         {Data: []byte(`<!doctype html><script src="./assets/app-1.js"></script>`)},
		"assets/app-1.js":    {Data: []byte(big)},
		"assets/app-1.css":   {Data: []byte("body{}")},
		"assets/.gitkeep":    {Data: nil},
		"assets/logo-2.webp": {Data: []byte("RIFF....WEBP")},
	})
	if err != nil {
		t.Fatal(err)
	}

	r := get(t, h, "/ui/", nil)
	if r.code != http.StatusOK || !strings.HasPrefix(r.header.Get("Content-Type"), "text/html") ||
		r.header.Get("Cache-Control") != "no-cache" || r.header.Get("Content-Security-Policy") != SecurityPolicy ||
		r.header.Get("X-Content-Type-Options") != "nosniff" || r.header.Get("X-Frame-Options") != "DENY" {
		t.Errorf("index: %d %v", r.code, r.header)
	}
	etag := r.header.Get("ETag")
	if r := get(t, h, "/ui/", map[string]string{"If-None-Match": etag}); r.code != http.StatusNotModified {
		t.Errorf("revalidation: %d", r.code)
	}

	// Assets: immutable, gzip when accepted and worth it.
	r = get(t, h, "/ui/assets/app-1.js", map[string]string{"Accept-Encoding": "br, gzip"})
	if r.code != http.StatusOK || r.header.Get("Content-Encoding") != "gzip" || !strings.Contains(r.header.Get("Vary"), "Accept-Encoding") ||
		r.header.Get("Cache-Control") != "public, max-age=31536000, immutable" || !strings.Contains(r.header.Get("Content-Type"), "javascript") {
		t.Fatalf("js gzip: %d %v", r.code, r.header)
	}
	zr, err := gzip.NewReader(strings.NewReader(r.body))
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(zr); string(b) != big {
		t.Error("gzip body differs")
	}
	gzTag := r.header.Get("ETag")
	r = get(t, h, "/ui/assets/app-1.js", map[string]string{"Accept-Encoding": "gzip;q=0"})
	if r.header.Get("Content-Encoding") != "" || r.body != big || r.header.Get("ETag") == gzTag {
		t.Errorf("identity: %v", r.header)
	}
	// Small files are not compressed.
	if r := get(t, h, "/ui/assets/app-1.css", map[string]string{"Accept-Encoding": "gzip"}); r.header.Get("Content-Encoding") != "" ||
		!strings.HasPrefix(r.header.Get("Content-Type"), "text/css") {
		t.Errorf("css: %v", r.header)
	}

	// No directory listings, dotfiles, index.html alias or paths outside /ui.
	for _, p := range []string{"/ui/assets/", "/ui/assets/.gitkeep", "/ui/index.html", "/ui/nope.js", "/ui/../go.mod"} {
		if r := get(t, h, p, nil); r.code != http.StatusNotFound {
			t.Errorf("%s: %d", p, r.code)
		}
	}
}

func TestHandlerNeedsIndex(t *testing.T) {
	if _, err := newHandler(fstest.MapFS{"assets/a.js": {Data: []byte("x")}}); err == nil {
		t.Error("build without index.html accepted")
	}
}

// The committed build is complete: everything index.html loads is served.
func TestEmbeddedBuild(t *testing.T) {
	h, err := Handler()
	if err != nil {
		t.Fatal(err)
	}
	r := get(t, h, "/ui/", nil)
	index := r.body
	if r.code != http.StatusOK || !strings.Contains(index, `<div id="app">`) {
		t.Fatalf("index: %d %s", r.code, index)
	}
	refs := regexp.MustCompile(`(?:src|href)="\./(assets/[^"]+)"`).FindAllStringSubmatch(index, -1)
	if len(refs) < 2 {
		t.Fatalf("index.html references %d assets: %s", len(refs), index)
	}
	for _, m := range refs {
		r := get(t, h, "/ui/"+m[1], nil)
		if b := r.body; r.code != http.StatusOK || len(b) == 0 {
			t.Errorf("%s: %d, %d bytes", m[1], r.code, len(b))
		}
	}
	if bytes.Contains([]byte(index), []byte("<script>")) {
		t.Error("inline script in index.html would be blocked by the CSP")
	}
}
