// Package ui serves the embedded web UI: the Vue application in ui/ at the
// repository root, built into dist/ by `make ui` (npm run build) and
// committed, so the Go build needs no Node.js.
package ui

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed all:dist
var dist embed.FS

// SecurityPolicy is the Content-Security-Policy of every UI response: only
// the UI's own scripts, styles and API. Inline styles are allowed because
// the chart library styles its tooltips inline; inline scripts are not.
const SecurityPolicy = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; " +
	"form-action 'none'; frame-ancestors 'none'"

type file struct {
	name  string
	data  []byte
	gz    []byte // nil when compression does not pay off
	etag  string
	ctype string
}

// Handler serves the UI below /ui/: index.html at /ui/ and the hashed assets
// below /ui/assets/ (cached for a year; index.html is revalidated). Mount it
// at "GET /ui/".
func Handler() (http.Handler, error) {
	root, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil, fmt.Errorf("ui: %w", err)
	}
	return newHandler(root)
}

func newHandler(root fs.FS) (http.Handler, error) {
	files := map[string]*file{}
	err := fs.WalkDir(root, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return err
		}
		b, err := fs.ReadFile(root, p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		f := &file{name: p, data: b, etag: `"` + hex.EncodeToString(sum[:8]) + `"`,
			ctype: mime.TypeByExtension(path.Ext(p))}
		if f.ctype == "" {
			f.ctype = http.DetectContentType(b)
		}
		if compressible(f.ctype) && len(b) > 1024 {
			var buf bytes.Buffer
			zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
			_, _ = zw.Write(b)
			_ = zw.Close()
			if buf.Len() < len(b)*9/10 {
				f.gz = buf.Bytes()
			}
		}
		files["/"+p] = f
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("ui: read embedded files: %w", err)
	}
	if files["/index.html"] == nil {
		return nil, fmt.Errorf("ui: no index.html in the embedded build")
	}
	files["/"] = files["/index.html"]
	delete(files, "/index.html")

	return http.StripPrefix("/ui", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", SecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		f := files[r.URL.Path]
		if f == nil {
			http.NotFound(w, r)
			return
		}
		h.Set("Content-Type", f.ctype)
		if strings.HasPrefix(r.URL.Path, "/assets/") {
			h.Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			h.Set("Cache-Control", "no-cache")
		}
		body, etag := f.data, f.etag
		if f.gz != nil {
			h.Add("Vary", "Accept-Encoding")
			if acceptsGzip(r) {
				h.Set("Content-Encoding", "gzip")
				r.Header.Del("Range") // ranges of the compressed bytes would confuse clients
				body, etag = f.gz, strings.TrimSuffix(f.etag, `"`)+`-gzip"`
			}
		}
		h.Set("ETag", etag)
		http.ServeContent(w, r, f.name, time.Time{}, bytes.NewReader(body))
	})), nil
}

func compressible(ctype string) bool {
	return strings.HasPrefix(ctype, "text/") || strings.Contains(ctype, "javascript") ||
		strings.Contains(ctype, "json") || strings.Contains(ctype, "svg")
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		enc, q, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(strings.TrimSpace(enc), "gzip") && strings.TrimSpace(q) != "q=0" {
			return true
		}
	}
	return false
}
