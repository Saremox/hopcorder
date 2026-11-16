package output

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJSONLFileAppends(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out.jsonl")
	if err := os.WriteFile(p, []byte("{\"old\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	j, err := NewJSONL(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Deliver(t.Context(), [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := j.Deliver(t.Context(), [][]byte{[]byte(`{"c":3}`)}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(p)
	if want := "{\"old\":1}\n{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"; string(got) != want {
		t.Errorf("file = %q, want %q", got, want)
	}
}

func TestJSONLWriter(t *testing.T) {
	var buf bytes.Buffer
	j := NewJSONLWriter(&buf)
	if err := j.Deliver(t.Context(), [][]byte{[]byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "{}\n" || j.Name() != "jsonl" || j.Close() != nil {
		t.Errorf("got %q", buf.String())
	}
}

func TestJSONLOpenError(t *testing.T) {
	if _, err := NewJSONL(filepath.Join(t.TempDir(), "missing", "x.jsonl")); err == nil {
		t.Error("opening a file in a missing directory succeeded")
	}
}

// shortWriter tears the first write and fails, then works.
type shortWriter struct {
	buf    bytes.Buffer
	failed bool
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if !w.failed {
		w.failed = true
		n, _ := w.buf.Write(p[:len(p)/2-3]) // tears the first record
		return n, errors.New("disk full")
	}
	return w.buf.Write(p)
}

func TestJSONLShortWriteStartsFreshLine(t *testing.T) {
	w := &shortWriter{}
	j := NewJSONLWriter(w)
	batch := [][]byte{[]byte(`{"seq":1}`), []byte(`{"seq":2}`)}
	if err := j.Deliver(t.Context(), batch); err == nil {
		t.Fatal("short write not reported")
	}
	if err := j.Deliver(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(w.buf.String(), "\n"), "\n")
	// The torn fragment is on its own line; both records follow intact.
	if len(lines) != 3 || lines[1] != `{"seq":1}` || lines[2] != `{"seq":2}` {
		t.Errorf("output %q", w.buf.String())
	}
}

func TestJSONLTornTailFromCrash(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out.jsonl")
	if err := os.WriteFile(p, []byte("{\"seq\":1}\n{\"se"), 0o600); err != nil {
		t.Fatal(err)
	}
	j, err := NewJSONL(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Deliver(t.Context(), [][]byte{[]byte(`{"seq":2}`)}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	got, _ := os.ReadFile(p)
	if want := "{\"seq\":1}\n{\"se\n{\"seq\":2}\n"; string(got) != want {
		t.Errorf("file = %q, want %q", got, want)
	}
}
