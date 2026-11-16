package output

import (
	"bytes"
	"os"
	"path/filepath"
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
