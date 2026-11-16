package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

// Read-API response bounds.
const (
	// DefaultMaxResponseBytes bounds the rows of one read-API response.
	DefaultMaxResponseBytes = 32 << 20
	// DefaultMaxQueryMemory is ClickHouse's max_memory_usage for one
	// read-API query.
	DefaultMaxQueryMemory = 1 << 30
	// streamFlushBytes is how much a streamed response buffers before it
	// is written to the client.
	streamFlushBytes = 64 << 10
)

// errResponseFull ends a query whose next row does not fit the response.
var errResponseFull = errors.New("response byte budget reached")

// jsonStream writes a read-API response, a JSON object of arrays of rows, as
// the rows arrive, so a response never holds more than its byte budget of
// rows (and a streamed one only streamFlushBytes at a time). Rows that do
// not fit are dropped and the object ends with "truncated": true, like the
// row caps. Nothing is sent before the first flush, so a query that fails
// before that still gets a clean 502.
type jsonStream struct {
	w         http.ResponseWriter
	rc        *http.ResponseController
	buf       bytes.Buffer
	scratch   bytes.Buffer
	budget    int64 // row bytes still allowed
	flushAt   int   // buffered bytes that trigger a write; 0 buffers until done
	started   bool  // the status line and some of the body have been sent
	fields    int   // fields of the object written so far
	rows      int   // rows of the current array
	truncated bool
	err       error // writing to the client failed
}

// newJSONStream starts a response with a budget of row bytes. flushAt 0
// keeps the whole response (at most the budget) until done, so any failure
// can still be answered with an error status.
func newJSONStream(w http.ResponseWriter, budget int64, flushAt int) *jsonStream {
	j := &jsonStream{w: w, rc: http.NewResponseController(w), budget: budget, flushAt: flushAt}
	j.buf.WriteByte('{')
	return j
}

func (j *jsonStream) key(k string) {
	if j.fields > 0 {
		j.buf.WriteByte(',')
	}
	j.fields++
	j.buf.WriteString(strconv.Quote(k))
	j.buf.WriteByte(':')
}

// number adds a numeric field.
func (j *jsonStream) number(k string, v int64) {
	j.key(k)
	j.buf.WriteString(strconv.FormatInt(v, 10))
}

// beginArray opens an array field; rows go into it until endArray.
func (j *jsonStream) beginArray(k string) {
	j.key(k)
	j.buf.WriteByte('[')
	j.rows = 0
}

func (j *jsonStream) endArray() { j.buf.WriteByte(']') }

// row appends one JSON value to the open array, HTML-escaped like
// encoding/json's output. It returns errResponseFull, and marks the
// response truncated, if the row does not fit the budget.
func (j *jsonStream) row(b []byte) error {
	if j.err != nil {
		return j.err
	}
	j.scratch.Reset()
	json.HTMLEscape(&j.scratch, b)
	n := int64(j.scratch.Len()) + 1 // and a comma
	if n > j.budget {
		j.truncated = true
		j.budget = 0
		return errResponseFull
	}
	j.budget -= n
	if j.rows > 0 {
		j.buf.WriteByte(',')
	}
	j.rows++
	j.buf.Write(j.scratch.Bytes())
	if j.flushAt > 0 && j.buf.Len() >= j.flushAt {
		return j.flush()
	}
	return nil
}

// full reports whether the budget is used up.
func (j *jsonStream) full() bool { return j.budget <= 0 }

func (j *jsonStream) flush() error {
	if !j.started {
		j.started = true
		j.w.Header().Set("Content-Type", "application/json")
		j.w.WriteHeader(http.StatusOK)
	}
	if _, err := j.w.Write(j.buf.Bytes()); err != nil {
		j.err = err
		return err
	}
	j.buf.Reset()
	if err := j.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		j.err = err
		return err
	}
	return nil
}

// finish closes the object with the truncated flag and sends the rest.
func (j *jsonStream) finish() {
	j.key("truncated")
	j.buf.WriteString(strconv.FormatBool(j.truncated))
	j.buf.WriteString("}\n")
	_ = j.flush()
}

// fail answers a failed query: with 502 if nothing was sent yet, else by
// aborting the response, so the client sees a broken body rather than a
// complete-looking one.
func (j *jsonStream) fail() {
	if !j.started {
		writeJSON(j.w, http.StatusBadGateway, errorBody{Error: "query failed"})
		return
	}
	panic(http.ErrAbortHandler)
}
