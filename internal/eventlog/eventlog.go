// Package eventlog writes one JSON object per line to an io.Writer. The Monitor
// uses it to record heartbeats, state transitions, ping failures, stream
// closes, and registrations. The schema is documented in the design spec
// §7.1; tests verify the wire format.
package eventlog

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Event is the union of all log record types. Use omitempty so callers can
// fill only the fields relevant to a given event type.
type Event struct {
	Type      string  `json:"type"`
	Worker    string  `json:"worker,omitempty"`
	Seq       int64   `json:"seq,omitempty"`
	Transport string  `json:"transport,omitempty"`
	Detector  string  `json:"detector,omitempty"`
	From      string  `json:"from,omitempty"`
	To        string  `json:"to,omitempty"`
	Suspicion float64 `json:"suspicion,omitempty"`
	Error     string  `json:"error,omitempty"`
	Addr      string  `json:"addr,omitempty"`
	Accepted  bool    `json:"accepted,omitempty"`
}

// Logger writes Events as JSONL to an underlying writer. It is safe for
// concurrent use; each Event call writes one complete line.
type Logger struct {
	mu sync.Mutex
	w  io.Writer
}

func NewLogger(w io.Writer) *Logger {
	return &Logger{w: w}
}

// envelope is what actually gets serialized; pulls Event fields up alongside
// the timestamp.
type envelope struct {
	TS string `json:"ts"`
	Event
}

// Event writes a single record. ts is the wall-clock time used in the "ts"
// field; the caller is expected to pass time.Now().UTC() in production.
func (l *Logger) Event(ts time.Time, e Event) {
	env := envelope{
		TS:    ts.UTC().Format(time.RFC3339Nano),
		Event: e,
	}
	b, err := json.Marshal(env)
	if err != nil {
		return // unreachable: Event has only marshalable types
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Write(b)
	l.w.Write([]byte{'\n'})
}
