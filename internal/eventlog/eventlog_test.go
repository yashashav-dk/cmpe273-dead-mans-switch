package eventlog

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestLogger_WritesOneJSONPerLine(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf)

	now := time.Date(2026, 4, 30, 14, 30, 1, 234_567_000, time.UTC)
	l.Event(now, Event{Type: "hb", Worker: "w1", Seq: 42, Transport: "push"})
	l.Event(now.Add(50*time.Millisecond), Event{Type: "state", Worker: "w1", From: "ALIVE", To: "MISSING", Suspicion: 1.42, Detector: "phi"})

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}

	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line 0 not valid JSON: %v\n%s", err, lines[0])
	}
	if first["type"] != "hb" {
		t.Errorf("type = %v, want \"hb\"", first["type"])
	}
	if first["worker"] != "w1" {
		t.Errorf("worker = %v, want \"w1\"", first["worker"])
	}
	if first["transport"] != "push" {
		t.Errorf("transport = %v, want \"push\"", first["transport"])
	}
	if !strings.HasPrefix(first["ts"].(string), "2026-04-30T14:30:01") {
		t.Errorf("ts = %v, want RFC3339Nano starting 2026-04-30T14:30:01", first["ts"])
	}
}

func TestLogger_OmitsZeroFields(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf)
	l.Event(time.Unix(0, 0), Event{Type: "register", Worker: "w1", Addr: "localhost:50061", Accepted: true})

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["seq"]; ok {
		t.Error("zero seq should be omitted")
	}
	if _, ok := got["from"]; ok {
		t.Error("empty from should be omitted")
	}
	if got["accepted"] != true {
		t.Errorf("accepted = %v, want true", got["accepted"])
	}
}
