package monitor

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
)

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry(eventlog.NewLogger(&bytes.Buffer{}))
	if !r.Register("w1", "localhost:50061") {
		t.Error("first Register should accept")
	}
	if r.Register("w1", "localhost:50062") {
		t.Error("duplicate Register should reject")
	}
	if got := r.Workers(); len(got) != 1 {
		t.Errorf("Workers() len = %d, want 1", len(got))
	}
}

func TestRegistry_OnHeartbeatRegistersIfUnknown(t *testing.T) {
	r := NewRegistry(eventlog.NewLogger(&bytes.Buffer{}))
	r.OnHeartbeat("w1", time.Unix(0, 0), "push")
	if got := r.Workers(); len(got) != 1 || got[0] != "w1" {
		t.Errorf("OnHeartbeat should auto-register; got %v", got)
	}
}

func TestRegistry_TransitionLogsStateChange(t *testing.T) {
	var buf bytes.Buffer
	r := NewRegistry(eventlog.NewLogger(&buf))
	r.Register("w1", "localhost:50061")

	// ALIVE -> MISSING transition is logged.
	r.Transition("w1", detector.Missing, 1.42, "phi")
	out := buf.String()
	if !strings.Contains(out, `"type":"state"`) {
		t.Errorf("missing state event: %s", out)
	}
	if !strings.Contains(out, `"from":"ALIVE"`) || !strings.Contains(out, `"to":"MISSING"`) {
		t.Errorf("wrong from/to: %s", out)
	}
}

func TestRegistry_TransitionToSameStateIsNoop(t *testing.T) {
	var buf bytes.Buffer
	r := NewRegistry(eventlog.NewLogger(&buf))
	r.Register("w1", "localhost:50061")
	buf.Reset() // discard register line so we only see the transition (or absence)

	r.Transition("w1", detector.Alive, 0.0, "phi") // no change
	if buf.Len() != 0 {
		t.Errorf("noop transition should not log: %s", buf.String())
	}
}

func TestRegistry_TransitionLogsRecovery(t *testing.T) {
	var buf bytes.Buffer
	r := NewRegistry(eventlog.NewLogger(&buf))
	r.Register("w1", "localhost:50061")
	r.Transition("w1", detector.Missing, 1.5, "phi")
	buf.Reset()

	r.Transition("w1", detector.Alive, 0.05, "phi")
	if !strings.Contains(buf.String(), `"from":"MISSING"`) || !strings.Contains(buf.String(), `"to":"ALIVE"`) {
		t.Errorf("recovery not logged: %s", buf.String())
	}
}

func TestRegistry_StateOf(t *testing.T) {
	r := NewRegistry(eventlog.NewLogger(&bytes.Buffer{}))
	r.Register("w1", "x")
	if got := r.StateOf("w1"); got != detector.Alive {
		t.Errorf("initial state = %s, want ALIVE", got)
	}
	r.Transition("w1", detector.Dead, 9.9, "phi")
	if got := r.StateOf("w1"); got != detector.Dead {
		t.Errorf("after transition = %s, want DEAD", got)
	}
}
