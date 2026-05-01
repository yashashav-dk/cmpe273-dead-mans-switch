package monitor

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
)

func TestEvaluator_TickMovesAliveToMissingToDead(t *testing.T) {
	var buf bytes.Buffer
	reg := NewRegistry(eventlog.NewLogger(&buf))
	det := detector.NewFixedWindow(1*time.Second, 3, 10)

	reg.Register("w1", "x")
	t0 := time.Unix(0, 0)
	reg.OnHeartbeat("w1", t0, "push")
	det.Heartbeat("w1", t0)

	ev := NewEvaluator(reg, det, "fixed")

	// At t0+5s, fixed says MISSING.
	ev.tickAt(t0.Add(5 * time.Second))
	if got := reg.StateOf("w1"); got != detector.Missing {
		t.Errorf("after 5s: got %s, want MISSING", got)
	}
	// At t0+15s, DEAD.
	ev.tickAt(t0.Add(15 * time.Second))
	if got := reg.StateOf("w1"); got != detector.Dead {
		t.Errorf("after 15s: got %s, want DEAD", got)
	}

	out := buf.String()
	if !strings.Contains(out, `"to":"MISSING"`) {
		t.Errorf("missing MISSING transition: %s", out)
	}
	if !strings.Contains(out, `"to":"DEAD"`) {
		t.Errorf("missing DEAD transition: %s", out)
	}
}

func TestEvaluator_HeartbeatRecovers(t *testing.T) {
	reg := NewRegistry(eventlog.NewLogger(&bytes.Buffer{}))
	det := detector.NewFixedWindow(1*time.Second, 3, 10)
	reg.Register("w1", "x")
	t0 := time.Unix(0, 0)
	reg.OnHeartbeat("w1", t0, "push")
	det.Heartbeat("w1", t0)
	ev := NewEvaluator(reg, det, "fixed")

	ev.tickAt(t0.Add(5 * time.Second))
	if got := reg.StateOf("w1"); got != detector.Missing {
		t.Fatalf("setup: got %s, want MISSING", got)
	}

	// Fresh heartbeat at t=5s.
	reg.OnHeartbeat("w1", t0.Add(5*time.Second), "push")
	det.Heartbeat("w1", t0.Add(5*time.Second))
	ev.tickAt(t0.Add(5*time.Second + 100*time.Millisecond))
	if got := reg.StateOf("w1"); got != detector.Alive {
		t.Errorf("after fresh heartbeat: got %s, want ALIVE", got)
	}
}
