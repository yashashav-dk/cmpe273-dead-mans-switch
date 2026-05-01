package detector

import (
	"testing"
	"time"
)

func TestFixedWindow_StateTransitions(t *testing.T) {
	hbInterval := 1 * time.Second
	d := NewFixedWindow(hbInterval, 3, 10) // T_missing=3s, T_dead=10s
	t0 := time.Unix(0, 0)
	d.Heartbeat("w1", t0)

	cases := []struct {
		elapsed time.Duration
		want    State
	}{
		{500 * time.Millisecond, Alive},
		{2 * time.Second, Alive},
		{2999 * time.Millisecond, Alive},
		{3 * time.Second, Missing},
		{5 * time.Second, Missing},
		{9999 * time.Millisecond, Missing},
		{10 * time.Second, Dead},
		{30 * time.Second, Dead},
	}
	for _, c := range cases {
		_, got := d.Suspicion("w1", t0.Add(c.elapsed))
		if got != c.want {
			t.Errorf("elapsed=%s: got %s, want %s", c.elapsed, got, c.want)
		}
	}
}

func TestFixedWindow_SuspicionMonotone(t *testing.T) {
	d := NewFixedWindow(1*time.Second, 3, 10)
	t0 := time.Unix(0, 0)
	d.Heartbeat("w1", t0)

	prev := -1.0
	for s := 0; s <= 15; s++ {
		got, _ := d.Suspicion("w1", t0.Add(time.Duration(s)*time.Second))
		if got < prev {
			t.Errorf("suspicion not monotone at s=%d: %f < %f", s, got, prev)
		}
		prev = got
	}
}

func TestFixedWindow_UnknownWorker(t *testing.T) {
	d := NewFixedWindow(1*time.Second, 3, 10)
	susp, st := d.Suspicion("never-heartbeated", time.Unix(100, 0))
	if st != Dead {
		t.Errorf("unknown worker: got %s, want DEAD", st)
	}
	if susp < 1.0 {
		t.Errorf("unknown worker suspicion: got %f, want >= 1", susp)
	}
}

func TestFixedWindow_HeartbeatRecovers(t *testing.T) {
	d := NewFixedWindow(1*time.Second, 3, 10)
	t0 := time.Unix(0, 0)
	d.Heartbeat("w1", t0)
	if _, st := d.Suspicion("w1", t0.Add(5*time.Second)); st != Missing {
		t.Fatalf("setup: expected MISSING, got %s", st)
	}
	d.Heartbeat("w1", t0.Add(5*time.Second))
	if _, st := d.Suspicion("w1", t0.Add(5*time.Second+100*time.Millisecond)); st != Alive {
		t.Errorf("after heartbeat: got %s, want ALIVE", st)
	}
}

func TestFixedWindow_Forget(t *testing.T) {
	d := NewFixedWindow(1*time.Second, 3, 10)
	t0 := time.Unix(0, 0)
	d.Heartbeat("w1", t0)
	d.Forget("w1")
	_, st := d.Suspicion("w1", t0.Add(1*time.Second))
	if st != Dead {
		t.Errorf("after forget: got %s, want DEAD (worker unknown)", st)
	}
}
