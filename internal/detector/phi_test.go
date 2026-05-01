package detector

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

const (
	hb         = 1 * time.Second
	winSize    = 1000
	minSamples = 10
	phiMissing = 1.0
	phiDead    = 8.0
)

func newPhi(t *testing.T) *PhiAccrual {
	t.Helper()
	return NewPhiAccrual(hb, winSize, minSamples, phiMissing, phiDead, NewFixedWindow(hb, 3, 10))
}

// feed sends `n` heartbeats at exactly `interval` starting at t0; returns the
// arrival time of the last heartbeat.
func feed(d *PhiAccrual, id string, t0 time.Time, interval time.Duration, n int) time.Time {
	last := t0
	for i := 0; i < n; i++ {
		d.Heartbeat(id, last)
		last = last.Add(interval)
	}
	return last.Add(-interval)
}

func TestPhi_BootstrapFallsBackToFixed(t *testing.T) {
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	d.Heartbeat("w1", t0) // 1 sample, below minSamples=10

	// Within k_miss=3*1s, fixed says ALIVE.
	if _, st := d.Suspicion("w1", t0.Add(2*time.Second)); st != Alive {
		t.Errorf("bootstrap @ 2s: got %s, want ALIVE", st)
	}
	// Past k_dead=10*1s, fixed says DEAD.
	if _, st := d.Suspicion("w1", t0.Add(15*time.Second)); st != Dead {
		t.Errorf("bootstrap @ 15s: got %s, want DEAD", st)
	}
}

func TestPhi_SteadyArrivalsStayLow(t *testing.T) {
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	last := feed(d, "w1", t0, hb, 50) // 50 samples at 1s each

	susp, st := d.Suspicion("w1", last.Add(500*time.Millisecond))
	if st != Alive {
		t.Errorf("steady @ 0.5s after last: got %s, want ALIVE", st)
	}
	if susp >= phiMissing {
		t.Errorf("phi too high: %f, want < %f", susp, phiMissing)
	}
}

func TestPhi_BigGapDeclaresDead(t *testing.T) {
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	// Use jittered feed so sigma is realistic; perfect 1s intervals would
	// give sigma=0, an instant ALIVE->DEAD step that skips MISSING entirely
	// regardless of probe granularity.
	rng := rand.New(rand.NewSource(99))
	last := t0
	for i := 0; i < 50; i++ {
		jitter := time.Duration(rng.NormFloat64() * float64(80*time.Millisecond))
		last = last.Add(hb + jitter)
		d.Heartbeat("w1", last)
	}

	// Walk forward at 100ms granularity; the MISSING window is a few hundred
	// ms wide so coarser sampling can miss it.
	sawMissing := false
	sawDead := false
	for ms := 100; ms <= 30000; ms += 100 {
		_, st := d.Suspicion("w1", last.Add(time.Duration(ms)*time.Millisecond))
		if st == Missing {
			sawMissing = true
		}
		if st == Dead {
			sawDead = true
			break
		}
	}
	if !sawMissing {
		t.Error("never saw MISSING during gap")
	}
	if !sawDead {
		t.Error("never saw DEAD even after 30s gap")
	}
}

func TestPhi_JitterTolerant(t *testing.T) {
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	rng := rand.New(rand.NewSource(42))

	// 200 samples, mean=1s, stddev≈80ms (well-behaved jitter).
	last := t0
	for i := 0; i < 200; i++ {
		jitter := time.Duration(rng.NormFloat64() * float64(80*time.Millisecond))
		last = last.Add(hb + jitter)
		d.Heartbeat("w1", last)
	}

	// 0.5s after last arrival, still ALIVE; phi well below 8.
	susp, st := d.Suspicion("w1", last.Add(500*time.Millisecond))
	if st == Dead {
		t.Errorf("jitter false positive: got DEAD with phi=%f", susp)
	}
	if susp >= phiDead {
		t.Errorf("phi=%f >= %f under normal jitter", susp, phiDead)
	}
}

func TestPhi_LateArrivalEntersWindow(t *testing.T) {
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	last := feed(d, "w1", t0, hb, 50)

	muBefore, _ := d.stats("w1")

	// 5s late arrival.
	lateArrival := last.Add(5 * time.Second)
	d.Heartbeat("w1", lateArrival)

	muAfter, _ := d.stats("w1")
	if muAfter <= muBefore {
		t.Errorf("late arrival did not raise mean: before=%f after=%f", muBefore, muAfter)
	}
}

func TestPhi_Forget(t *testing.T) {
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	feed(d, "w1", t0, hb, 50)
	d.Forget("w1")
	_, st := d.Suspicion("w1", t0.Add(60*time.Second))
	if st != Dead {
		t.Errorf("after forget: got %s, want DEAD (unknown worker)", st)
	}
}

func TestPhi_PhiFormulaMonotone(t *testing.T) {
	// With μ=1s, σ≈80ms, phi should be strictly increasing in elapsed beyond μ.
	d := newPhi(t)
	t0 := time.Unix(0, 0)
	rng := rand.New(rand.NewSource(7))
	last := t0
	for i := 0; i < 200; i++ {
		jitter := time.Duration(rng.NormFloat64() * float64(80*time.Millisecond))
		last = last.Add(hb + jitter)
		d.Heartbeat("w1", last)
	}
	prev := math.Inf(-1)
	for ms := 1100; ms <= 5000; ms += 200 {
		susp, _ := d.Suspicion("w1", last.Add(time.Duration(ms)*time.Millisecond))
		if susp < prev {
			t.Errorf("phi non-monotone at +%dms: %f < %f", ms, susp, prev)
		}
		prev = susp
	}
}
