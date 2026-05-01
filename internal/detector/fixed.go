package detector

import (
	"sync"
	"time"
)

// FixedWindow declares a worker MISSING after k_miss missed intervals and
// DEAD after k_dead missed intervals. Simple, deterministic, intolerant of
// jitter; included for the paper's comparison against PhiAccrual.
type FixedWindow struct {
	hbInterval time.Duration
	tMissing   time.Duration
	tDead      time.Duration

	mu   sync.Mutex
	last map[string]time.Time
}

// NewFixedWindow returns a FixedWindow with the given heartbeat interval and
// MISSING/DEAD multipliers. Both multipliers are integers, applied to
// hbInterval. kDead must be >= kMiss; the constructor does not enforce that.
func NewFixedWindow(hbInterval time.Duration, kMiss, kDead int) *FixedWindow {
	return &FixedWindow{
		hbInterval: hbInterval,
		tMissing:   hbInterval * time.Duration(kMiss),
		tDead:      hbInterval * time.Duration(kDead),
		last:       make(map[string]time.Time),
	}
}

func (f *FixedWindow) Heartbeat(workerID string, arrival time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last[workerID] = arrival
}

func (f *FixedWindow) Suspicion(workerID string, now time.Time) (float64, State) {
	f.mu.Lock()
	last, ok := f.last[workerID]
	f.mu.Unlock()
	if !ok {
		// Never heard from this worker; treat as DEAD with high suspicion.
		return 1e9, Dead
	}
	elapsed := now.Sub(last)
	if elapsed < 0 {
		elapsed = 0
	}
	susp := float64(elapsed) / float64(f.tDead)

	switch {
	case elapsed < f.tMissing:
		return susp, Alive
	case elapsed < f.tDead:
		return susp, Missing
	default:
		return susp, Dead
	}
}

func (f *FixedWindow) Forget(workerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.last, workerID)
}
