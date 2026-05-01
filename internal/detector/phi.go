package detector

import (
	"math"
	"sync"
	"time"
)

// PhiAccrual implements the Akka-style Phi Accrual failure detector
// (Hayashibara et al. 2004). It maintains a fixed-size sliding window of
// inter-arrival intervals, computes phi from a Normal-CDF approximation,
// and falls back to a Fixed Window detector while it has fewer than
// minSamples observations.
//
// The window uses a fixed-size []float64 with running sum and sum-of-squares
// updated incrementally on insert/evict — never iterating the whole window
// inside Suspicion.
type PhiAccrual struct {
	hbInterval time.Duration
	winSize    int
	minSamples int
	phiMissing float64
	phiDead    float64
	fallback   *FixedWindow

	mu      sync.Mutex
	workers map[string]*phiState
}

type phiState struct {
	window   []float64 // length winSize, ring buffer
	writeIdx int
	count    int // number of valid samples (<= winSize)
	sum      float64
	sumSq    float64
	last     time.Time // time of most recent heartbeat
	hasLast  bool
}

// NewPhiAccrual constructs a detector. fallback is the Fixed Window used while
// the per-worker window has fewer than minSamples samples; pass a freshly
// constructed FixedWindow with conservative multipliers (typically the same
// defaults as the user's --miss-multiplier / --dead-multiplier).
func NewPhiAccrual(hbInterval time.Duration, winSize, minSamples int, phiMissing, phiDead float64, fallback *FixedWindow) *PhiAccrual {
	return &PhiAccrual{
		hbInterval: hbInterval,
		winSize:    winSize,
		minSamples: minSamples,
		phiMissing: phiMissing,
		phiDead:    phiDead,
		fallback:   fallback,
		workers:    make(map[string]*phiState),
	}
}

func (p *PhiAccrual) Heartbeat(workerID string, arrival time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	st, ok := p.workers[workerID]
	if !ok {
		st = &phiState{window: make([]float64, p.winSize)}
		p.workers[workerID] = st
	}

	if st.hasLast {
		interval := arrival.Sub(st.last).Seconds()
		// Late arrivals are recorded unconditionally (matches Akka behavior;
		// see spec §5.2 late-arrival policy).
		p.insertInterval(st, interval)
	}
	st.last = arrival
	st.hasLast = true

	// Mirror the heartbeat into the fallback so its decision after Forget or
	// during bootstrap is consistent.
	p.fallback.Heartbeat(workerID, arrival)
}

// Suspicion locks just long enough to snapshot mu/sigma/last, then releases
// before the (allocation-free, math-only) phi computation. A Heartbeat that
// lands between the snapshot and the return is intentionally not reflected in
// this verdict; the next eval tick will see it. That is fine: Suspicion is
// idempotent and the evaluator runs every --eval-interval.
func (p *PhiAccrual) Suspicion(workerID string, now time.Time) (float64, State) {
	p.mu.Lock()
	st, ok := p.workers[workerID]
	if !ok || !st.hasLast {
		p.mu.Unlock()
		return p.fallback.Suspicion(workerID, now)
	}
	if st.count < p.minSamples {
		p.mu.Unlock()
		return p.fallback.Suspicion(workerID, now)
	}
	mu, sigma := p.statsLocked(st)
	last := st.last
	p.mu.Unlock()

	if sigma <= 0 {
		// Degenerate: all samples identical. Use a tiny floor so phi is finite.
		sigma = 1e-9
	}
	elapsed := now.Sub(last).Seconds()
	phi := computePhi(elapsed, mu, sigma)

	switch {
	case phi < p.phiMissing:
		return phi, Alive
	case phi < p.phiDead:
		return phi, Missing
	default:
		return phi, Dead
	}
}

func (p *PhiAccrual) Forget(workerID string) {
	p.mu.Lock()
	delete(p.workers, workerID)
	p.mu.Unlock()
	p.fallback.Forget(workerID)
}

// stats returns the current mean and stddev for a worker; intended for tests
// and for the TUI. It locks the detector internally.
func (p *PhiAccrual) stats(workerID string) (mu, sigma float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.workers[workerID]
	if !ok || st.count == 0 {
		return 0, 0
	}
	return p.statsLocked(st)
}

// insertInterval appends `interval` (seconds) to the ring buffer, maintaining
// running sum and sumSq.
func (p *PhiAccrual) insertInterval(st *phiState, interval float64) {
	if st.count == p.winSize {
		// Evict oldest sample at writeIdx.
		old := st.window[st.writeIdx]
		st.sum -= old
		st.sumSq -= old * old
	} else {
		st.count++
	}
	st.window[st.writeIdx] = interval
	st.sum += interval
	st.sumSq += interval * interval
	st.writeIdx = (st.writeIdx + 1) % p.winSize
}

// statsLocked computes mean and population stddev from running totals.
// Caller must hold p.mu.
func (p *PhiAccrual) statsLocked(st *phiState) (mu, sigma float64) {
	n := float64(st.count)
	mu = st.sum / n
	variance := st.sumSq/n - mu*mu
	if variance < 0 {
		variance = 0
	}
	return mu, math.Sqrt(variance)
}

// computePhi returns -log10(P_later) using Akka's polynomial approximation to
// the Normal CDF survival function.
func computePhi(elapsed, mu, sigma float64) float64 {
	if elapsed <= mu {
		return 0
	}
	y := (elapsed - mu) / sigma
	pLater := math.Exp(-y * (1.5976 + 0.070566*y*y))
	if pLater <= 0 {
		return math.Inf(1)
	}
	return -math.Log10(pLater)
}
