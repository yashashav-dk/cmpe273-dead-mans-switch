package monitor

import (
	"context"
	"time"

	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
)

// Evaluator periodically asks the Detector for each registered worker's
// suspicion + state and pushes the result into the Registry. Transitions are
// driven exclusively from Suspicion() polls (see spec §3) — the Detector
// itself never transitions the registry.
type Evaluator struct {
	reg          *Registry
	det          detector.Detector
	detectorName string // "phi" or "fixed", carried into log events
}

func NewEvaluator(reg *Registry, det detector.Detector, detectorName string) *Evaluator {
	return &Evaluator{reg: reg, det: det, detectorName: detectorName}
}

// Run blocks until ctx is cancelled, ticking every interval and calling tickAt
// with time.Now() each time.
func (e *Evaluator) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			e.tickAt(now)
		}
	}
}

// tickAt evaluates every known worker against the detector at the given
// timestamp. Exposed (lowercase) so tests can drive it deterministically.
func (e *Evaluator) tickAt(now time.Time) {
	for _, id := range e.reg.Workers() {
		susp, state := e.det.Suspicion(id, now)
		e.reg.Transition(id, state, susp, e.detectorName)
	}
}
