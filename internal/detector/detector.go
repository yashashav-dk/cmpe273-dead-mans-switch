// Package detector defines the failure-detection interface and the two
// concrete detectors used by the Monitor (Fixed Window and Phi Accrual).
//
// All detectors are transport-agnostic: they receive (workerID, arrivalTime)
// events via Heartbeat() and answer Suspicion() queries from the evaluator
// goroutine. Time is always provided by the caller, never read from the
// system clock inside this package — that lets tests drive deterministic
// scenarios.
package detector

import (
	"fmt"
	"time"
)

// State is the registry's view of a worker.
type State int

const (
	Alive State = iota
	Missing
	Dead
)

func (s State) String() string {
	switch s {
	case Alive:
		return "ALIVE"
	case Missing:
		return "MISSING"
	case Dead:
		return "DEAD"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// ParseState is the inverse of String. Returns an error for unknown names.
func ParseState(name string) (State, error) {
	switch name {
	case "ALIVE":
		return Alive, nil
	case "MISSING":
		return Missing, nil
	case "DEAD":
		return Dead, nil
	default:
		return 0, fmt.Errorf("detector: unknown state %q", name)
	}
}

// Detector is the contract every failure detector implements.
//
// Heartbeat is called when a heartbeat arrives (push) or when a Ping reply
// succeeds (pull). The arrival time is supplied by the caller so the detector
// is fully deterministic in tests; in production callers pass time.Now().
//
// Suspicion is called on a periodic tick by the evaluator (see
// internal/monitor/evaluator.go). It returns a unitless monotone "how dead
// does this look" number for logging plus the categorical state. Callers
// drive transitions from State alone; suspicion is informational.
//
// For PhiAccrual, suspicion is the phi value. For FixedWindow, suspicion is
// elapsed/T_dead clamped at >= 0; suspicion >= 1 means DEAD.
type Detector interface {
	Heartbeat(workerID string, arrival time.Time)
	Suspicion(workerID string, now time.Time) (suspicion float64, state State)
	// Forget removes a worker from the detector's internal state.
	Forget(workerID string)
}
