// Package worker implements the Worker side of the dead-man's-switch system:
// the heartbeat pusher, the ping responder, and the chaos controller used to
// simulate failures (lag, drop, crash) for benchmarks.
package worker

import (
	"math/rand"
	"time"
)

// ChaosConfig is the worker's chaos-injection configuration; all fields are
// zero-valued by default (no chaos).
type ChaosConfig struct {
	LagMean    time.Duration // mean of injected latency before send
	LagStddev  time.Duration // jitter (Normal noise around LagMean)
	DropRate   float64       // probability of skipping a heartbeat / dropping a Ping reply
	KillAfter  time.Duration // exit(1) after this duration; zero means never
	CrashAfter time.Duration // exit(0) after this duration; zero means never
}

// Chaos is the runtime controller. SampleLag/ShouldDrop call into the rng
// without locking; callers are expected to invoke from a single goroutine
// (the pusher loop or the responder handler).
type Chaos struct {
	cfg     ChaosConfig
	rng     *rand.Rand
	started time.Time
}

func NewChaos(cfg ChaosConfig, rng *rand.Rand) *Chaos {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Chaos{cfg: cfg, rng: rng, started: time.Now()}
}

// StartedAt returns the time the chaos controller was constructed; kill/crash
// deadlines are measured from this point.
func (c *Chaos) StartedAt() time.Time { return c.started }

// SampleLag returns a non-negative duration sampled from N(LagMean, LagStddev²).
// Returns 0 if LagMean is zero.
func (c *Chaos) SampleLag() time.Duration {
	if c.cfg.LagMean == 0 && c.cfg.LagStddev == 0 {
		return 0
	}
	d := time.Duration(c.rng.NormFloat64()*float64(c.cfg.LagStddev)) + c.cfg.LagMean
	if d < 0 {
		return 0
	}
	return d
}

// ShouldDrop reports whether this heartbeat / reply should be skipped.
func (c *Chaos) ShouldDrop() bool {
	if c.cfg.DropRate <= 0 {
		return false
	}
	if c.cfg.DropRate >= 1 {
		return true
	}
	return c.rng.Float64() < c.cfg.DropRate
}

// ShouldKill reports whether a hard exit(1) should fire by the given moment.
// Returns false if KillAfter is zero.
func (c *Chaos) ShouldKill(now time.Time) bool {
	if c.cfg.KillAfter == 0 {
		return false
	}
	return now.Sub(c.started) >= c.cfg.KillAfter
}

// ShouldCrash reports whether a clean exit(0) should fire by the given moment.
func (c *Chaos) ShouldCrash(now time.Time) bool {
	if c.cfg.CrashAfter == 0 {
		return false
	}
	return now.Sub(c.started) >= c.cfg.CrashAfter
}
