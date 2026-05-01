// Package monitor implements the Monitor side of the dead-man's-switch system:
// the gRPC server that receives push heartbeats, the gRPC client pool that
// sends pull pings, the node registry, and the periodic evaluator that asks
// the failure detector for each worker's state.
package monitor

import (
	"sort"
	"sync"
	"time"

	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
)

// Worker is the registry's view of a single worker.
type Worker struct {
	ID            string
	Addr          string
	State         detector.State
	LastHeartbeat time.Time // monotonic; zero if never received
	LastSuspicion float64
}

// Registry tracks the set of known workers and their categorical states.
// It logs every transition (including recoveries to ALIVE) to the event log.
type Registry struct {
	log *eventlog.Logger

	mu      sync.RWMutex
	workers map[string]*Worker
}

func NewRegistry(log *eventlog.Logger) *Registry {
	return &Registry{
		log:     log,
		workers: make(map[string]*Worker),
	}
}

// Register adds a worker. Returns false if the worker_id is already present.
func (r *Registry) Register(id, addr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.workers[id]; exists {
		r.log.Event(time.Now(), eventlog.Event{
			Type: "register", Worker: id, Addr: addr, Accepted: false, Error: "duplicate_worker_id",
		})
		return false
	}
	r.workers[id] = &Worker{ID: id, Addr: addr, State: detector.Alive}
	r.log.Event(time.Now(), eventlog.Event{
		Type: "register", Worker: id, Addr: addr, Accepted: true,
	})
	return true
}

// OnHeartbeat records a heartbeat arrival and auto-registers the worker if
// it isn't known yet (push mode lets workers appear without an explicit
// Register call).
func (r *Registry) OnHeartbeat(id string, arrival time.Time, transport string) {
	r.mu.Lock()
	w, ok := r.workers[id]
	if !ok {
		w = &Worker{ID: id, State: detector.Alive}
		r.workers[id] = w
	}
	w.LastHeartbeat = arrival
	r.mu.Unlock()

	r.log.Event(time.Now(), eventlog.Event{
		Type: "hb", Worker: id, Transport: transport,
	})
}

// Transition records a state change for a worker. If newState equals the
// current state it is a no-op (no log line emitted), so the evaluator can
// call this on every tick without flooding the log.
func (r *Registry) Transition(id string, newState detector.State, suspicion float64, detectorName string) {
	r.mu.Lock()
	w, ok := r.workers[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	if w.State == newState {
		w.LastSuspicion = suspicion
		r.mu.Unlock()
		return
	}
	from := w.State
	w.State = newState
	w.LastSuspicion = suspicion
	r.mu.Unlock()

	r.log.Event(time.Now(), eventlog.Event{
		Type:      "state",
		Worker:    id,
		From:      from.String(),
		To:        newState.String(),
		Suspicion: suspicion,
		Detector:  detectorName,
	})
}

// StateOf returns the current categorical state of a worker, or Alive if
// unknown (callers should typically check existence with Workers() first).
func (r *Registry) StateOf(id string) detector.State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if w, ok := r.workers[id]; ok {
		return w.State
	}
	return detector.Alive
}

// Workers returns a stable, sorted list of known worker IDs.
func (r *Registry) Workers() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.workers))
	for id := range r.workers {
		out = append(out, id)
	}
	r.mu.RUnlock()
	sort.Strings(out)
	return out
}

// Snapshot returns a copy of the worker view (for the TUI and tests).
func (r *Registry) Snapshot() []Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Worker, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
