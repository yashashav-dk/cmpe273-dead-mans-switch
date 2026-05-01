# Distributed Dead Man's Switch — Design Specification

**Date:** 2026-04-30
**Course:** CMPE 273 (San José State University)
**Author:** yashashav.dk@gmail.com
**Deliverable:** Research paper + Go implementation

---

## 1. Objective

Design and implement a high-availability monitoring system that detects node failures in a distributed environment using heartbeats. The deliverable is twofold:

1. A formal research paper covering two areas:
   - **Heartbeat Strategy** — Push vs Pull
   - **Timeout Dilemma** — when does a "missing" node become a "dead" node
2. A working Go implementation of the proposed failure-detection logic, including both detection algorithms and both heartbeat directions, so the paper can cite empirical numbers rather than only theory.

## 2. Scope

### In scope

- Single Monitor process observing N Worker processes.
- Two heartbeat transport patterns: **Push** (worker → monitor stream) and **Pull** (monitor polls worker).
- Two failure detectors: **Fixed Window** and **Phi Accrual** (the algorithm used by Cassandra and Akka).
- gRPC as the RPC transport.
- Chaos-injection knobs on Workers (crash, lag, drop heartbeat) for reproducible experiments.
- Live TUI dashboard on the Monitor.
- Structured JSON event log for post-run analysis and paper plots.
- Benchmarks at N = 10, 100, 1000 workers.
- A 6-section paper (introduction, heartbeat strategy, timeout dilemma, implementation, results, conclusion).

### Out of scope (explicitly deferred)

- Multi-monitor / replicated monitors. SPOF acknowledged in the paper.
- Persistence of registry across Monitor restart.
- Lease tokens, fencing tokens, or leadership election.
- TLS / mTLS on gRPC.
- Cross-host orchestration. All experiments run on a single machine; the paper discusses the difference.
- Re-integration / rejoin protocol after a worker is declared DEAD. (Workers may re-register, but the system has no formal recovery story.)

## 3. Architecture

```
┌──────────────────────────┐         ┌──────────────────────────┐
│         Monitor          │  gRPC   │         Worker N         │
│  (single process, TUI)   │◄───────►│  (single process)        │
│                          │         │                          │
│  ┌────────────────────┐  │  Push:  │  ┌────────────────────┐  │
│  │ NodeRegistry       │  │ Worker  │  │ HeartbeatPusher    │  │
│  │ (worker state)     │  │  →Mon   │  │ (timer → stream)   │  │
│  └─────────┬──────────┘  │         │  └────────────────────┘  │
│  ┌─────────▼──────────┐  │  Pull:  │  ┌────────────────────┐  │
│  │ Detector           │  │ Mon→W   │  │ PingResponder      │  │
│  │ - PhiAccrual       │  │  ping   │  │ (gRPC unary)       │  │
│  │ - FixedWindow      │  │         │  └────────────────────┘  │
│  └─────────┬──────────┘  │         │  ┌────────────────────┐  │
│  ┌─────────▼──────────┐  │         │  │ ChaosController    │  │
│  │ TUI + JSON Logger  │  │         │  │ (crash/lag/drop)   │  │
│  └────────────────────┘  │         │  └────────────────────┘  │
└──────────────────────────┘         └──────────────────────────┘
```

Two binaries: `monitor` and `worker`. Single Go module, shared `proto/` and `internal/detector/`.

A `--mode` flag on the Monitor selects which transport pattern runs:

- `push` — Monitor accepts worker-initiated streams and does not run its poller.
- `pull` — Monitor polls each registered worker via unary RPC and does not accept push streams.
- `hybrid` — Monitor runs both: accepts pushes, and also polls every registered worker. The detector receives events from whichever transport produces them first; duplicate arrivals within `--eval-interval` are deduplicated by `(worker_id, seq)` for push and by ping reply for pull. Hybrid is intended for the side-by-side comparison run that produces the paper's bandwidth charts; it is not the recommended production setting.

Workers always run both endpoints. The Monitor decides which side initiates.

### Worker state machine

```
ALIVE  ────(detector.Suspicion crosses Φ_missing)────►  MISSING
MISSING ────(detector.Suspicion crosses Φ_dead)────►  DEAD
MISSING ────(heartbeat received)────►  ALIVE
DEAD    ────(worker re-registers)────►  ALIVE   (rejoin path; minimal)
```

`MISSING` is "haven't heard recently, but not yet declared dead". `DEAD` triggers a state-change log event and a red row in the TUI.

## 4. gRPC Protocol

`proto/heartbeat.proto`:

```proto
syntax = "proto3";
package deadman.v1;

service Heartbeat {
  // Push mode: worker streams heartbeats to monitor
  rpc StreamHeartbeats(stream Heartbeat) returns (StreamAck);

  // Pull mode: monitor pings worker
  rpc Ping(PingRequest) returns (PingReply);

  // Worker explicit register/deregister (used in pull mode so monitor knows
  // who to poll; optional in push mode because the stream itself registers)
  rpc Register(RegisterRequest) returns (RegisterReply);
}

message Heartbeat {
  string worker_id        = 1;
  int64  seq              = 2;  // monotonic, lets monitor detect gaps
  int64  sent_unix_nanos  = 3;
  map<string,string> meta = 4;  // load, version, etc.
}

message StreamAck { bool ok = 1; }

message PingRequest  { int64 sent_unix_nanos = 1; }
message PingReply    {
  string worker_id          = 1;
  int64  sent_unix_nanos    = 2;  // echo
  int64  worker_unix_nanos  = 3;  // for clock-skew note in paper
}

message RegisterRequest  { string worker_id = 1; string addr = 2; }
message RegisterReply    { bool   accepted  = 1; int64 lease_ms = 2; }
```

The `lease_ms` field is informational only — the Monitor returns its expected heartbeat interval so the worker can adjust. There is no lease-revocation protocol in this design.

### Push path

The worker opens a long-lived `StreamHeartbeats` and sends a `Heartbeat` every `--hb-interval` (default 1s). The Monitor reads the stream, timestamps the *arrival time* using its own clock, and feeds the event to the Detector.

### Pull path

The Monitor maintains a list of registered workers. One goroutine per worker calls `Ping` every `--poll-interval`. A successful reply produces a heartbeat event; an RPC error or deadline produces no event (the detector accrues suspicion on its own).

### Symmetric input

Both paths produce `(worker_id, arrival_time)` events. The Detector has no idea which mode produced them. This keeps the algorithm comparable across modes.

## 5. Failure Detection Algorithms

Both detectors implement the same Go interface:

```go
type Detector interface {
    // Called when heartbeat arrives (push) or ping reply succeeds (pull)
    Heartbeat(workerID string, arrival time.Time)

    // Called periodically by Monitor's evaluation loop.
    // suspicion is a unitless monotone "how dead does this look" number used
    // by the TUI/log; the State field is what drives transitions.
    //   - PhiAccrual  : suspicion = phi (canonical)
    //   - FixedWindow : suspicion = elapsed / T_dead (0..1+ ; ≥1 means DEAD)
    Suspicion(workerID string, now time.Time) (suspicion float64, state State)
}

type State int
const ( Alive State = iota; Missing; Dead )
```

### 5.1 Fixed Window Detector

```
T_missing = HB_INTERVAL * k_miss      // e.g., 1s * 3 = 3s
T_dead    = HB_INTERVAL * k_dead      // e.g., 1s * 10 = 10s

elapsed = now - last_heartbeat
if elapsed < T_missing : ALIVE
if elapsed < T_dead    : MISSING
else                   : DEAD
```

Multipliers `k_miss` and `k_dead` are exposed via `--miss-multiplier` and `--dead-multiplier`. Simple, deterministic, but produces false positives on jittery networks.

### 5.2 Phi Accrual Detector

Reference: Hayashibara et al., *The Φ Accrual Failure Detector* (2004). Used by Cassandra and Akka.

Maintain a sliding window of the last `N` inter-arrival intervals (default `N = 1000`). Compute the running mean μ and standard deviation σ.

Phi formula (assumes inter-arrivals are approximately Normal):

```
elapsed       = now - last_heartbeat
P_later(t)    = 1 - F(t)               where F is the CDF of Normal(μ, σ²)
phi(t)        = -log10( P_later(t) )
```

The production approximation used by Cassandra avoids `erfc`:

```
y       = (elapsed - μ) / σ
P_later ≈ exp( -y * (1.5976 + 0.070566 * y * y) )    if elapsed > μ
        ≈ 1                                            otherwise
phi     = -log10(P_later)
```

Thresholds:

```
phi < Φ_missing   (default 1.0)   → ALIVE
phi < Φ_dead      (default 8.0)   → MISSING
phi ≥ Φ_dead                      → DEAD
```

Φ = 1 means "roughly 10 % chance still alive". Φ = 8 means "roughly 10⁻⁸ chance still alive". Cassandra's default `phi_threshold` is 8.

**Bootstrap problem.** The sliding window is empty until the first ~10 heartbeats arrive. While the window has fewer than `--phi-min-samples` observations, the detector falls back to the Fixed Window with `k_miss = 3`, `k_dead = 10` (the same defaults as `--miss-multiplier` / `--dead-multiplier`).

```go
type PhiAccrual struct {
    window        *ring.Ring   // last N intervals (float64 seconds)
    n, minSamples int
    lastArrival   time.Time
    phiMissing    float64      // 1.0
    phiDead       float64      // 8.0
    fallback      *FixedWindow // used until window has min_samples
}
```

### 5.3 Proposed formula (the paper deliverable)

> A worker is declared **DEAD** when `phi(now − last_heartbeat) ≥ Φ_dead`, where `phi` is computed from the empirical distribution of inter-arrival times over a sliding window of the last N samples, with bootstrap fallback to fixed-window thresholding while the window contains fewer than `min_samples` observations.

Defaults: `N = 1000`, `min_samples = 10`, `Φ_missing = 1.0`, `Φ_dead = 8.0`.

## 6. Module Layout

```
cmpe273-dead-mans-switch/
├── go.mod
├── proto/
│   └── heartbeat.proto              # gRPC service def
├── gen/                             # protoc output (committed)
│   └── deadman/v1/*.pb.go
├── cmd/
│   ├── monitor/main.go              # binary entry
│   └── worker/main.go               # binary entry
├── internal/
│   ├── detector/
│   │   ├── detector.go              # Detector interface, State enum
│   │   ├── fixed.go                 # FixedWindow impl
│   │   ├── phi.go                   # PhiAccrual impl
│   │   └── *_test.go                # unit tests w/ synthetic arrival streams
│   ├── monitor/
│   │   ├── server.go                # gRPC server (push receiver)
│   │   ├── poller.go                # gRPC client per worker (pull driver)
│   │   ├── registry.go              # worker registry, state machine
│   │   ├── evaluator.go             # ticker that calls Detector.Suspicion
│   │   └── tui.go                   # bubbletea TUI
│   ├── worker/
│   │   ├── pusher.go                # streams heartbeats
│   │   ├── responder.go             # gRPC server for Ping
│   │   └── chaos.go                 # crash/lag/drop injection
│   └── log/
│       └── jsonlog.go               # structured event log → file (for plots)
├── scripts/
│   ├── run_demo.sh                  # spin up monitor + 5 workers locally
│   ├── bench_push_pull.sh           # benchmark for paper
│   └── plot.py                      # JSON log → matplotlib charts
├── docs/
│   └── superpowers/specs/2026-04-30-dead-mans-switch-design.md
└── paper/
    └── dead-mans-switch.md          # research paper deliverable
```

## 7. Data Flow

### Push mode

```
Worker.pusher  ──[gRPC stream Heartbeat]──►  Monitor.server
                                                  │
                                       arrival_time = now()
                                                  ▼
                                            registry.OnHeartbeat(id, arrival)
                                                  │
                                                  ▼
                                            detector.Heartbeat(id, arrival)
                                                  │
                                                  ▼
                                            jsonlog.Event("hb", id, arrival)

[evaluator goroutine, every --eval-interval]:
   for each worker w in registry:
       phi, state = detector.Suspicion(w.id, now)
       if state changed:
           registry.Transition(w.id, state)
           jsonlog.Event("state", id, state, phi)
           tui.Update(w.id, state, phi)
```

### Pull mode

```
Monitor.poller [goroutine per worker, every --poll-interval]:
   reply, err = client.Ping(ctx_with_timeout, ...)
   if err == nil:
       arrival = now()
       registry.OnHeartbeat(id, arrival)
       detector.Heartbeat(id, arrival)
   else:
       // RPC error or deadline → no event fed to detector
       jsonlog.Event("ping_fail", id, err)

[evaluator goroutine]: same as Push mode
```

The detector treats both modes identically. Push fails closed (worker stops sending = no event = phi rises). Pull fails closed (RPC error = no event = phi rises).

## 8. Configuration

### Monitor flags

```
--listen=:50051                 gRPC bind addr
--mode=push|pull|hybrid         heartbeat direction
--detector=phi|fixed            algorithm
--hb-interval=1s                expected heartbeat period
--miss-multiplier=3             fixed: T_missing = hb-interval * 3
--dead-multiplier=10            fixed: T_dead = hb-interval * 10
--phi-missing=1.0               phi threshold for MISSING
--phi-dead=8.0                  phi threshold for DEAD
--phi-window=1000               sliding window size
--phi-min-samples=10            bootstrap threshold
--poll-interval=1s              pull mode: how often Monitor pings
--poll-timeout=500ms            pull mode: per-RPC deadline
--eval-interval=200ms           how often state machine re-evaluated
--log-file=events.jsonl         structured log
```

### Worker flags

```
--id=worker-1                   logical name
--monitor=localhost:50051       Monitor addr (push mode)
--listen=:50061                 gRPC bind (pull mode)
--hb-interval=1s                push cadence
--chaos-crash-after=0           exit cleanly after N seconds (0 = never)
--chaos-kill-after=0            exit(1) immediately after N seconds (0 = never)
--chaos-lag-mean=0ms            inject latency before sending heartbeat
--chaos-lag-stddev=0ms          jitter
--chaos-drop-rate=0.0           probability of skipping a heartbeat
--chaos-pause-window=""         pause heartbeats for window (e.g., "60s:5s")
```

## 9. Error Handling

| Scenario | Push mode | Pull mode |
|----------|-----------|-----------|
| Worker crash | stream `Recv()` returns error → registry retains last_arrival, evaluator declares DEAD via detector | Ping returns connection-refused → no detector input → phi rises |
| Network blip | stream may reconnect (worker retries with exponential backoff, capped at 5s); detector sees a gap then resumes | Ping fails N times then succeeds; detector sees the same gap |
| Monitor crash | workers retry connect on backoff. No persistence; registry is in-memory. | workers idle until Monitor returns. |
| Slow heartbeat | Phi tolerates jitter; Fixed flags MISSING earlier. | Same. |
| Clock skew | Monitor uses *its own* arrival timestamp. Worker `sent_unix_nanos` is logged for paper analysis only, never used for detection. |
| Duplicate worker_id | Monitor rejects second `Register` with the same id (`RegisterReply.accepted = false`). |

No persistence. Monitor restart starts with an empty registry. Acceptable for the assignment.

## 10. Testing

### Unit tests (`go test ./internal/detector/...`)

- FixedWindow: synthetic arrivals at fixed cadence; assert state transitions at expected boundaries.
- PhiAccrual:
  - Steady arrivals → phi stays low.
  - Sudden gap → phi rises through Φ_missing then Φ_dead.
  - Bootstrap: window with fewer than `min_samples` falls back to fixed.
  - Jittery arrivals (Normal noise around μ) → no false positives at Φ = 8.

### Integration tests (`scripts/run_demo.sh`)

- 1 Monitor + 5 Workers, push mode, no chaos → all stay ALIVE for 60 s.
- Same setup, kill `worker-3` at t = 20 s → Monitor declares DEAD within ~10 s.
- Pull-mode equivalents.

### Benchmarks (`scripts/bench_push_pull.sh`)

For N = 10, 100, 1000 workers, both modes, measure:

- Monitor CPU and resident memory.
- Network bytes/second (gRPC stats handler).
- Detection latency: time from `kill -9` on a worker to the DEAD log event.

Output CSV → `scripts/plot.py` → PNG charts in the paper.

## 11. Paper Structure

```
1. Introduction          — problem statement, dead-man's switch use cases
2. Heartbeat Strategy
   2.1 Push model        — analysis: bandwidth O(N), connections O(N), firewall-friendly
   2.2 Pull model        — analysis: bandwidth O(N), connections O(N), needs reachable workers
   2.3 1000-worker case  — empirical numbers from benchmark
   2.4 Firewall case     — argument: push wins (worker initiates connection)
3. Timeout Dilemma
   3.1 Fixed Window      — formula, weakness on jitter
   3.2 Phi Accrual       — derivation, formula, parameters
   3.3 Proposed Logic    — chosen formula (phi ≥ Φ_dead with bootstrap)
   3.4 Empirical comparison — false-positive rate under injected jitter
4. Implementation Notes  — architecture, gRPC, code links
5. Results               — charts: detection latency, false-positive rate, bandwidth
6. Conclusion            — recommendation: push + Phi Accrual
References
```

## 12. TUI

The Monitor renders a live table to stdout via `bubbletea`, refreshed every `--eval-interval`:

```
┌─ Dead Man's Switch — Monitor ─────────────────────────────────────────────┐
│ Mode: push   Detector: phi   Workers: 5   Uptime: 00:02:14                │
├──────────────┬─────────┬──────────┬─────────┬─────────┬───────────────────┤
│ Worker       │ State   │ Last HB  │ Phi     │ μ (ms)  │ σ (ms)            │
├──────────────┼─────────┼──────────┼─────────┼─────────┼───────────────────┤
│ worker-1     │ ALIVE   │  0.4s    │  0.02   │ 1003.2  │  18.7             │
│ worker-2     │ ALIVE   │  0.7s    │  0.05   │ 1008.1  │  22.4             │
│ worker-3     │ MISSING │  4.2s    │  2.31   │ 1001.0  │  19.0             │
│ worker-4     │ DEAD    │ 18.9s    │  ∞      │ 1004.0  │  20.1             │
│ worker-5     │ ALIVE   │  0.2s    │  0.01   │ 1000.5  │  17.3             │
└──────────────┴─────────┴──────────┴─────────┴─────────┴───────────────────┘
[q] quit   [p] toggle push/pull   [d] toggle detector   [l] dump events.jsonl
```

Color: ALIVE = green, MISSING = yellow, DEAD = red.

## 13. Demo Script

```bash
#!/bin/bash
# Spawn Monitor + 5 Workers.
# worker-3 dies at t=20s (Phi declares DEAD within ~5–10s).
# worker-4 has injected jitter; Phi tolerates, Fixed (k_dead=3) would falsely flag MISSING.

./monitor --mode=push --detector=phi --log-file=demo.jsonl &
sleep 1
./worker --id=worker-1 --monitor=localhost:50051 &
./worker --id=worker-2 --monitor=localhost:50051 &
./worker --id=worker-3 --monitor=localhost:50051 --chaos-kill-after=20s &
./worker --id=worker-4 --monitor=localhost:50051 --chaos-lag-mean=200ms --chaos-lag-stddev=80ms &
./worker --id=worker-5 --monitor=localhost:50051 &
wait
```

## 14. Acceptance Criteria

- `go build ./...` succeeds; both binaries are produced.
- `go test ./...` passes.
- Demo script runs end-to-end on macOS and Linux.
- Phi detector never declares the jittery-but-alive worker DEAD over a 5-minute run.
- Fixed detector with `k_dead = 3` flags the same jittery worker DEAD (false positive captured for the paper).
- Benchmark produces CSV for N = 10, 100, 1000 workers in both modes.
- Paper file (`paper/dead-mans-switch.md`) contains all six sections plus charts generated from the structured log.
