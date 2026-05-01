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
- `hybrid` — Monitor accepts push streams **and** polls every registered worker concurrently. Both transports record bytes-on-the-wire metrics, but **only the push stream feeds the Detector** (pull replies are recorded for bandwidth measurement and dropped before reaching the detector). This avoids doubling the effective heartbeat rate, which would halve μ and break Phi. Hybrid is a benchmarking convenience for producing the paper's bandwidth comparison in a single run; it is not the recommended production setting.

Workers always run both endpoints. The Monitor decides which side initiates.

### Worker state machine

```
ALIVE   ────(detector.Suspicion ≥ Φ_missing at next eval)────►  MISSING
MISSING ────(detector.Suspicion ≥ Φ_dead    at next eval)────►  DEAD
MISSING ────(detector.Suspicion <  Φ_missing at next eval)────►  ALIVE
DEAD    is terminal for the run. (Re-integration is out of scope.)
```

**Transitions are driven only by `Suspicion()` polling at `--eval-interval`.** The `Heartbeat()` callback updates internal detector state (resets `last_arrival`, appends to the Phi sliding window) but never directly transitions the registry. Because both detectors compute suspicion from `elapsed = now - last_arrival`, an arriving heartbeat causes suspicion to drop to ~0 on the next eval, recovering MISSING → ALIVE naturally. There is no hysteresis: rapid flapping is possible if jitter sits exactly on Φ_missing, and the paper notes this as a known limitation.

**Every transition is logged**, including MISSING → ALIVE recoveries. This is what lets the paper plot false-alarm rates per detector configuration.

`MISSING` means "haven't heard recently, but not yet declared dead." `DEAD` triggers a state-change log event and a red row in the TUI.

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

  // Worker explicit register/deregister. Required in pull mode so the
  // Monitor knows who to poll. Also called in push mode so the Monitor's
  // registry is populated before the first heartbeat arrives — useful for
  // the TUI and for rejecting duplicate worker_ids early.
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
message RegisterReply    { bool   accepted  = 1; string reason = 2; }
```

`accepted = false` with `reason = "duplicate_worker_id"` is the only rejection case in scope. There is no lease-revocation protocol in this design.

**Codegen.** A `Makefile` target `make proto` runs `buf generate` (or `protoc`) and writes Go to `gen/deadman/v1/`. The generated files are committed so the project builds with plain `go build` and so a grader does not need `protoc` installed. `go.mod` pins `google.golang.org/grpc` and `google.golang.org/protobuf` to specific versions to keep the generated code consistent with the runtime.

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

Reference: Hayashibara et al., *The Φ Accrual Failure Detector* (2004). Both Akka and Cassandra ship implementations, but they use **different** distributional assumptions and therefore different closed-form approximations:

- **Akka** (`akka.remote.PhiAccrualFailureDetector`) — assumes inter-arrival times are approximately **Normal** and uses a logistic polynomial approximation to the Normal CDF.
- **Cassandra** (`org.apache.cassandra.gms.FailureDetector`) — assumes inter-arrival times are approximately **Exponential** and uses `phi = -log10( e / (1 + e) )` where `e = exp(-elapsed / μ)`.

This design implements the **Akka (Normal) variant** because it is the more common pedagogical reference and the one whose math the assignment most directly evokes. The paper notes the Cassandra variant and the practical consequence: the same Φ_dead threshold means different things under the two models.

Maintain a sliding window of the last `N` inter-arrival intervals (default `N = 1000`). Compute the running mean μ and standard deviation σ incrementally.

Phi formula (Akka / Normal model):

```
elapsed       = now - last_heartbeat
P_later(t)    = 1 - F(t)               where F is the CDF of Normal(μ, σ²)
phi(t)        = -log10( P_later(t) )
```

Akka's production approximation avoids `erfc`:

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

Φ = 1 means "roughly 10 % chance still alive." Φ = 8 means "roughly 10⁻⁸ chance still alive." Cassandra's default `phi_convict_threshold` is 8 under their **Exponential** model — that value is more conservative than 8 under the Normal model used here. The paper retains 8 as the default to match Cassandra's documented behavior, but the empirical-comparison section reports false-positive rate at Φ ∈ {3, 5, 8, 12} so the reader can see how the choice interacts with the distributional assumption.

**Bootstrap problem.** The sliding window is empty until the first ~10 heartbeats arrive. While the window has fewer than `--phi-min-samples` observations, the detector falls back to the Fixed Window with `k_miss = 3`, `k_dead = 10` (the same defaults as `--miss-multiplier` / `--dead-multiplier`). In **pull mode** the first inter-arrival sample requires *two* successful pings; the bootstrap window therefore covers the first `phi-min-samples + 1` poll cycles.

**Late-arrival policy.** When a heartbeat finally arrives after a long gap (e.g., chaos-injected lag of several seconds), the inter-arrival sample *is* added to the sliding window unconditionally. This matches Akka's behavior. The consequence is that a single large lag spike inflates μ and σ, which makes the detector temporarily more tolerant of subsequent gaps — this is intentional: it lets Phi adapt to changing network conditions, and is exactly the behavior the paper compares against Fixed Window.

**Window data structure.** Use a fixed-size `[]float64` of length `N` with a write index, and maintain running `sum` and `sum_of_squares` incrementally (subtract the evicted sample, add the new one). `container/ring` is **not** appropriate — it would cost an O(N) traversal on every `Suspicion()` call.

```go
type PhiAccrual struct {
    window        []float64    // length N, ring buffer with writeIdx
    writeIdx, n   int
    sum, sumSq    float64      // updated incrementally
    minSamples    int
    lastArrival   time.Time    // monotonic — see §5.4
    phiMissing    float64      // 1.0
    phiDead       float64      // 8.0
    fallback      *FixedWindow // used until window has min_samples
}
```

### 5.4 Clock discipline

Both detectors compute `elapsed = now - last_arrival`. The implementation **must** use Go's monotonic clock: store `time.Now()` directly in `lastArrival` and compute elapsed via `time.Since(lastArrival)` or `now.Sub(lastArrival)` where both timestamps were produced by `time.Now()`. **Do not** reconstruct arrival times from the structured log's Unix-nanos field — that strips the monotonic reading and an NTP step during a benchmark would corrupt the Phi window.

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
│   └── eventlog/
│       └── eventlog.go              # structured event log → file (for plots)
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

### 7.1 Structured log schema

Every line in `--log-file` is one JSON object. Schema:

```json
{ "ts": "2026-04-30T14:30:01.234567Z", "type": "hb",        "worker": "worker-3", "seq": 42, "transport": "push|pull" }
{ "ts": "...",                          "type": "ping_fail", "worker": "worker-3", "error": "DeadlineExceeded" }
{ "ts": "...",                          "type": "state",     "worker": "worker-3", "from": "ALIVE", "to": "MISSING", "suspicion": 1.42, "detector": "phi" }
{ "ts": "...",                          "type": "stream_close","worker": "worker-3", "error": "EOF" }
{ "ts": "...",                          "type": "register",  "worker": "worker-3", "addr": "localhost:50061", "accepted": true }
```

`ts` is the Monitor's wall-clock time in RFC 3339 with nanosecond precision (used for charts only; detection still uses monotonic clock per §5.4). `plot.py` consumes this format.

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
--tui=true                      live TUI dashboard; pass --tui=false during benchmarks
```

### Worker flags

```
--id=worker-1                          logical name
--monitor=localhost:50051              primary Monitor addr (push target; also used for Register so the
                                       worker is known to the Monitor regardless of mode)
--pull-monitors=                       optional, comma-separated additional Monitor addrs that should
                                       receive a Register and then poll this worker (used by demo to
                                       run a parallel pull-mode Monitor against the same worker)
--listen=:50061                        gRPC bind for Ping responder (pull mode)
--hb-interval=1s                       push cadence
--chaos-crash-after=0                  exit cleanly after N seconds (0 = never)
--chaos-kill-after=0                   exit(1) immediately after N seconds (0 = never)
--chaos-lag-mean=0ms                   inject latency before sending heartbeat (or before responding to Ping)
--chaos-lag-stddev=0ms                 jitter (Normal noise around lag-mean)
--chaos-drop-rate=0.0                  probability of skipping a heartbeat / dropping a Ping reply
```

## 9. Error Handling

| Scenario | Push mode | Pull mode |
|----------|-----------|-----------|
| Worker crash | stream `Recv()` returns error → registry retains last_arrival, evaluator declares DEAD via detector. **The stream-close event is logged but is intentionally not fed to the Detector** — this keeps the detector mode-agnostic, since pull mode has no equivalent signal. The paper notes this as a deliberate trade-off (faster crash detection in push mode is achievable but would require a transport-aware detector). | Ping returns connection-refused → no detector input → phi rises |
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
┌─ Dead Man's Switch — Monitor ───────────────────┐
│ Mode: push   Detector: phi   Workers: 5         │
│ Uptime: 00:02:14                                │
├──────────────┬─────────┬──────────┬─────────────┤
│ Worker       │ State   │ Last HB  │ Suspicion   │
├──────────────┼─────────┼──────────┼─────────────┤
│ worker-1     │ ALIVE   │  0.4s    │  0.02       │
│ worker-2     │ ALIVE   │  0.7s    │  0.05       │
│ worker-3     │ MISSING │  4.2s    │  2.31       │
│ worker-4     │ DEAD    │ 18.9s    │  1e9        │
│ worker-5     │ ALIVE   │  0.2s    │  0.01       │
└──────────────┴─────────┴──────────┴─────────────┘
[q] quit
```

The TUI shows a single `Suspicion` column. The underlying μ and σ from the
Phi sliding window are exposed via `(*PhiAccrual).stats(workerID)` for tests
but are intentionally not surfaced in the TUI to keep rows scannable. Inf is
clamped to `1e9` everywhere downstream of the detector (see §7.1) so the log
and TUI never see a literal `∞`.

Color: ALIVE = green, MISSING = yellow, DEAD = red.

## 13. Demo Script

The demo runs **two Monitors side-by-side** against the same set of Workers, so the contrast between Phi and Fixed is visible in real time. Each Monitor binds a different gRPC port and writes a separate log file. Workers connect only to the Phi monitor; the Fixed monitor runs in pull mode against the workers' `--listen` ports.

```bash
#!/bin/bash
# Spawn two Monitors + 5 Workers.
#   monitor-phi   : push, Phi detector, default thresholds
#   monitor-fixed : pull, Fixed detector with aggressive k_dead=3
# worker-3 dies at t=20s   → both monitors should declare DEAD
# worker-4 has heavy jitter → Fixed (k_dead=3) declares DEAD falsely; Phi does not.

./monitor --listen=:50051 --mode=push --detector=phi --tui=false \
          --log-file=demo-phi.jsonl &

./monitor --listen=:50052 --mode=pull --detector=fixed \
          --miss-multiplier=2 --dead-multiplier=3 \
          --tui=false --log-file=demo-fixed.jsonl &
sleep 1

# Workers register with both monitors. --monitor= is the push target;
# --pull-monitors= is a comma-list of monitors that will Register & poll us.
# Each worker binds an explicit --listen=:5006N (no shared base flag).
COMMON="--monitor=localhost:50051 --pull-monitors=localhost:50052"
./worker --id=worker-1 $COMMON --listen=:50061 &
./worker --id=worker-2 $COMMON --listen=:50062 &
./worker --id=worker-3 $COMMON --listen=:50063 --chaos-kill-after=20s &
./worker --id=worker-4 $COMMON --listen=:50064 --chaos-lag-mean=2500ms --chaos-lag-stddev=1000ms &
./worker --id=worker-5 $COMMON --listen=:50065 &
wait
```

## 14. Acceptance Criteria

- `go build ./...` succeeds; both binaries are produced.
- `go test ./...` passes.
- Demo script runs end-to-end on macOS and Linux and produces two log files.
- Phi monitor (Φ_dead = 8) does **not** declare `worker-4` DEAD over a 5-minute run despite injected lag of mean 2.5 s ± 1 s against a 1 s heartbeat interval.
- Fixed monitor (`k_dead = 3`, i.e., `T_dead = 3 s`) declares `worker-4` DEAD at least once in the same run — the captured false positive that the paper compares against Phi.
- Both monitors declare `worker-3` DEAD within 10 s of `--chaos-kill-after` firing at t = 20 s.
- Benchmark produces CSV for N = 10, 100, 1000 workers in both push and pull modes, with `--tui=false`.
- Paper file (`paper/dead-mans-switch.md`) contains all six sections plus charts generated from the structured log.
