# Distributed Dead Man's Switch (CMPE 273)

Heartbeat-based failure detection for distributed systems. Single Monitor process observes N Worker processes over gRPC. Compares **push vs pull** heartbeat transports and **Fixed Window vs Phi Accrual** failure detectors empirically.

📄 **Paper:** [`paper/dead-mans-switch.md`](paper/dead-mans-switch.md) (renders inline on GitHub) · [PDF download](paper/dead-mans-switch.pdf)

**Deliverable:** Go implementation + research paper at `paper/dead-mans-switch.md`. The paper cites empirical numbers measured by `bench.csv` and `phi_sweep.csv` in this repo.

---

## Prerequisites

- Go 1.22+ (developed on Go 1.25)
- `make`
- Python 3 with `matplotlib` (for chart regeneration only — generated PNGs are committed in `paper/figures/`)
- macOS or Linux. Tested on macOS 14 (Apple Silicon)

`protoc` and the protoc plugins are NOT required — generated gRPC code is committed under `gen/deadman/v1/`.

---

## Quick start

```bash
make build         # → bin/monitor, bin/worker
make test          # all unit + integration tests, ~5s
bash scripts/e2e_smoke.sh    # 1 monitor + 1 worker, kill, expect DEAD; ~13s
```

Expected smoke output:
```
2026/05/01 ... monitor listening on :51051 mode=push detector=fixed
2026/05/01 ... worker smoke-w responder on :51061
smoke OK: DEAD transition observed
```

---

## Reproducing the paper's experiments

### 1. Demo (Phi vs Fixed head-to-head, ~30s)

```bash
bash scripts/run_demo.sh         # Ctrl-C after ~30s
grep '"to":"DEAD"' demo-phi.jsonl    | head
grep '"to":"DEAD"' demo-fixed.jsonl  | head
```

Two Monitors run side-by-side: one with Phi (`Φ_dead=8`, default), one with Fixed Window (`k_dead=3`, aggressive). Five workers; `worker-3` self-terminates at +20s; `worker-4` injects 2.5 s ± 1 s lag.

Acceptance:
- Both monitors declare `worker-3` DEAD within ~10 s of kill ✓
- Fixed declares `worker-4` DEAD spuriously (3 false positives observed) ✓
- Phi never declares `worker-4` DEAD ✓

### 2. Bench sweep (push/pull × phi/fixed × N ∈ {10, 100, 1000}, ~10 min)

```bash
bash scripts/bench_push_pull.sh
cat bench.csv
```

Output: `bench.csv` with columns `N,mode,detector,peak_rss_kb,cpu_secs,detection_latency_ms`.

Sample (committed): see [`bench.csv`](bench.csv). Headline at N=1000:
- push/Phi = 1186 ms detection vs Fixed = 9910 ms
- pull/Phi = 642 ms detection vs Fixed = 9833 ms
- pull RSS 200 MB vs push RSS 78 MB

### 3. Φ_dead threshold sweep (~8 min)

```bash
bash scripts/phi_sweep.sh
cat phi_sweep.csv
```

Sweeps Φ_dead ∈ {3, 5, 8, 12} under both lag and 50%-drop scenarios; counts false-positive DEADs on a jittery-but-alive worker.

| Φ_dead | scenario | false-positive DEADs |
|--------|----------|---------------------:|
| 3      | lag      | 0 |
| 5      | lag      | 0 |
| 8      | lag      | 0 |
| 12     | lag      | 0 |
| 3      | drop     | 1 |
| 5      | drop     | 0 |
| 8      | drop     | 0 |
| 12     | drop     | 0 |

Justifies the `Φ_dead=8` default empirically. See `paper/dead-mans-switch.md` §3.4.1.

### 4. Regenerate figures

```bash
python3 scripts/plot.py --csv bench.csv --log demo-phi.jsonl --phi-csv phi_sweep.csv --outdir paper/figures
ls paper/figures/
```

Produces `detection_latency.png`, `rss.png`, `state_transitions.png`, `phi_sweep.png`. PNG files are committed; `plot.py` is only needed if regenerating.

---

## TUI

Live dashboard (default; pass `--tui=false` for headless benchmarks):

```bash
./bin/monitor --listen=:50051 &
./bin/worker --id=w1 --monitor=127.0.0.1:50051 --listen=:50061 &
./bin/worker --id=w2 --monitor=127.0.0.1:50051 --listen=:50062 &
```

Sample frame (rendered via `go run ./cmd/tuisnap`; ANSI colors stripped):

```
Dead Man's Switch — Monitor
Mode: push   Detector: phi   Workers: 5   Uptime: 2m14s

Worker         State     Last HB    Suspicion
worker-1       ALIVE     0.4s       0.02
worker-2       ALIVE     0.7s       0.05
worker-3       MISSING   4.2s       2.31
worker-4       DEAD      18.9s      1e9
worker-5       ALIVE     0.2s       0.01

[q] quit
```

Live TUI uses ANSI: ALIVE=green, MISSING=yellow, DEAD=red.

---

## Repository layout

```
.
├── proto/deadman/v1/heartbeat.proto    # gRPC service definition
├── gen/deadman/v1/                     # generated Go (committed)
├── cmd/
│   ├── monitor/main.go                 # monitor binary entry
│   ├── worker/main.go                  # worker binary entry
│   └── tuisnap/main.go                 # one-frame TUI snapshot tool
├── internal/
│   ├── detector/                       # Detector interface + Fixed + Phi
│   │   ├── detector.go fixed.go phi.go
│   │   └── *_test.go
│   ├── monitor/                        # gRPC server, registry, evaluator, poller, TUI
│   ├── worker/                         # pusher, responder, chaos
│   └── eventlog/                       # JSONL event log
├── scripts/
│   ├── e2e_smoke.sh                    # 1-min smoke test
│   ├── run_demo.sh                     # spec §13 head-to-head demo
│   ├── bench_push_pull.sh              # full N sweep → bench.csv
│   ├── phi_sweep.sh                    # Φ_dead threshold sweep → phi_sweep.csv
│   └── plot.py                         # CSVs + JSONL → PNG charts
├── paper/
│   ├── dead-mans-switch.md             # research paper
│   └── figures/*.png                   # charts cited by paper
├── docs/superpowers/
│   ├── specs/2026-04-30-dead-mans-switch-design.md
│   └── plans/2026-04-30-dead-mans-switch.md
├── bench.csv                           # full N sweep results (committed)
├── phi_sweep.csv                       # Φ_dead sweep results (committed)
├── paper/data/sample-demo-{phi,fixed}.jsonl  # one captured demo run, for the grader
└── README.md
```

---

## What to read first (for the grader)

1. `paper/dead-mans-switch.md` — the deliverable.
2. `internal/detector/phi.go` — Phi Accrual implementation (Akka Normal-CDF approximation, sliding window with incremental sum / sum-of-squares, Fixed Window bootstrap fallback).
3. `internal/detector/phi_test.go` — unit tests covering steady, jittered, big-gap, late-arrival, monotonicity, bootstrap.
4. `bench.csv` and `phi_sweep.csv` — the empirical data backing the paper's claims.
5. `paper/figures/*.png` — figures cited by the paper.

---

## Design

`docs/superpowers/specs/2026-04-30-dead-mans-switch-design.md` — full design spec covering scope, gRPC protocol, state machine, detector algorithms, configuration, error handling, testing, and acceptance criteria.

`docs/superpowers/plans/2026-04-30-dead-mans-switch.md` — 22-task implementation plan used to build this repository.

---

## Verification at a glance

| Check | Command | Expected |
|-------|---------|----------|
| Builds | `make build` | `bin/monitor` and `bin/worker` produced |
| Unit + integration tests pass | `make test` | all packages OK |
| Race detector clean | `go test ./... -race` | no DATA RACE |
| End-to-end smoke | `bash scripts/e2e_smoke.sh` | `smoke OK: DEAD transition observed` |
| Demo Phi vs Fixed | `bash scripts/run_demo.sh` (~30 s) | `worker-3` DEAD in both logs; `worker-4` DEAD only in `demo-fixed.jsonl` |
| Bench sweep | `bash scripts/bench_push_pull.sh` | `bench.csv` 13 rows, all `detection_latency_ms` non-NA |
| Φ-sweep | `bash scripts/phi_sweep.sh` | `phi_sweep.csv` 9 rows, FPRs match the paper §3.4.1 table |
