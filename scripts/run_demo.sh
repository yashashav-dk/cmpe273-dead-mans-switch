#!/usr/bin/env bash
# Demo per design spec §13: two side-by-side Monitors observing the same
# 5 workers. The Phi monitor (push) tolerates jitter; the Fixed monitor
# (pull, k_dead=3) does not — and worker-3 dies hard at t=20s so both
# monitors should agree it's DEAD.
set -euo pipefail

cd "$(dirname "$0")/.."
make build >/dev/null

rm -f demo-phi.jsonl demo-fixed.jsonl

./bin/monitor --listen=:50051 --mode=push --detector=phi \
    --tui=false --log-file=demo-phi.jsonl &
MON_PHI=$!

./bin/monitor --listen=:50052 --mode=pull --detector=fixed \
    --miss-multiplier=2 --dead-multiplier=3 \
    --tui=false --log-file=demo-fixed.jsonl &
MON_FIXED=$!

trap 'kill $MON_PHI $MON_FIXED 2>/dev/null || true; pkill -P $$ || true' EXIT
sleep 1

COMMON_FLAGS=(
  --monitor=127.0.0.1:50051
  --pull-monitors=127.0.0.1:50052
)

./bin/worker --id=worker-1 --listen=:50061 "${COMMON_FLAGS[@]}" &
./bin/worker --id=worker-2 --listen=:50062 "${COMMON_FLAGS[@]}" &
./bin/worker --id=worker-3 --listen=:50063 "${COMMON_FLAGS[@]}" --chaos-kill-after=20s &
./bin/worker --id=worker-4 --listen=:50064 "${COMMON_FLAGS[@]}" \
    --chaos-lag-mean=2500ms --chaos-lag-stddev=1000ms &
./bin/worker --id=worker-5 --listen=:50065 "${COMMON_FLAGS[@]}" &

echo "demo running. Monitor logs: demo-phi.jsonl, demo-fixed.jsonl"
echo "Ctrl-C to stop."
wait
