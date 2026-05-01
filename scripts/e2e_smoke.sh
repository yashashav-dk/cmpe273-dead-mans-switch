#!/usr/bin/env bash
# Spawn one Monitor + one Worker. Kill the Worker and assert that within 12s
# the Monitor's event log contains a state transition to DEAD.
#
# Exit codes:
#   0   smoke test passed
#   1   monitor failed to start
#   2   no DEAD transition observed within deadline
set -euo pipefail

cd "$(dirname "$0")/.."
make build >/dev/null

LOG="/tmp/dms-smoke.jsonl"
rm -f "$LOG"

./bin/monitor --listen=:51051 --mode=push --detector=fixed \
    --hb-interval=500ms --miss-multiplier=2 --dead-multiplier=4 \
    --eval-interval=100ms --tui=false --log-file="$LOG" &
MON=$!
trap 'kill $MON 2>/dev/null || true' EXIT

sleep 0.5
if ! kill -0 $MON 2>/dev/null; then
  echo "monitor failed to start"; exit 1
fi

./bin/worker --id=smoke-w --monitor=127.0.0.1:51051 --listen=:51061 \
    --hb-interval=500ms &
WRK=$!

# Let some heartbeats land.
sleep 1

# Kill the worker.
kill -9 $WRK || true

# Wait up to 12s for a DEAD transition in the log.
deadline=$(( $(date +%s) + 12 ))
while [ "$(date +%s)" -lt $deadline ]; do
  if grep -q '"to":"DEAD"' "$LOG"; then
    echo "smoke OK: DEAD transition observed"
    exit 0
  fi
  sleep 0.2
done

echo "no DEAD transition in $LOG"
cat "$LOG"
exit 2
