#!/usr/bin/env bash
# Phi-threshold sweep: for each Φ_dead in {3,5,8,12}, run a 60-second scenario
# with one calm worker and one jittery worker (lag mean=2.5s, stddev=1s
# against hb-interval=1s) and count false-positive DEAD transitions on the
# jittery-but-alive worker. Output: phi_sweep.csv.
#
# Used to materialize the false-positive-rate-vs-Φ table promised in paper §3.2.
set -uo pipefail

cd "$(dirname "$0")/.."
make build >/dev/null

OUT="phi_sweep.csv"
echo "phi_dead,scenario,worker4_false_dead_count,worker4_to_missing_count" > "$OUT"

run_one() {
  local PHI=$1 SCENARIO=$2 LAG_MEAN=$3 LAG_STD=$4 DROP=$5
  echo "=== Φ_dead=$PHI scenario=$SCENARIO ==="
  local LOG="/tmp/phi-sweep-$PHI-$SCENARIO.jsonl"
  rm -f "$LOG"

  ./bin/monitor --listen=:55000 --mode=push --detector=phi \
      --phi-dead="$PHI" --hb-interval=1s --eval-interval=200ms \
      --tui=false --log-file="$LOG" &
  local MON=$!
  sleep 0.5

  ./bin/worker --id=calm --monitor=127.0.0.1:55000 --listen=:55011 \
      --hb-interval=1s >/dev/null 2>&1 &
  local W1=$!
  ./bin/worker --id=jitter --monitor=127.0.0.1:55000 --listen=:55012 \
      --hb-interval=1s --chaos-lag-mean="$LAG_MEAN" --chaos-lag-stddev="$LAG_STD" \
      --chaos-drop-rate="$DROP" >/dev/null 2>&1 &
  local W2=$!

  sleep 60

  local dead miss
  dead=$(grep '"worker":"jitter"' "$LOG" | grep -c '"to":"DEAD"' || true)
  miss=$(grep '"worker":"jitter"' "$LOG" | grep -c '"to":"MISSING"' || true)
  echo "$PHI,$SCENARIO,$dead,$miss" >> "$OUT"

  kill -9 "$MON" "$W1" "$W2" 2>/dev/null || true
  sleep 1
}

# Two scenarios so the reader can see Phi's behavior across both jitter shapes:
#   "lag":  worker still sends every heartbeat but each one arrives delayed
#   "drop": worker skips heartbeats outright (no lag), creating discrete gaps
for PHI in 3 5 8 12; do
  run_one "$PHI" lag  2500ms 1000ms 0.0
done
for PHI in 3 5 8 12; do
  run_one "$PHI" drop 0ms    0ms    0.5
done

echo
cat "$OUT"
