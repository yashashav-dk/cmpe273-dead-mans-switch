#!/usr/bin/env bash
# Sweep N in {10, 100, 1000} for both push and pull modes, capturing for the
# Monitor process: peak resident memory (KB), CPU seconds, and the inter-event
# lag from worker-1's last heartbeat to its eventual DEAD declaration after a
# kill.
#
# Output: bench.csv with columns:
#   N,mode,detector,peak_rss_kb,cpu_secs,detection_latency_ms
set -euo pipefail

cd "$(dirname "$0")/.."
make build >/dev/null

OUT="bench.csv"
echo "N,mode,detector,peak_rss_kb,cpu_secs,detection_latency_ms" > "$OUT"

run_one() {
  local N=$1 MODE=$2 DET=$3
  local LOG="/tmp/bench-$N-$MODE-$DET.jsonl"
  rm -f "$LOG"

  ./bin/monitor --listen=:53000 --mode="$MODE" --detector="$DET" \
      --hb-interval=1s --eval-interval=200ms \
      --tui=false --log-file="$LOG" &
  local MON=$!
  sleep 0.5

  local pids=()
  for i in $(seq 1 "$N"); do
    local port=$((54000 + i))
    ./bin/worker --id="bw-$i" --monitor=127.0.0.1:53000 --listen=":$port" \
        --hb-interval=1s >/dev/null 2>&1 &
    pids+=($!)
    if (( i % 50 == 0 )); then sleep 0.2; fi
  done

  # Warm up. Must exceed phi-min-samples × hb-interval (10s default) so the
  # PhiAccrual sliding window is populated before the kill — otherwise pull
  # mode's bootstrap fallback declares DEAD prematurely and the latency we
  # measure is not phi's verdict.
  sleep 15

  # Kill worker bw-1 and time the DEAD transition.
  local kill_time
  kill_time=$(python3 -c 'import time;print(int(time.time()*1000))')
  kill -9 "${pids[0]}" || true

  local detected_ms=""
  local deadline=$(( $(date +%s) + 30 ))
  while [ "$(date +%s)" -lt $deadline ]; do
    # Look for any state event for bw-1 that lands in DEAD; field order in
    # the log envelope is not guaranteed, so match worker and to=DEAD separately.
    if grep '"worker":"bw-1"' "$LOG" | grep -q '"to":"DEAD"' ; then
      detected_ms=$(python3 -c 'import time;print(int(time.time()*1000))')
      break
    fi
    sleep 0.1
  done
  local latency_ms=""
  if [ -n "$detected_ms" ]; then
    latency_ms=$((detected_ms - kill_time))
  fi

  # Capture monitor RSS + CPU before tearing down.
  local rss_kb cpu_secs
  if rss_kb=$(ps -o rss= -p "$MON" 2>/dev/null | tr -d ' '); then :; else rss_kb=""; fi
  if cpu_secs=$(ps -o cputime= -p "$MON" 2>/dev/null | awk -F: '{ if (NF==3) print $1*3600+$2*60+$3; else print $1*60+$2 }'); then :; else cpu_secs=""; fi

  echo "$N,$MODE,$DET,${rss_kb:-NA},${cpu_secs:-NA},${latency_ms:-NA}" >> "$OUT"

  kill "$MON" 2>/dev/null || true
  for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
  sleep 1
}

for N in 10 100 1000; do
  for MODE in push pull; do
    for DET in phi fixed; do
      echo "=== N=$N mode=$MODE det=$DET ==="
      run_one "$N" "$MODE" "$DET"
    done
  done
done

echo "wrote $OUT"
