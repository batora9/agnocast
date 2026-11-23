#!/bin/bash
# Smoke test: kill the daemon under a running pub/sub and restart it.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT=${1:?out dir}
KILL_AT=${KILL_AT:-4}
OUTAGE=${OUTAGE:-0.5}
DURATION=${DURATION:-10}
rm -rf "$OUT"; mkdir -p "$OUT/sync" "$OUT/res"
export AGNOCAST_DAEMON_STATE_PATH="$OUT/state"
DAEMON=${DAEMON:-$ROOT/agnocast_daemon/build/agnocast_daemon}
export AGNOCAST_DAEMON_REGION_PATH=/dev/shm/agnocast_eval.a AGNOCAST_DAEMON_LOG_PATH=/dev/shm/agnocast_eval.b
rm -f /dev/shm/agnocast_eval.a /dev/shm/agnocast_eval.b
set +u; source /opt/ros/humble/setup.bash; source $ROOT/ws/daemon/install/setup.bash; set -u
HEAP=$ROOT/ws/daemon/install/agnocastlib/lib/libagnocast_heaphook.so
PUB=$ROOT/ws/daemon/install/agnocast_bench_publisher/lib/agnocast_bench_publisher/bench_publisher
SUB=$ROOT/ws/daemon/install/agnocast_bench_subscriber/lib/agnocast_bench_subscriber/bench_subscriber
env ${FAULT_POINT:+AGNOCAST_DAEMON_FAULT_POINT=$FAULT_POINT AGNOCAST_DAEMON_FAULT_HIT=${FAULT_HIT:-1}}   "$DAEMON" > "$OUT/daemon1.log" 2>&1 &
D1=$!
for _ in $(seq 50); do [[ -S /tmp/agnocast_daemon.sock ]] && break; sleep 0.1; done
common=(-p topic_prefix:=/e2e -p topic_index:=0 -p rate_hz:=100.0 -p duration_sec:=${DURATION}.0
  -p warmup_sec:=1.0 -p keep_alive_sec:=2.0 -p qos_depth:=10 -p output_dir:="$OUT/res" -p sync_dir:="$OUT/sync")
run() { exec env LD_PRELOAD="$HEAP" AGNOCAST_BRIDGE_MODE=off AGNOCAST_NO_DISCOVERY_AGENT=1 "$@"; }
run "$PUB" --ros-args "${common[@]}" > "$OUT/pub.log" 2>&1 & P=$!
for _ in $(seq 100); do ls "$OUT"/sync/pub_*.ready >/dev/null 2>&1 && break; sleep 0.1; done
run "$SUB" --ros-args "${common[@]}" -p subscriber_index:=0 > "$OUT/sub0.log" 2>&1 & S0=$!
run "$SUB" --ros-args "${common[@]}" -p subscriber_index:=1 > "$OUT/sub1.log" 2>&1 & S1=$!
for _ in $(seq 100); do [[ $(ls "$OUT"/sync/sub_*.ready 2>/dev/null | wc -l) -ge 2 ]] && break; sleep 0.1; done
touch "$OUT/sync/START"
D2=$D1
if [[ -n "${FAULT_POINT:-}" ]]; then
  wait $D1 2>/dev/null
  echo "daemon died at fault point $FAULT_POINT (hit ${FAULT_HIT:-1})" > "$OUT/events"
  sleep "$OUTAGE"
  "$DAEMON" > "$OUT/daemon2.log" 2>&1 &
  D2=$!
elif [[ "$KILL_AT" != none ]]; then
  sleep "$KILL_AT"
  kill -9 $D1; wait $D1 2>/dev/null
  echo "killed daemon at ${KILL_AT}s" > "$OUT/events"
  sleep "$OUTAGE"
  "$DAEMON" > "$OUT/daemon2.log" 2>&1 &
  D2=$!
fi
touch "$OUT/events" "$OUT/daemon2.log"
rc=0
for p in $P $S0 $S1; do wait $p || { echo "client $p exited with $?" >> "$OUT/events"; rc=1; }; done
kill -TERM $D2; wait $D2 2>/dev/null
cat "$OUT/events"; echo "--- daemon2"; cat "$OUT/daemon2.log"
echo "--- client reconnects"; grep -h "reconnect\|restored\|failed\|ERROR" "$OUT"/pub.log "$OUT"/sub*.log | head -20
pkill -9 -x consistency_pub 2>/dev/null; pkill -9 -x consistency_sub 2>/dev/null; pkill -9 -x bench_publisher 2>/dev/null; pkill -9 -x bench_subscribe 2>/dev/null
exit $rc
