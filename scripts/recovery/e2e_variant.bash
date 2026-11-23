#!/bin/bash
# Daemon restart under a tight-loop publisher and NSUB subscribers that hold
# their latest message, for one recovery variant (DAEMON=<binary>).
#   KILL_AT/OUTAGE      kill time and downtime (random kill)
#   FAULT_POINT/_HIT    let the first daemon kill itself at a fault point
#   CORRUPT_HIT=<n>     inject a corrupted reference at publish n and check
#                       references (the daemon aborts); restart up to 3 times
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT=${1:?out dir}
DAEMON=${DAEMON:?daemon binary}
KILL_AT=${KILL_AT:-1.5}
OUTAGE=${OUTAGE:-0.3}
DEPTH=${DEPTH:-10}
NSUB=${NSUB:-2}
rm -rf "$OUT"; mkdir -p "$OUT/sync"
export AGNOCAST_DAEMON_STATE_PATH="$OUT/state"
export AGNOCAST_DAEMON_REGION_PATH="/dev/shm/agnocast_eval.a"
export AGNOCAST_DAEMON_LOG_PATH="/dev/shm/agnocast_eval.b"
rm -f /dev/shm/agnocast_eval.a /dev/shm/agnocast_eval.b
set +u; source /opt/ros/humble/setup.bash; source $ROOT/ws/daemon/install/setup.bash; set -u
I=$ROOT/ws/daemon/install
HEAP=$I/agnocastlib/lib/libagnocast_heaphook.so
PUB=$I/agnocast_consistency/lib/agnocast_consistency/consistency_publisher
SUB=$I/agnocast_consistency/lib/agnocast_consistency/consistency_subscriber
run() { exec env LD_PRELOAD="$HEAP" AGNOCAST_BRIDGE_MODE=off AGNOCAST_NO_DISCOVERY_AGENT=1 "$@"; }
check_env=()
if [[ -n "${CORRUPT_HIT:-}" ]]; then
  check_env=(AGNOCAST_DAEMON_CHECK=1)
  env "${check_env[@]}" AGNOCAST_DAEMON_FAULT_POINT=corrupt_reference AGNOCAST_DAEMON_FAULT_HIT=$CORRUPT_HIT \
    "$DAEMON" > "$OUT/daemon1.log" 2>&1 &
else
  env ${FAULT_POINT:+AGNOCAST_DAEMON_FAULT_POINT=$FAULT_POINT AGNOCAST_DAEMON_FAULT_HIT=${FAULT_HIT:-1}} \
    "$DAEMON" > "$OUT/daemon1.log" 2>&1 &
fi
D1=$!
for _ in $(seq 50); do [[ -S /tmp/agnocast_daemon.sock ]] && break; sleep 0.1; done
run "$PUB" --ros-args -p topic:=/hold -p qos_depth:=$DEPTH -p status_file:="$OUT/pub.status" \
  -p sync_dir:="$OUT/sync" > "$OUT/pub.log" 2>&1 & P=$!
SUBS=()
for i in $(seq 0 $((NSUB - 1))); do
  CONSISTENCY_SUB_INDEX=$i run "$SUB" --ros-args -p topic:=/hold -p role:=holder -p qos_depth:=$DEPTH \
    -p subscriber_index:=$i -p sync_dir:="$OUT/sync" > "$OUT/sub$i.log" 2>&1 &
  SUBS+=($!)
done
for _ in $(seq 200); do [[ $(ls "$OUT"/sync/*.ready 2>/dev/null | wc -l) -ge $((NSUB + 1)) ]] && break; sleep 0.1; done
touch "$OUT/sync/START"
if [[ -n "${FAULT_POINT:-}${CORRUPT_HIT:-}" ]]; then
  wait $D1 2>/dev/null
else
  sleep "$KILL_AT"; kill -9 $D1; wait $D1 2>/dev/null
fi
crashes=0
D2=""
for attempt in 1 2 3; do
  sleep "$OUTAGE"
  env "${check_env[@]}" "$DAEMON" > "$OUT/daemon2_$attempt.log" 2>&1 & D2=$!
  sleep 1.5
  if kill -0 $D2 2>/dev/null; then break; fi
  wait $D2 2>/dev/null; crashes=$((crashes + 1)); D2=""
done
if [[ -n "$D2" ]]; then
  touch "$OUT/sync/DROP"
  for _ in $(seq 100); do [[ $(ls "$OUT"/sync/sub_*.dropped 2>/dev/null | wc -l) -ge $NSUB ]] && break; sleep 0.05; done
  sleep 0.5
  touch "$OUT/sync/FLUSH"
  for _ in $(seq 100); do [[ -f "$OUT/sync/pub.flushed" ]] && break; sleep 0.05; done
fi
alive=0; for p in $P "${SUBS[@]}"; do kill -0 $p 2>/dev/null && alive=$((alive+1)); done
touch "$OUT/sync/PAUSE"; sleep 0.2
kill -INT $P "${SUBS[@]}" 2>/dev/null; sleep 0.5; kill -9 $P "${SUBS[@]}" 2>/dev/null
[[ -n "$D2" ]] && { kill -TERM $D2; wait $D2 2>/dev/null; }
pkill -9 -x consistency_pub 2>/dev/null; pkill -9 -x consistency_sub 2>/dev/null
L=$(ls -t "$OUT"/daemon2_*.log 2>/dev/null | head -1)
refs=$(grep -o "[0-9]* references" "$L" | grep -o "^[0-9]*")
rej=$(grep -o "[0-9]* rejected" "$L" | grep -o "^[0-9]*")
dedup=$(grep -c "repeated publish" "$L")
grace=$(grep -o "recovery done in [0-9.]*" "$L" | grep -o "[0-9.]*$")
load=$(grep -o "loaded the persisted metadata in [0-9.]*" "$L" | grep -o "[0-9.]*$")
outstanding=$(awk '{print $3}' "$OUT/pub.status" 2>/dev/null)
ok=OK
[[ -n "$D2" && "$alive" == $((NSUB + 1)) && "${rej:-0}" == 0 && "${outstanding:-999}" -le $((DEPTH + 1)) ]] || ok=NG
echo "alive=$alive/$((NSUB + 1)) crashes_after_restart=$crashes refs=${refs:-} rejected=${rej:-} dedup=$dedup outstanding=${outstanding:-} grace_ms=${grace:-} load_ms=${load:-} $ok"
