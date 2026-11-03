#!/usr/bin/env bash
set -euo pipefail
#
# Run one daemon-crash rollback trial (user daemon backend only).
#
#   scripts/consistency/run_daemon_crash.bash --mode kill --output-dir /tmp/trial
#   scripts/consistency/run_daemon_crash.bash --mode fault --fault-point shm.remove_subscriber \
#     --fault-hit 3 --output-dir /tmp/trial
#   scripts/consistency/run_daemon_crash.bash --mode none --output-dir /tmp/golden
#
# Topology: 1 publisher (tight loop) + N holder subscribers (executor, keep the
# latest message) + 1 churn process that keeps registering and removing a
# subscriber and a publisher, so the daemon is often inside a segment operation.
#
# Crash modes:
#   kill   SIGKILL the daemon --kill-delay-sec after START
#   fault  the daemon kills itself at --fault-point on its --fault-hit-th visit
#          after START (visits made by the setup are not counted)
#   none   no crash (golden run; same flow otherwise)
#
# After the crash the daemon stays down for --outage-ms, then a fresh daemon is
# started. Recorded per trial (metrics.json):
#   continuity     publishes and deliveries made while the daemon was down
#   recovery       the new daemon's recovery time and what it restored / dropped
#   post_flow      publishes and deliveries after the recovery
#   snapshot.json  membership + segment invariant check, after the churn
#                  stopped and the holders let go (same procedure as the
#                  subscriber-crash trial in run.bash)
#   outstanding    publisher's outstanding entries; must be back to qos_depth
#   survivors      whether every client process is still alive

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

cons_bin() {
  echo "$(install_dir daemon)/agnocast_consistency/lib/agnocast_consistency/consistency_$1"
}

MODE="kill"
FAULT_POINT=""
FAULT_HIT=1
OUTPUT_DIR=""
TOPIC="/consistency"
QOS_DEPTH=10
NUM_SUBSCRIBERS=4
WARMUP_SEC="0.5"
KILL_DELAY_SEC=""
OUTAGE_MS=300
POST_SEC="0.5"
WAIT_TIMEOUT_SEC=20
FAULT_WAIT_SEC=10
CHURN=1
CHURN_INTERVAL_MS=2
RECONNECT=1
RECOVERY_TIMEOUT_MS=5000


while [[ $# -gt 0 ]]; do
  case "$1" in
  --mode) MODE="$2"; shift 2 ;;
  --fault-point) FAULT_POINT="$2"; shift 2 ;;
  --fault-hit) FAULT_HIT="$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  --topic) TOPIC="$2"; shift 2 ;;
  --qos-depth) QOS_DEPTH="$2"; shift 2 ;;
  --num-subscribers) NUM_SUBSCRIBERS="$2"; shift 2 ;;
  --warmup-sec) WARMUP_SEC="$2"; shift 2 ;;
  --kill-delay-sec) KILL_DELAY_SEC="$2"; shift 2 ;;
  --outage-ms) OUTAGE_MS="$2"; shift 2 ;;
  --post-sec) POST_SEC="$2"; shift 2 ;;
  --wait-timeout-sec) WAIT_TIMEOUT_SEC="$2"; shift 2 ;;
  --no-churn) CHURN=0; shift ;;
  --churn-interval-ms) CHURN_INTERVAL_MS="$2"; shift 2 ;;
  --no-reconnect) RECONNECT=0; shift ;;
  --recovery-timeout-ms) RECOVERY_TIMEOUT_MS="$2"; shift 2 ;;
  -h | --help) sed -n '3,31p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done

case "${MODE}" in
kill | none) ;;
fault) [[ -n "${FAULT_POINT}" ]] || die "--mode fault needs --fault-point" ;;
*) die "--mode must be kill, fault or none" ;;
esac
[[ -n "${OUTPUT_DIR}" ]] || die "--output-dir is required"
[[ "${NUM_SUBSCRIBERS}" -ge 1 ]] || die "--num-subscribers must be >= 1"
# Default: a random point in [0.2, 1.0) s, so the kill lands at varied phases.
if [[ -z "${KILL_DELAY_SEC}" ]]; then
  KILL_DELAY_SEC="$(awk -v r="${RANDOM}" 'BEGIN { printf "%.3f", 0.2 + (r / 32768.0) * 0.8 }')"
fi

setup="$(install_dir daemon)/setup.bash"
[[ -f "${setup}" ]] || die "daemon workspace not built. Run: scripts/consistency/build.bash daemon"
[[ -x "$(cons_bin churn)" ]] || die "consistency_churn missing. Run: scripts/consistency/build.bash daemon"
set +u
# shellcheck disable=SC1090
source "${setup}"
set -u
HEAPHOOK_LIB="$(heaphook_lib daemon)"
CONS_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"

mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
LOG_DIR="${OUTPUT_DIR}/logs"
SYNC_DIR="${OUTPUT_DIR}/sync"
STATUS_FILE="${OUTPUT_DIR}/publisher_status.txt"
mkdir -p "${LOG_DIR}" "${SYNC_DIR}"

PIDS=()
CHURN_PID=""
cleanup() {
  local pid
  for pid in "${PIDS[@]:-}" ${CHURN_PID}; do
    [[ -n "${pid}" ]] && kill -KILL "${pid}" 2>/dev/null || true
  done
  stop_daemon
  pkill -KILL -x agnocast_daemon 2>/dev/null || true
}
trap cleanup EXIT

now_ms() { date +%s%3N; }

wait_for_files() {
  local timeout="$1"
  shift
  local start f missing
  start="$(date +%s)"
  while true; do
    missing=0
    for f in "$@"; do
      [[ -f "${f}" ]] || { missing=1; break; }
    done
    [[ "${missing}" -eq 0 ]] && return 0
    (($(date +%s) - start >= timeout)) && return 1
    sleep 0.02
  done
}

wait_for_clean_state() {
  pkill -9 -f '[c]onsistency_(publisher|subscriber|observer|churn)' 2>/dev/null || true
  pkill -KILL -x agnocast_daemon 2>/dev/null || true
  for _ in $(seq 1 50); do
    pgrep -f '[c]onsistency_(publisher|subscriber|observer|churn)' >/dev/null 2>&1 || break
    sleep 0.1
  done
  rm -f /dev/shm/agnocast@* /dev/mqueue/agnocast@* 2>/dev/null || true
  rm -f "${AGNOCAST_DAEMON_SOCKET_PATH}" "${AGNOCAST_DAEMON_STATE_PATH}" 2>/dev/null || true
}

bg_launch() {
  local log="$1"
  shift
  env \
    LD_LIBRARY_PATH="${CONS_LD_LIBRARY_PATH}" \
    LD_PRELOAD="${HEAPHOOK_LIB}" \
    AGNOCAST_BRIDGE_MODE=off \
    AGNOCAST_NO_DISCOVERY_AGENT=1 \
    AGNOCAST_DAEMON_RECONNECT="${RECONNECT}" \
    "$@" >"${log}" 2>&1 &
  disown $! 2>/dev/null || true
}

observer() {
  local log="$1"
  shift
  env \
    LD_LIBRARY_PATH="${CONS_LD_LIBRARY_PATH}" \
    LD_PRELOAD="${HEAPHOOK_LIB}" \
    AGNOCAST_BRIDGE_MODE=off \
    AGNOCAST_NO_DISCOVERY_AGENT=1 \
    "$(cons_bin observer)" --ros-args -p topic:="${TOPIC}" \
    -p exclude_node_substr:=churn -p wait_timeout_sec:="${WAIT_TIMEOUT_SEC}" "$@" \
    >"${log}" 2>&1
}

daemon_pid() { pgrep -n -x agnocast_daemon 2>/dev/null || true; }

launch_daemon() {
  # $@: extra environment (VAR=value) for this daemon only
  rm -f "${AGNOCAST_DAEMON_SOCKET_PATH}"
  BENCH_DAEMON_PID=""
  env "$@" AGNOCAST_DAEMON_RECOVERY_TIMEOUT_MS="${RECOVERY_TIMEOUT_MS}" \
    "${AGNOCAST_DAEMON_BIN}" >>"${LOG_DIR}/agnocast_daemon.log" 2>&1 &
  BENCH_DAEMON_PID=$!
  for _ in $(seq 1 100); do
    daemon_running && return 0
    kill -0 "${BENCH_DAEMON_PID}" 2>/dev/null || die "agnocast_daemon exited during startup"
    sleep 0.05
  done
  die "agnocast_daemon did not create its socket"
}

# Counters: "<published> <sub_0> <sub_1> ..."
sample_counters() {
  local pub=0 c idx
  if [[ -f "${STATUS_FILE}" ]]; then read -r pub _ <"${STATUS_FILE}" || pub=0; fi
  c="${pub}"
  for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do
    local f="${OUTPUT_DIR}/sub_${idx}_status.txt" n=0
    if [[ -f "${f}" ]]; then read -r n <"${f}" || n=0; fi
    c="${c} ${n}"
  done
  echo "${c}"
}

FAILED_STEP=""
fail_step() {
  [[ -z "${FAILED_STEP}" ]] && FAILED_STEP="$1"
  echo "WARNING: $1" >&2
}

wait_for_clean_state

FAULT_ENV=()
if [[ "${MODE}" == "fault" ]]; then
  # --fault-hit counts from START: skip the visits the setup itself makes
  # (each holder's subscriber registration, the publisher's registration).
  setup_visits=0
  case "${FAULT_POINT}" in
  *add_subscriber) setup_visits="${NUM_SUBSCRIBERS}" ;;
  *add_publisher) setup_visits=1 ;;
  esac
  FAULT_ENV=(AGNOCAST_DAEMON_FAULT_POINT="${FAULT_POINT}"
    AGNOCAST_DAEMON_FAULT_HIT="$((FAULT_HIT + setup_visits))")
fi
launch_daemon "${FAULT_ENV[@]}"
FIRST_DAEMON_PID="${BENCH_DAEMON_PID}"

bg_launch "${LOG_DIR}/publisher.log" "$(cons_bin publisher)" --ros-args \
  -p topic:="${TOPIC}" -p qos_depth:="${QOS_DEPTH}" \
  -p status_file:="${STATUS_FILE}" -p sync_dir:="${SYNC_DIR}"
PUB_PID=$!
PIDS+=("${PUB_PID}")

SUB_PIDS=()
READY_FILES=("${SYNC_DIR}/pub.ready")
for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do
  bg_launch "${LOG_DIR}/subscriber_${idx}.log" \
    CONSISTENCY_SUB_INDEX="${idx}" "$(cons_bin subscriber)" --ros-args \
    -p topic:="${TOPIC}" -p qos_depth:="${QOS_DEPTH}" -p role:=holder \
    -p subscriber_index:="${idx}" -p sync_dir:="${SYNC_DIR}" \
    -p status_file:="${OUTPUT_DIR}/sub_${idx}_status.txt"
  SUB_PIDS+=("$!")
  PIDS+=("$!")
  READY_FILES+=("${SYNC_DIR}/sub_${idx}.ready")
done

wait_for_files "${WAIT_TIMEOUT_SEC}" "${READY_FILES[@]}" || die "pub/sub did not become ready. See ${LOG_DIR}"
observer "${LOG_DIR}/observer_wait.log" -p wait_publishers:=1 \
  -p wait_subscribers:="${NUM_SUBSCRIBERS}" -p output:="${OUTPUT_DIR}/ready.json" ||
  die "topology did not come up. See ${LOG_DIR}/observer_wait.log"

touch "${SYNC_DIR}/START"
if [[ "${CHURN}" -eq 1 ]]; then
  bg_launch "${LOG_DIR}/churn.log" "$(cons_bin churn)" --ros-args \
    -p topic:="${TOPIC}" -p qos_depth:="${QOS_DEPTH}" -p interval_ms:="${CHURN_INTERVAL_MS}"
  CHURN_PID=$!
fi
sleep "${WARMUP_SEC}"

# ---- crash -------------------------------------------------------------------
CRASHED=0
T_CRASH=""
case "${MODE}" in
kill)
  sleep "${KILL_DELAY_SEC}"
  kill -KILL "${FIRST_DAEMON_PID}" 2>/dev/null || true
  wait "${FIRST_DAEMON_PID}" 2>/dev/null || true
  T_CRASH="$(now_ms)"
  CRASHED=1
  ;;
fault)
  deadline=$(($(date +%s) + FAULT_WAIT_SEC))
  while kill -0 "${FIRST_DAEMON_PID}" 2>/dev/null && (($(date +%s) < deadline)); do
    sleep 0.005
  done
  if ! kill -0 "${FIRST_DAEMON_PID}" 2>/dev/null; then
    wait "${FIRST_DAEMON_PID}" 2>/dev/null || true
    T_CRASH="$(now_ms)"
    CRASHED=1
  fi
  ;;
none) ;;
esac
BENCH_DAEMON_PID=""
C0="$(sample_counters)"

RECOVERY_WALL_MS=-1
if [[ "${CRASHED}" -eq 1 ]]; then
  sleep "$(awk -v ms="${OUTAGE_MS}" 'BEGIN { printf "%.3f", ms / 1000 }')"
  C1="$(sample_counters)"
  T_RESTART="$(now_ms)"
  launch_daemon
  if observer "${LOG_DIR}/observer_recovery.log" -p wait_recovery_done:=true \
    -p recovery_output:="${OUTPUT_DIR}/recovery.json"; then
    :
  else
    fail_step "recovery did not finish"
  fi
  if observer "${LOG_DIR}/observer_recovered.log" -p wait_publishers:=1 \
    -p wait_subscribers:="${NUM_SUBSCRIBERS}" -p output:="${OUTPUT_DIR}/recovered.json"; then
    RECOVERY_WALL_MS=$(($(now_ms) - T_RESTART))
  else
    fail_step "membership did not come back after the restart"
  fi
else
  C1="${C0}"
fi
sleep "${POST_SEC}"
C2="$(sample_counters)"

# ---- quiesce and check -------------------------------------------------------
CHURN_EXIT=-1
if [[ -n "${CHURN_PID}" ]]; then
  kill -INT "${CHURN_PID}" 2>/dev/null || true
  for _ in $(seq 1 $((WAIT_TIMEOUT_SEC * 20))); do
    kill -0 "${CHURN_PID}" 2>/dev/null || break
    sleep 0.05
  done
  if kill -0 "${CHURN_PID}" 2>/dev/null; then
    fail_step "churn did not exit after SIGINT"
    kill -KILL "${CHURN_PID}" 2>/dev/null || true
  else
    CHURN_EXIT=0
  fi
  CHURN_PID=""
fi

touch "${SYNC_DIR}/SNAP"
HOLDER_PAUSE=()
for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do HOLDER_PAUSE+=("${SYNC_DIR}/sub_${idx}.paused"); done
wait_for_files "${WAIT_TIMEOUT_SEC}" "${HOLDER_PAUSE[@]}" || fail_step "holders did not pause"

touch "${SYNC_DIR}/RELEASE_HELD"
RELEASED=()
for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do RELEASED+=("${SYNC_DIR}/sub_${idx}.released"); done
wait_for_files "${WAIT_TIMEOUT_SEC}" "${RELEASED[@]}" || fail_step "holders did not release"

# The publisher keeps publishing, and each publish hands back up to 3 entries
# beyond its depth; wait until only the depth (+1 in flight) is outstanding.
drain_start="$(date +%s)"
while true; do
  out=""
  [[ -f "${STATUS_FILE}" ]] && read -r _ _ out <"${STATUS_FILE}" || true
  [[ -n "${out}" && "${out}" -ge "${QOS_DEPTH}" && "${out}" -le "$((QOS_DEPTH + 1))" ]] && break
  if (($(date +%s) - drain_start >= WAIT_TIMEOUT_SEC)); then
    echo "WARNING: outstanding did not return to ${QOS_DEPTH}..$((QOS_DEPTH + 1))" >&2
    break
  fi
  sleep 0.01
done
touch "${SYNC_DIR}/FLUSH"
wait_for_files "${WAIT_TIMEOUT_SEC}" "${SYNC_DIR}/pub.flushed" || fail_step "publisher did not flush"
[[ -f "${STATUS_FILE}" ]] && cp "${STATUS_FILE}" "${OUTPUT_DIR}/outstanding.txt"
touch "${SYNC_DIR}/PAUSE"
wait_for_files "${WAIT_TIMEOUT_SEC}" "${SYNC_DIR}/pub.paused" || fail_step "publisher did not pause"

observer "${LOG_DIR}/observer_snapshot.log" -p check_invariants:=true \
  -p output:="${OUTPUT_DIR}/snapshot.json" || fail_step "snapshot failed"

SURVIVORS=0
for pid in "${PIDS[@]}"; do
  kill -0 "${pid}" 2>/dev/null && SURVIVORS=$((SURVIVORS + 1))
done
RECONNECTS=$(cat "${LOG_DIR}"/publisher.log "${LOG_DIR}"/subscriber_*.log 2>/dev/null |
  grep -c "reconnected to agnocast_daemon" || true)
FAULT_LINE="$(grep -m1 "fault injection" "${LOG_DIR}/agnocast_daemon.log" 2>/dev/null || true)"

python3 - "${OUTPUT_DIR}/metrics.json" <<EOF
import json, sys
c0 = [int(x) for x in "${C0}".split()]
c1 = [int(x) for x in "${C1}".split()]
c2 = [int(x) for x in "${C2}".split()]
json.dump({
    "mode": "${MODE}",
    "fault_point": "${FAULT_POINT}",
    "fault_hit": ${FAULT_HIT},
    "fault_line": """${FAULT_LINE}""",
    "kill_delay_sec": ${KILL_DELAY_SEC},
    "outage_ms": ${OUTAGE_MS},
    "reconnect": ${RECONNECT},
    "churn": ${CHURN},
    "crashed": bool(${CRASHED}),
    "counters_at_crash": c0,
    "counters_before_restart": c1,
    "counters_after_recovery": c2,
    "outage_published": c1[0] - c0[0],
    "outage_received_min": min(b - a for a, b in zip(c0[1:], c1[1:])) if len(c0) > 1 else 0,
    "post_published": c2[0] - c1[0],
    "post_received_min": min(b - a for a, b in zip(c1[1:], c2[1:])) if len(c1) > 1 else 0,
    "recovery_wall_ms": ${RECOVERY_WALL_MS},
    "survivors": ${SURVIVORS},
    "clients": ${#PIDS[@]},
    "reconnects": ${RECONNECTS},
    "churn_exit": ${CHURN_EXIT},
    "failed_step": "${FAILED_STEP}",
}, open(sys.argv[1], "w"), indent=2)
EOF

echo "trial written to ${OUTPUT_DIR}${FAILED_STEP:+ (failed: ${FAILED_STEP})}"
