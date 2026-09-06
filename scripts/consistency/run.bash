#!/usr/bin/env bash
set -euo pipefail
#
# Run one metadata-consistency trial.
#
#   scripts/consistency/run.bash --backend daemon --crash-role sub --signal kill \
#     --output-dir results/consistency/trial
#
# Topology is 1 publisher + 4 subscribers. The victim (sub_0 or the publisher)
# is in a tight metadata loop; remaining subscribers hold the latest message.
# After SIGINT or SIGKILL:
#   subscriber crash — pause holders, wait until the victim leaves public
#     membership (process-exit cleanup has cleared its ref bits), then FLUSH
#     outstanding while the publisher is still publishing, then pause it and
#     snapshot membership.
#   publisher crash — wait --cleanup-wait-sec, pause holders, snapshot, then
#     DROP holder refs and snapshot again (after_drop.json).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

consistency_pub_bin() {
  echo "$(install_dir "$1")/agnocast_consistency/lib/agnocast_consistency/consistency_publisher"
}
consistency_sub_bin() {
  echo "$(install_dir "$1")/agnocast_consistency/lib/agnocast_consistency/consistency_subscriber"
}
consistency_obs_bin() {
  echo "$(install_dir "$1")/agnocast_consistency/lib/agnocast_consistency/consistency_observer"
}

source_consistency_ws() {
  local backend="$1"
  local setup
  setup="$(install_dir "${backend}")/setup.bash"
  [[ -f "${setup}" ]] || die "workspace for '${backend}' not built. Run: scripts/consistency/build.bash ${backend}"
  [[ -x "$(consistency_pub_bin "${backend}")" ]] ||
    die "consistency_publisher missing for '${backend}'. Run: scripts/consistency/build.bash ${backend}"
  [[ -f "$(heaphook_lib "${backend}")" ]] ||
    die "libagnocast_heaphook.so missing for '${backend}'. Run: scripts/consistency/build.bash ${backend}"
  set +u
  # shellcheck disable=SC1090
  source "${setup}"
  set -u
}

BACKEND=""
CRASH_ROLE="sub"
SIGNAL="kill"
OUTPUT_DIR=""
TOPIC="/consistency"
QOS_DEPTH=10
NUM_SUBSCRIBERS=4
WARMUP_SEC="0.2"
CLEANUP_WAIT_SEC="1"
FLUSH_WINDOW_SEC="1"
WAIT_TIMEOUT_SEC="15"
TRACE_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --backend) BACKEND="$2"; shift 2 ;;
  --crash-role) CRASH_ROLE="$2"; shift 2 ;;
  --signal) SIGNAL="$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  --topic) TOPIC="$2"; shift 2 ;;
  --qos-depth) QOS_DEPTH="$2"; shift 2 ;;
  --num-subscribers) NUM_SUBSCRIBERS="$2"; shift 2 ;;
  --warmup-sec) WARMUP_SEC="$2"; shift 2 ;;
  --cleanup-wait-sec) CLEANUP_WAIT_SEC="$2"; shift 2 ;;
  --flush-window-sec) FLUSH_WINDOW_SEC="$2"; shift 2 ;;
  --wait-timeout-sec) WAIT_TIMEOUT_SEC="$2"; shift 2 ;;
  --trace-file) TRACE_FILE="$2"; shift 2 ;;
  -h | --help) sed -n '3,15p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done

[[ -n "${BACKEND}" ]] || die "--backend kmod|daemon is required"
validate_backend "${BACKEND}"
[[ "${CRASH_ROLE}" == "sub" || "${CRASH_ROLE}" == "pub" ]] || die "--crash-role must be sub or pub"
[[ "${SIGNAL}" == "int" || "${SIGNAL}" == "kill" ]] || die "--signal must be int or kill"
[[ "${NUM_SUBSCRIBERS}" -ge 2 ]] || die "--num-subscribers must be >= 2"
OUTPUT_DIR="${OUTPUT_DIR:-${BENCH_ROOT}/results/consistency/${BACKEND}_${CRASH_ROLE}_${SIGNAL}}"

source_consistency_ws "${BACKEND}"
HEAPHOOK_LIB="$(heaphook_lib "${BACKEND}")"
PUB_BIN="$(consistency_pub_bin "${BACKEND}")"
SUB_BIN="$(consistency_sub_bin "${BACKEND}")"
OBS_BIN="$(consistency_obs_bin "${BACKEND}")"
CONS_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"

PIDS=()
cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    kill -KILL "${pid}" 2>/dev/null || true
  done
  stop_daemon
}
trap cleanup EXIT

wait_for_files() {
  local timeout="$1"
  shift
  local start now f
  start="$(date +%s)"
  while true; do
    local missing=0
    for f in "$@"; do
      if [[ ! -f "${f}" ]]; then
        missing=1
        break
      fi
    done
    [[ "${missing}" -eq 0 ]] && return 0
    now="$(date +%s)"
    if ((now - start >= timeout)); then
      echo "ERROR: timed out waiting for: $*" >&2
      return 1
    fi
    sleep 0.05
  done
}

# True when publisher_status.txt outstanding is in [lo, hi].
outstanding_in_range() {
  local lo="$1"
  local hi="$2"
  local pub rel out
  [[ -f "${STATUS_FILE}" ]] || return 1
  read -r pub rel out <"${STATUS_FILE}" || return 1
  [[ -n "${out:-}" ]] || return 1
  [[ "${out}" -ge "${lo}" && "${out}" -le "${hi}" ]]
}

wait_for_clean_state() {
  pkill -9 -f '[c]onsistency_publisher|[c]onsistency_subscriber|[c]onsistency_observer' 2>/dev/null || true
  for _ in $(seq 1 40); do
    pgrep -f '[c]onsistency_publisher|[c]onsistency_subscriber|[c]onsistency_observer' >/dev/null 2>&1 || break
    sleep 0.1
  done
  for _ in $(seq 1 40); do
    local shm mq
    shm=$(find /dev/shm -maxdepth 1 -name 'agnocast@*' 2>/dev/null | wc -l)
    mq=$(find /dev/mqueue -maxdepth 1 -name 'agnocast@*' 2>/dev/null | wc -l)
    [[ "${shm}" -eq 0 && "${mq}" -eq 0 ]] && break
    sleep 0.1
  done
  rm -f /dev/shm/agnocast@* /dev/mqueue/agnocast@* 2>/dev/null || true
}

launch_env() {
  env \
    LD_LIBRARY_PATH="${CONS_LD_LIBRARY_PATH}" \
    LD_PRELOAD="${HEAPHOOK_LIB}" \
    AGNOCAST_BRIDGE_MODE=off \
    AGNOCAST_NO_DISCOVERY_AGENT=1 \
    "$@"
}

# Start a node in the background so $! is the env/binary PID, not a bash
# function subshell (those ignore SIGINT, which broke the golden path).
bg_launch() {
  local log="$1"
  shift
  env \
    LD_LIBRARY_PATH="${CONS_LD_LIBRARY_PATH}" \
    LD_PRELOAD="${HEAPHOOK_LIB}" \
    AGNOCAST_BRIDGE_MODE=off \
    AGNOCAST_NO_DISCOVERY_AGENT=1 \
    "$@" >"${log}" 2>&1 &
  disown $! 2>/dev/null || true
}

mkdir -p "${OUTPUT_DIR}"
wait_for_clean_state

if [[ "${BACKEND}" == "daemon" ]]; then
  restart_daemon
else
  require_backend kmod
fi

STATUS_FILE="${OUTPUT_DIR}/publisher_status.txt"
SYNC_DIR="${OUTPUT_DIR}/sync"
LOG_DIR="${OUTPUT_DIR}/logs"
mkdir -p "${LOG_DIR}" "${SYNC_DIR}"

bg_launch "${LOG_DIR}/publisher.log" "${PUB_BIN}" --ros-args \
  -p topic:="${TOPIC}" \
  -p qos_depth:="${QOS_DEPTH}" \
  -p status_file:="${STATUS_FILE}" \
  -p sync_dir:="${SYNC_DIR}"
PUB_PID=$!
PIDS+=("${PUB_PID}")

SUB_PIDS=()
READY_FILES=("${SYNC_DIR}/pub.ready")
for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do
  role="holder"
  if [[ "${CRASH_ROLE}" == "sub" && "${idx}" -eq 0 ]]; then
    role="victim"
  fi
  bg_launch "${LOG_DIR}/subscriber_${idx}.log" \
    CONSISTENCY_SUB_INDEX="${idx}" "${SUB_BIN}" --ros-args \
    -p topic:="${TOPIC}" \
    -p qos_depth:="${QOS_DEPTH}" \
    -p role:="${role}" \
    -p subscriber_index:="${idx}" \
    -p sync_dir:="${SYNC_DIR}" \
    -p debug_status_file:="${OUTPUT_DIR}/sub_${idx}_debug.txt"
  SUB_PIDS+=("$!")
  PIDS+=("$!")
  READY_FILES+=("${SYNC_DIR}/sub_${idx}.ready")
done

if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${READY_FILES[@]}"; then
  die "pub/sub did not become ready. See ${LOG_DIR}"
fi

if ! launch_env "${OBS_BIN}" --ros-args \
  -p topic:="${TOPIC}" \
  -p wait_publishers:=1 \
  -p wait_subscribers:="${NUM_SUBSCRIBERS}" \
  -p wait_timeout_sec:="${WAIT_TIMEOUT_SEC}" \
  -p output:="${OUTPUT_DIR}/ready.json" \
  >"${LOG_DIR}/observer_wait.log" 2>&1; then
  die "topology did not reach 1 publisher / ${NUM_SUBSCRIBERS} subscribers. See ${LOG_DIR}/observer_wait.log"
fi

touch "${SYNC_DIR}/START"

sleep "${WARMUP_SEC}"

if [[ "${CRASH_ROLE}" == "pub" ]]; then
  VICTIM_PID="${PUB_PID}"
else
  VICTIM_PID="${SUB_PIDS[0]}"
fi

if ! kill -0 "${VICTIM_PID}" 2>/dev/null; then
  die "victim pid ${VICTIM_PID} already exited before the injected signal"
fi
if [[ ! -r "/proc/${VICTIM_PID}/cmdline" ]] ||
  ! tr '\0' ' ' <"/proc/${VICTIM_PID}/cmdline" | grep -q 'consistency_'; then
  die "victim pid ${VICTIM_PID} is not a consistency node (got a wrapper shell?). cmdline: $(tr '\0' ' ' <"/proc/${VICTIM_PID}/cmdline" 2>/dev/null || true)"
fi

if [[ "${SIGNAL}" == "int" ]]; then
  kill -INT "${VICTIM_PID}" || true
  for _ in $(seq 1 50); do
    kill -0 "${VICTIM_PID}" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "${VICTIM_PID}" 2>/dev/null; then
    die "victim pid ${VICTIM_PID} did not exit after SIGINT"
  fi
else
  kill -KILL "${VICTIM_PID}" || true
fi

# Drop the victim from the cleanup PID list; it is already gone.
NEW_PIDS=()
for pid in "${PIDS[@]}"; do
  [[ "${pid}" == "${VICTIM_PID}" ]] && continue
  NEW_PIDS+=("${pid}")
done
PIDS=("${NEW_PIDS[@]}")

if [[ "${CRASH_ROLE}" == "sub" ]]; then
  # Holders keep taking shared locks on receive/release. Pause them first so
  # process-exit cleanup can take unique_lock and erase the victim (that is
  # also when its entry ref bits are cleared). The publisher keeps publishing.
  touch "${SYNC_DIR}/SNAP"
  HOLDER_PAUSE_FILES=()
  for idx in $(seq 1 $((NUM_SUBSCRIBERS - 1))); do
    HOLDER_PAUSE_FILES+=("${SYNC_DIR}/sub_${idx}.paused")
  done
  if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${HOLDER_PAUSE_FILES[@]}"; then
    die "holders did not pause IPC. See ${LOG_DIR}"
  fi

  VICTIM_NODE="/consistency_sub_0"
  if ! launch_env "${OBS_BIN}" --ros-args \
    -p topic:="${TOPIC}" \
    -p wait_publishers:=1 \
    -p wait_subscribers:="$((NUM_SUBSCRIBERS - 1))" \
    -p wait_absent_subscriber:="${VICTIM_NODE}" \
    -p wait_timeout_sec:="${WAIT_TIMEOUT_SEC}" \
    -p output:="${OUTPUT_DIR}/cleanup_seen.json" \
    >"${LOG_DIR}/observer_cleanup_wait.log" 2>&1; then
    die "victim ${VICTIM_NODE} did not leave membership (cleanup did not finish). See ${LOG_DIR}/observer_cleanup_wait.log"
  fi

  # Pausing holders pins their last messages; those age out of qos_depth while
  # the publisher continues. Drop last_ only (keep the subscription) so KeepLast
  # can reclaim everything except a true leftover victim ref.
  touch "${SYNC_DIR}/RELEASE_HELD"
  RELEASE_FILES=()
  for idx in $(seq 1 $((NUM_SUBSCRIBERS - 1))); do
    RELEASE_FILES+=("${SYNC_DIR}/sub_${idx}.released")
  done
  if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${RELEASE_FILES[@]}"; then
    die "holders did not release pinned messages. See ${LOG_DIR}"
  fi

  # Bits are cleared, but KeepLast GC still needs publishes (max 3/call).
  # Wait until the live counter is back in range, or time out and sample anyway
  # (that leftover is a real leak).
  drain_start="$(date +%s)"
  while ! outstanding_in_range "${QOS_DEPTH}" "$((QOS_DEPTH + 1))"; do
    if (( "$(date +%s)" - drain_start >= WAIT_TIMEOUT_SEC )); then
      echo "WARNING: outstanding did not return to ${QOS_DEPTH}..$((QOS_DEPTH + 1)) after cleanup" >&2
      break
    fi
    sleep 0.01
  done

  # Sample outstanding only after cleanup: leftover victim bits are gone, and
  # the still-running publisher can release unreferenced entries (max 3/publish).
  touch "${SYNC_DIR}/FLUSH"
  if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${SYNC_DIR}/pub.flushed"; then
    die "publisher did not flush outstanding. See ${LOG_DIR}"
  fi
  if [[ -f "${STATUS_FILE}" ]]; then
    cp "${STATUS_FILE}" "${OUTPUT_DIR}/outstanding.txt"
  else
    die "publisher status file missing after subscriber crash"
  fi

  touch "${SYNC_DIR}/PAUSE"
  if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${SYNC_DIR}/pub.paused"; then
    die "publisher did not pause IPC. See ${LOG_DIR}"
  fi
else
  sleep "${CLEANUP_WAIT_SEC}"
  touch "${SYNC_DIR}/PAUSE"
  touch "${SYNC_DIR}/SNAP"
  PAUSE_FILES=()
  for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do
    PAUSE_FILES+=("${SYNC_DIR}/sub_${idx}.paused")
  done
  if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${PAUSE_FILES[@]}"; then
    die "survivors did not pause IPC. See ${LOG_DIR}"
  fi
fi

if ! launch_env "${OBS_BIN}" --ros-args \
  -p topic:="${TOPIC}" \
  -p output:="${OUTPUT_DIR}/snapshot.json" \
  >"${LOG_DIR}/observer_snapshot.log" 2>&1; then
  die "post-cleanup snapshot failed. See ${LOG_DIR}/observer_snapshot.log"
fi

if [[ "${CRASH_ROLE}" == "pub" ]]; then
  # Holders still pin orphaned messages, so the dead publisher remaining in
  # membership is the correct answer. Leak is whether that record survives
  # after every live subscriber drops its last ipc_shared_ptr and unsubscribes
  # (remove_subscriber runs the exited-publisher entry GC).
  touch "${SYNC_DIR}/DROP"
  DROP_FILES=()
  for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do
    DROP_FILES+=("${SYNC_DIR}/sub_${idx}.dropped")
  done
  if ! wait_for_files "${WAIT_TIMEOUT_SEC}" "${DROP_FILES[@]}"; then
    die "holders did not drop refs. See ${LOG_DIR}"
  fi
  if ! launch_env "${OBS_BIN}" --ros-args \
    -p topic:="${TOPIC}" \
    -p output:="${OUTPUT_DIR}/after_drop.json" \
    >"${LOG_DIR}/observer_after_drop.log" 2>&1; then
    die "after-drop snapshot failed. See ${LOG_DIR}/observer_after_drop.log"
  fi
fi

{
  echo "backend=${BACKEND}"
  echo "crash_role=${CRASH_ROLE}"
  echo "signal=${SIGNAL}"
  echo "qos_depth=${QOS_DEPTH}"
  echo "num_subscribers=${NUM_SUBSCRIBERS}"
  echo "victim_pid=${VICTIM_PID}"
} >"${OUTPUT_DIR}/meta.txt"

echo "trial written to ${OUTPUT_DIR}"
