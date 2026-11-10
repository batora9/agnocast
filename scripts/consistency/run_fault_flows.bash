#!/usr/bin/env bash
set -euo pipefail
#
# Run one trial of a failure the client-side data plane must recover from
# (user daemon backend only).
#
#   scripts/consistency/run_fault_flows.bash --scenario stall --output-dir /tmp/trial
#
# Topology: 1 publisher (tight loop) + N holder subscribers (executor, keep the
# latest message), plus what the scenario adds. After --warmup-sec the
# scenario's failure happens; then the flow is measured for --post-sec, the
# topic is quiesced and the daemon checks the segment and its registry.
#
# Scenarios (what fails  →  who must notice  →  what must follow):
#   client_kill        a holder is SIGKILLed outside any segment operation
#                      → the daemon (socket EOF / pidfd) removes it
#   crash_publish      a second publisher dies inside a publish, holding the
#                      segment's mutex → the next locker rolls it back
#   crash_receive      a subscriber dies inside a receive, holding the mutex
#   crash_release      a subscriber dies inside a release, holding the mutex
#   stall              a second publisher stops (SIGSTOP) inside a publish,
#                      holding the mutex → waiters time out and report, the
#                      daemon kills the holder, the next locker rolls back
#   bad_address        a second publisher publishes an address outside its pool
#                      → subscribers skip it and report, the daemon quarantines
#                      the writer and rebuilds the segment
#   scribble           a second publisher writes garbage over the segment
#                      without the mutex → the next locker's check fails, the
#                      daemon rebuilds the segment and moves the topic to the
#                      daemon path
#   scribble_protected the same with AGNOCAST_SHM_PROTECT=1 → the stray write
#                      faults in the writer (needs CPU protection keys;
#                      "unsupported" otherwise)
#   reclaim            the daemon is killed, then a subscriber holding most of
#                      the segment's slots dies → the publisher drops its
#                      references itself while the daemon is still down
#   fork_exit          a subscriber that forked a child (which keeps the daemon
#                      socket open) is SIGKILLed → the daemon notices through
#                      the process's pidfd, not the socket
#
# The daemon-crash flows (the daemon dies, possibly inside a segment
# operation) are run by run_daemon_crash.bash.
#
# Options:
#   --daemon-env VAR=VALUE   extra environment for the daemon (repeatable),
#                            e.g. AGNOCAST_DAEMON_PIDFD=0 or
#                            AGNOCAST_STALL_ACTION=report for a baseline
#   --client-env VAR=VALUE   extra environment for every client (repeatable)
#
# Recorded per trial: metrics.json (flow, membership, checks per scenario),
# snapshot.json (the daemon's invariant check), stats/*.json (each client's
# data-plane counters), logs/.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

cons_bin() {
  echo "$(install_dir daemon)/agnocast_consistency/lib/agnocast_consistency/consistency_$1"
}

SCENARIO=""
OUTPUT_DIR=""
TOPIC="/consistency"
QOS_DEPTH=10
NUM_SUBSCRIBERS=2
WARMUP_SEC="0.5"
POST_SEC="1.0"
WAIT_TIMEOUT_SEC=20
FAULT_HIT=2000
HOARD_NUM=3700
OUTAGE_MS=1500
LOCK_TIMEOUT_MS=300
DAEMON_ENV=()
CLIENT_ENV=()

while [[ $# -gt 0 ]]; do
  case "$1" in
  --scenario) SCENARIO="$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  --topic) TOPIC="$2"; shift 2 ;;
  --qos-depth) QOS_DEPTH="$2"; shift 2 ;;
  --num-subscribers) NUM_SUBSCRIBERS="$2"; shift 2 ;;
  --warmup-sec) WARMUP_SEC="$2"; shift 2 ;;
  --post-sec) POST_SEC="$2"; shift 2 ;;
  --wait-timeout-sec) WAIT_TIMEOUT_SEC="$2"; shift 2 ;;
  --fault-hit) FAULT_HIT="$2"; shift 2 ;;
  --hoard-num) HOARD_NUM="$2"; shift 2 ;;
  --outage-ms) OUTAGE_MS="$2"; shift 2 ;;
  --lock-timeout-ms) LOCK_TIMEOUT_MS="$2"; shift 2 ;;
  --daemon-env) DAEMON_ENV+=("$2"); shift 2 ;;
  --client-env) CLIENT_ENV+=("$2"); shift 2 ;;
  -h | --help) sed -n '3,50p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done

SCENARIOS="client_kill crash_publish crash_receive crash_release stall bad_address scribble scribble_protected reclaim fork_exit"
[[ " ${SCENARIOS} " == *" ${SCENARIO} "* ]] || die "--scenario must be one of: ${SCENARIOS}"
[[ -n "${OUTPUT_DIR}" ]] || die "--output-dir is required"
[[ "${NUM_SUBSCRIBERS}" -ge 1 ]] || die "--num-subscribers must be >= 1"

setup="$(install_dir daemon)/setup.bash"
[[ -f "${setup}" ]] || die "daemon workspace not built. Run: scripts/consistency/build.bash daemon"
[[ -x "$(cons_bin rogue)" ]] || die "consistency_rogue missing. Run: scripts/consistency/build.bash daemon"
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
STATS_DIR="${OUTPUT_DIR}/stats"
STATUS_FILE="${OUTPUT_DIR}/publisher_status.txt"
rm -rf "${LOG_DIR}" "${SYNC_DIR}" "${STATS_DIR}"
mkdir -p "${LOG_DIR}" "${SYNC_DIR}" "${STATS_DIR}"

PIDS=()
EXTRA_PIDS=()
cleanup() {
  local pid
  for pid in "${PIDS[@]:-}" "${EXTRA_PIDS[@]:-}"; do
    [[ -n "${pid}" ]] && kill -KILL "${pid}" 2>/dev/null || true
  done
  stop_daemon
  pkill -KILL -x agnocast_daemon 2>/dev/null || true
}
trap cleanup EXIT

now_ms() { date +%s%3N; }
sec() { awk -v ms="$1" 'BEGIN { printf "%.3f", ms / 1000 }'; }

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

wait_for_exit() {
  local pid="$1" timeout_ms="$2" start
  start="$(now_ms)"
  while kill -0 "${pid}" 2>/dev/null; do
    (($(now_ms) - start >= timeout_ms)) && return 1
    sleep 0.01
  done
  return 0
}

wait_for_clean_state() {
  pkill -9 -f '[c]onsistency_(publisher|subscriber|observer|churn|rogue)' 2>/dev/null || true
  pkill -KILL -x agnocast_daemon 2>/dev/null || true
  for _ in $(seq 1 50); do
    pgrep -f '[c]onsistency_(publisher|subscriber|observer|churn|rogue)' >/dev/null 2>&1 || break
    sleep 0.1
  done
  rm -f /dev/shm/agnocast@* /dev/mqueue/agnocast@* 2>/dev/null || true
  rm -f "${AGNOCAST_DAEMON_SOCKET_PATH}" "${AGNOCAST_DAEMON_STATE_PATH}" 2>/dev/null || true
}

client_env() {
  echo LD_LIBRARY_PATH="${CONS_LD_LIBRARY_PATH}" LD_PRELOAD="${HEAPHOOK_LIB}" \
    AGNOCAST_BRIDGE_MODE=off AGNOCAST_NO_DISCOVERY_AGENT=1 \
    AGNOCAST_SHM_LOCK_TIMEOUT_MS="${LOCK_TIMEOUT_MS}"
}

# Started in the background and left as a child of this shell, so its exit
# status can be collected with `wait`.
launch() {
  local log="$1"
  shift
  # shellcheck disable=SC2046
  env $(client_env) "${CLIENT_ENV[@]}" "$@" >"${log}" 2>&1 &
  LAUNCHED_PID=$!
}

observer() {
  local log="$1"
  shift
  # shellcheck disable=SC2046
  env $(client_env) "${CLIENT_ENV[@]}" \
    "$(cons_bin observer)" --ros-args -p topic:="${TOPIC}" \
    -p wait_timeout_sec:="${WAIT_TIMEOUT_SEC}" "$@" >"${log}" 2>&1
}

launch_daemon() {
  rm -f "${AGNOCAST_DAEMON_SOCKET_PATH}"
  BENCH_DAEMON_PID=""
  env "${DAEMON_ENV[@]}" "$@" "${AGNOCAST_DAEMON_BIN}" >>"${LOG_DIR}/agnocast_daemon.log" 2>&1 &
  BENCH_DAEMON_PID=$!
  DAEMON_PID="${BENCH_DAEMON_PID}"
  for _ in $(seq 1 100); do
    daemon_running && return 0
    kill -0 "${BENCH_DAEMON_PID}" 2>/dev/null || die "agnocast_daemon exited during startup"
    sleep 0.05
  done
  die "agnocast_daemon did not create its socket"
}

# "<published> <sub_0> <sub_1> ..." for the holders still running.
ACTIVE_HOLDERS="${NUM_SUBSCRIBERS}"
sample_counters() {
  local pub=0 c idx
  if [[ -f "${STATUS_FILE}" ]]; then read -r pub _ <"${STATUS_FILE}" || pub=0; fi
  c="${pub}"
  for idx in $(seq 0 $((ACTIVE_HOLDERS - 1))); do
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

# ---- topology ------------------------------------------------------------------
wait_for_clean_state
launch_daemon
FIRST_DAEMON_PID="${DAEMON_PID}"

if [[ "${SCENARIO}" == "scribble_protected" ]]; then
  CLIENT_ENV+=(AGNOCAST_SHM_PROTECT=1)
fi

launch "${LOG_DIR}/publisher.log" "$(cons_bin publisher)" --ros-args \
  -p topic:="${TOPIC}" -p qos_depth:="${QOS_DEPTH}" -p status_file:="${STATUS_FILE}" \
  -p sync_dir:="${SYNC_DIR}" -p stats_file:="${STATS_DIR}/publisher.json"
PUB_PID="${LAUNCHED_PID}"
PIDS+=("${PUB_PID}")

HOLDER_PIDS=()
READY_FILES=("${SYNC_DIR}/pub.ready")
for idx in $(seq 0 $((NUM_SUBSCRIBERS - 1))); do
  launch "${LOG_DIR}/subscriber_${idx}.log" CONSISTENCY_SUB_INDEX="${idx}" \
    "$(cons_bin subscriber)" --ros-args -p topic:="${TOPIC}" -p qos_depth:="${QOS_DEPTH}" \
    -p role:=holder -p subscriber_index:="${idx}" -p sync_dir:="${SYNC_DIR}" \
    -p status_file:="${OUTPUT_DIR}/sub_${idx}_status.txt" \
    -p stats_file:="${STATS_DIR}/sub_${idx}.json"
  HOLDER_PIDS+=("${LAUNCHED_PID}")
  PIDS+=("${LAUNCHED_PID}")
  READY_FILES+=("${SYNC_DIR}/sub_${idx}.ready")
done

# What the scenario adds (index NUM_SUBSCRIBERS for an extra subscriber).
EXTRA_IDX="${NUM_SUBSCRIBERS}"
EXTRA_PID=""
EXPECT_PUBS=1
EXPECT_SUBS="${NUM_SUBSCRIBERS}"
rogue() {
  launch "${LOG_DIR}/rogue.log" "$@" "$(cons_bin rogue)" --ros-args -p topic:="${TOPIC}" \
    -p qos_depth:="${QOS_DEPTH}" -p sync_dir:="${SYNC_DIR}" -p stats_file:="${STATS_DIR}/rogue.json" \
    -p mode:="${ROGUE_MODE}"
  EXTRA_PID="${LAUNCHED_PID}"
  EXTRA_PIDS+=("${EXTRA_PID}")
  READY_FILES+=("${SYNC_DIR}/rogue.ready")
}
extra_subscriber() {
  # $1: role; rest: extra parameters / environment handled by caller via ENV
  local role="$1"
  shift
  launch "${LOG_DIR}/subscriber_${EXTRA_IDX}.log" CONSISTENCY_SUB_INDEX="${EXTRA_IDX}" "${EXTRA_ENV[@]}" \
    "$(cons_bin subscriber)" --ros-args -p topic:="${TOPIC}" -p qos_depth:="${QOS_DEPTH}" \
    -p role:="${role}" -p subscriber_index:="${EXTRA_IDX}" -p sync_dir:="${SYNC_DIR}" \
    -p stats_file:="${STATS_DIR}/sub_${EXTRA_IDX}.json" "$@"
  EXTRA_PID="${LAUNCHED_PID}"
  EXTRA_PIDS+=("${EXTRA_PID}")
  READY_FILES+=("${SYNC_DIR}/sub_${EXTRA_IDX}.ready")
}

EXTRA_ENV=()
case "${SCENARIO}" in
crash_publish)
  ROGUE_MODE=publish
  rogue AGNOCAST_CLIENT_FAULT_POINT=shm.publish AGNOCAST_CLIENT_FAULT_HIT="${FAULT_HIT}"
  ;;
stall)
  ROGUE_MODE=publish
  rogue AGNOCAST_CLIENT_FAULT_POINT=shm.publish AGNOCAST_CLIENT_FAULT_HIT="${FAULT_HIT}" \
    AGNOCAST_CLIENT_FAULT_ACTION=stop
  ;;
bad_address | scribble | scribble_protected)
  ROGUE_MODE="${SCENARIO%_protected}"
  rogue AGNOCAST_ALLOW_DEBUG_FAULTS=1
  ;;
crash_receive | crash_release)
  EXTRA_ENV=(AGNOCAST_CLIENT_FAULT_POINT="shm.${SCENARIO#crash_}"
    AGNOCAST_CLIENT_FAULT_HIT="${FAULT_HIT}")
  extra_subscriber victim
  ;;
reclaim)
  extra_subscriber holder -p hoard_num:="${HOARD_NUM}" \
    -p status_file:="${OUTPUT_DIR}/sub_${EXTRA_IDX}_status.txt"
  ;;
fork_exit)
  extra_subscriber holder -p fork_child:=true \
    -p status_file:="${OUTPUT_DIR}/sub_${EXTRA_IDX}_status.txt"
  ;;
esac
INITIAL_SUBS="${NUM_SUBSCRIBERS}"
INITIAL_PUBS=1
case "${SCENARIO}" in
crash_receive | crash_release | reclaim | fork_exit) INITIAL_SUBS=$((NUM_SUBSCRIBERS + 1)) ;;
crash_publish | stall | bad_address | scribble | scribble_protected) INITIAL_PUBS=2 ;;
esac
[[ "${SCENARIO}" == "client_kill" ]] && EXPECT_SUBS=$((NUM_SUBSCRIBERS - 1))

wait_for_files "${WAIT_TIMEOUT_SEC}" "${READY_FILES[@]}" || die "clients did not become ready. See ${LOG_DIR}"
observer "${LOG_DIR}/observer_wait.log" -p wait_publishers:="${INITIAL_PUBS}" \
  -p wait_subscribers:="${INITIAL_SUBS}" -p output:="${OUTPUT_DIR}/ready.json" ||
  die "topology did not come up. See ${LOG_DIR}/observer_wait.log"

touch "${SYNC_DIR}/START"
sleep "${WARMUP_SEC}"

# ---- the failure -------------------------------------------------------------------
C0="$(sample_counters)"
T_EVENT="$(now_ms)"
EVENT_MS=-1          # from the failure until the scenario's own recovery signal
EXTRA_EXIT=""        # exit status of the scenario's extra process, if it ended
OUTAGE_PUBLISHED=-1
PUB_RECLAIMS_DURING_OUTAGE=-1
CHILD_PID=""
SUPPORTED=1

json_field() { python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get(sys.argv[2], -1))" "$1" "$2" 2>/dev/null || echo -1; }

case "${SCENARIO}" in
client_kill)
  victim="${HOLDER_PIDS[$((NUM_SUBSCRIBERS - 1))]}"
  kill -KILL "${victim}"
  wait "${victim}" 2>/dev/null || true
  HOLDER_PIDS=("${HOLDER_PIDS[@]:0:$((NUM_SUBSCRIBERS - 1))}")
  ACTIVE_HOLDERS=$((NUM_SUBSCRIBERS - 1))
  if observer "${LOG_DIR}/observer_event.log" -p wait_subscribers:="${EXPECT_SUBS}" \
    -p output:="${OUTPUT_DIR}/after_event.json"; then
    EVENT_MS=$(($(now_ms) - T_EVENT))
  else
    fail_step "the killed subscriber was not removed"
  fi
  ;;
crash_publish | crash_receive | crash_release | stall)
  # The extra process reaches its fault point on its own.
  if wait_for_exit "${EXTRA_PID}" $((WAIT_TIMEOUT_SEC * 1000)); then
    EVENT_MS=$(($(now_ms) - T_EVENT))
    status=0
    wait "${EXTRA_PID}" 2>/dev/null || status=$?
    EXTRA_EXIT="${status}"
  else
    fail_step "the faulting client is still running"
    # A stall nobody resolved (e.g. AGNOCAST_STALL_ACTION=report): let the
    # holder go on, or nothing below could finish.
    kill -CONT "${EXTRA_PID}" 2>/dev/null || true
  fi
  # A dead publisher stays listed while entries of its are referenced (as
  # with the kernel module), so only the subscribers are waited for.
  observer "${LOG_DIR}/observer_event.log" -p wait_subscribers:="${EXPECT_SUBS}" \
    -p output:="${OUTPUT_DIR}/after_event.json" || fail_step "the dead client was not removed"
  ;;
bad_address | scribble | scribble_protected)
  wait_for_files "${WAIT_TIMEOUT_SEC}" "${SYNC_DIR}/rogue.injecting" || fail_step "rogue did not inject"
  T_EVENT="$(now_ms)"
  if [[ "${SCENARIO}" == "scribble_protected" ]]; then
    if wait_for_exit "${EXTRA_PID}" 2000; then
      status=0
      wait "${EXTRA_PID}" 2>/dev/null || status=$?
      EXTRA_EXIT="${status}"
      EVENT_MS=$(($(now_ms) - T_EVENT))
    else
      protected="$(json_field "${STATS_DIR}/rogue.json" shm_protected)"
      [[ "${protected}" != "True" ]] && SUPPORTED=0
    fi
  fi
  # Wait until the daemon has acted (a new epoch) or give up after the timeout.
  if [[ "${SCENARIO}" != "scribble_protected" ]]; then
    start="$(now_ms)"
    while (($(now_ms) - start < WAIT_TIMEOUT_SEC * 1000)); do
      observer "${LOG_DIR}/observer_event.log" -p check_invariants:=true \
        -p output:="${OUTPUT_DIR}/after_event.json" || true
      epoch="$(python3 -c "import json; print(json.load(open('${OUTPUT_DIR}/after_event.json'))['check'].get('epoch', 0))" 2>/dev/null || echo 0)"
      if [[ "${epoch}" -ge 1 ]]; then
        EVENT_MS=$(($(now_ms) - T_EVENT))
        break
      fi
      sleep 0.05
    done
    [[ "${EVENT_MS}" -ge 0 ]] || fail_step "the daemon did not replace the segment"
  fi
  ;;
reclaim)
  wait_for_files "${WAIT_TIMEOUT_SEC}" "${SYNC_DIR}/sub_${EXTRA_IDX}.hoarded" ||
    fail_step "the hoarder did not fill up"
  kill -KILL "${FIRST_DAEMON_PID}" 2>/dev/null || true
  wait "${FIRST_DAEMON_PID}" 2>/dev/null || true
  BENCH_DAEMON_PID=""
  T_EVENT="$(now_ms)"
  C0="$(sample_counters)"
  kill -KILL "${EXTRA_PID}"
  wait "${EXTRA_PID}" 2>/dev/null || true
  sleep "$(sec "${OUTAGE_MS}")"
  read -r OUTAGE_END_PUB _ <"${STATUS_FILE}" || OUTAGE_END_PUB=0
  OUTAGE_PUBLISHED=$((OUTAGE_END_PUB - ${C0%% *}))
  # The publisher writes its counters every 1024 publishes.
  PUB_RECLAIMS_DURING_OUTAGE="$(json_field "${STATS_DIR}/publisher.json" peer_reclaims)"
  launch_daemon
  observer "${LOG_DIR}/observer_recovery.log" -p wait_recovery_done:=true \
    -p recovery_output:="${OUTPUT_DIR}/recovery.json" || fail_step "recovery did not finish"
  observer "${LOG_DIR}/observer_event.log" -p wait_publishers:=1 \
    -p wait_subscribers:="${EXPECT_SUBS}" -p output:="${OUTPUT_DIR}/after_event.json" ||
    fail_step "membership did not come back after the restart"
  EVENT_MS=$(($(now_ms) - T_EVENT))
  ;;
fork_exit)
  CHILD_PID="$(cat "${SYNC_DIR}/sub_${EXTRA_IDX}.child" 2>/dev/null || true)"
  [[ -n "${CHILD_PID}" ]] || fail_step "the subscriber did not fork"
  kill -KILL "${EXTRA_PID}"
  wait "${EXTRA_PID}" 2>/dev/null || true
  if observer "${LOG_DIR}/observer_event.log" -p wait_subscribers:="${EXPECT_SUBS}" \
    -p wait_timeout_sec:=3 -p output:="${OUTPUT_DIR}/after_event.json"; then
    EVENT_MS=$(($(now_ms) - T_EVENT))
  else
    fail_step "the dead subscriber was not removed while its child lived"
  fi
  CHILD_ALIVE=0
  [[ -n "${CHILD_PID}" ]] && kill -0 "${CHILD_PID}" 2>/dev/null && CHILD_ALIVE=1
  [[ -n "${CHILD_PID}" ]] && kill -KILL "${CHILD_PID}" 2>/dev/null || true
  ;;
esac

C1="$(sample_counters)"
sleep "${POST_SEC}"
C2="$(sample_counters)"

# ---- quiesce and check (same procedure as run_daemon_crash.bash) -----------------
for pid in "${EXTRA_PIDS[@]}"; do
  if kill -0 "${pid}" 2>/dev/null; then
    kill -CONT "${pid}" 2>/dev/null || true
    kill -INT "${pid}" 2>/dev/null || true
    wait_for_exit "${pid}" 3000 || kill -KILL "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
done
touch "${SYNC_DIR}/SNAP"
HOLDER_PAUSE=()
RELEASED=()
for idx in $(seq 0 $((${#HOLDER_PIDS[@]} - 1))); do
  HOLDER_PAUSE+=("${SYNC_DIR}/sub_${idx}.paused")
  RELEASED+=("${SYNC_DIR}/sub_${idx}.released")
done
wait_for_files "${WAIT_TIMEOUT_SEC}" "${HOLDER_PAUSE[@]}" || fail_step "holders did not pause"
touch "${SYNC_DIR}/RELEASE_HELD"
wait_for_files "${WAIT_TIMEOUT_SEC}" "${RELEASED[@]}" || fail_step "holders did not release"
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
# Publishers: the live ones, plus a dead one that may stay listed until its
# entries go (crash_publish, stall, scribble_protected).
FINAL_PUBS_MIN="${EXPECT_PUBS}"
FINAL_PUBS_MAX="${EXPECT_PUBS}"
case "${SCENARIO}" in
crash_publish | stall | scribble_protected) FINAL_PUBS_MAX=2 ;;
esac
FINAL_SUBS="${#HOLDER_PIDS[@]}"
observer "${LOG_DIR}/observer_snapshot.log" -p check_invariants:=true \
  -p wait_publishers:="$([[ "${FINAL_PUBS_MIN}" == "${FINAL_PUBS_MAX}" ]] && echo "${FINAL_PUBS_MIN}" || echo 0)" \
  -p wait_subscribers:="${FINAL_SUBS}" \
  -p output:="${OUTPUT_DIR}/snapshot.json" || fail_step "snapshot failed"

SURVIVORS=0
for pid in "${PUB_PID}" "${HOLDER_PIDS[@]}"; do
  kill -0 "${pid}" 2>/dev/null && SURVIVORS=$((SURVIVORS + 1))
done
FAULT_LINE="$(cat "${LOG_DIR}"/*.log 2>/dev/null | grep -m1 "fault injection" || true)"

python3 - "${OUTPUT_DIR}" <<EOF
import glob, json, os, sys
out = sys.argv[1]
def load(p, default=None):
    try:
        return json.load(open(p))
    except Exception:
        return default
c0 = [int(x) for x in "${C0}".split()]
c1 = [int(x) for x in "${C1}".split()]
c2 = [int(x) for x in "${C2}".split()]
snap = load(os.path.join(out, "snapshot.json"), {}) or {}
check = snap.get("check", {}) or {}
stats = {os.path.basename(p)[:-5]: load(p, {}) for p in glob.glob(os.path.join(out, "stats", "*.json"))}
outstanding = None
try:
    outstanding = int(open(os.path.join(out, "outstanding.txt")).read().split()[2])
except Exception:
    pass
holders = len(c2) - 1
post_received_min = min((b - a for a, b in zip(c1[1:], c2[1:])), default=0)
scenario = "${SCENARIO}"
extra_exit = "${EXTRA_EXIT}"
m = {
    "scenario": scenario,
    "supported": bool(${SUPPORTED}),
    "fault_hit": ${FAULT_HIT},
    "fault_line": """${FAULT_LINE}""",
    "event_ms": ${EVENT_MS},
    "extra_exit": int(extra_exit) if extra_exit else None,
    "counters_before": c0, "counters_after_event": c1, "counters_end": c2,
    "event_published": c1[0] - c0[0],
    "post_published": c2[0] - c1[0],
    "post_received_min": post_received_min,
    "outage_published": ${OUTAGE_PUBLISHED},
    "publisher_reclaims_during_outage": ${PUB_RECLAIMS_DURING_OUTAGE},
    "survivors": ${SURVIVORS},
    "expected_survivors": 1 + holders,
    "publisher_num": snap.get("publisher_num"),
    "subscriber_num": snap.get("subscriber_num"),
    "expected_publisher_num": [${FINAL_PUBS_MIN}, ${FINAL_PUBS_MAX}],
    "expected_subscriber_num": ${FINAL_SUBS},
    "outstanding": outstanding,
    "qos_depth": ${QOS_DEPTH},
    "check": check,
    "stats": stats,
    "child_alive_at_detection": bool(${CHILD_ALIVE:-0}),
    "daemon_env": """${DAEMON_ENV[*]:-}""",
    "client_env": """${CLIENT_ENV[*]:-}""",
    "failed_step": "${FAILED_STEP}",
}
violations = (check.get("shm_violations", 1) or 0) + (check.get("registry_violations", 1) or 0)
base = {
    "flow": m["post_published"] > 0 and post_received_min > 0,
    "consistent": bool(check.get("ok")) and violations == 0,
    "membership": m["publisher_num"] is not None
                  and ${FINAL_PUBS_MIN} <= m["publisher_num"] <= ${FINAL_PUBS_MAX}
                  and m["subscriber_num"] == m["expected_subscriber_num"],
    "leak_free": outstanding is not None and ${QOS_DEPTH} <= outstanding <= ${QOS_DEPTH} + 1,
    "survived": m["survivors"] == m["expected_survivors"],
}
s = {}
sum_stat = lambda k: sum((v or {}).get(k, 0) for v in stats.values())
if scenario == "client_kill":
    s["removed"] = m["event_ms"] >= 0
elif scenario in ("crash_publish", "crash_receive", "crash_release"):
    s["fault_fired"] = "killed at" in m["fault_line"]
    s["rolled_back"] = check.get("rollback_count", 0) >= 1
elif scenario == "stall":
    s["fault_fired"] = "stopped at" in m["fault_line"]
    s["holder_killed"] = check.get("stall_kill_num", 0) >= 1 and m["extra_exit"] == 137
    s["rolled_back"] = check.get("rollback_count", 0) >= 1
elif scenario == "bad_address":
    s["skipped_by_readers"] = sum_stat("bad_entries") >= 1
    s["quarantined"] = check.get("quarantined_num", 0) >= 1
    s["rebuilt"] = check.get("rotation_num", 0) >= 1 and check.get("mode", 1) == 0
elif scenario == "scribble":
    s["rebuilt"] = check.get("rotation_num", 0) >= 1
    s["daemon_path"] = check.get("mode", 0) == 1 and stats.get("publisher", {}).get("daemon_path_calls", 0) > 0
elif scenario == "scribble_protected":
    s["writer_faulted"] = m["extra_exit"] == 139
    s["segment_untouched"] = check.get("rotation_num", 1) == 0
elif scenario == "reclaim":
    s["reclaimed_while_down"] = m["publisher_reclaims_during_outage"] >= 1
    s["published_while_down"] = m["outage_published"] > 0
elif scenario == "fork_exit":
    s["removed"] = m["event_ms"] >= 0
    s["child_alive"] = m["child_alive_at_detection"]
m["checks"] = {**base, **s}
m["passed"] = m["supported"] and all(m["checks"].values()) and not m["failed_step"]
json.dump(m, open(os.path.join(out, "metrics.json"), "w"), indent=2)
print(("PASS" if m["passed"] else ("UNSUPPORTED" if not m["supported"] else "FAIL")) + f" {scenario}: "
      + ", ".join(f"{k}={'ok' if v else 'NG'}" for k, v in m["checks"].items())
      + f" event_ms={m['event_ms']}")
EOF

echo "trial written to ${OUTPUT_DIR}${FAILED_STEP:+ (failed: ${FAILED_STEP})}"
