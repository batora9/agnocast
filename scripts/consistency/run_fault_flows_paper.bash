#!/usr/bin/env bash
set -euo pipefail
#
# Paper sweep for "the client-side data plane recovers from failures, relying
# on the daemon" (user daemon backend).
#
#   source /opt/ros/humble/setup.bash
#   scripts/consistency/build.bash daemon
#   scripts/consistency/run_fault_flows_paper.bash --output-dir results/fault_flows
#
# Cells (each --n trials of run_fault_flows.bash):
#   <scenario>              every scenario of run_fault_flows.bash; the crash
#                           and stall points are hit at a random publish/receive
#                           (--fault-hit drawn from [500, 5000])
#   <scenario>__baseline    the same failure with the mechanism under test
#                           turned off (--n-baseline trials), to show that the
#                           check catches its absence:
#                             fork_exit  AGNOCAST_DAEMON_PIDFD=0   (socket EOF only)
#                             stall      AGNOCAST_STALL_ACTION=report
#                             reclaim    AGNOCAST_PEER_RECLAIM=0
#
# The daemon-crash flows are swept by run_daemon_crash_paper.bash.
#
# Then: scripts/consistency/summarize_fault_flows.py <output-dir>
#
# Smoke run:
#   scripts/consistency/run_fault_flows_paper.bash --n 1 --n-baseline 1 \
#     --output-dir results/fault_flows_smoke

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

N=20
N_BASELINE=5
OUTPUT_DIR="${BENCH_ROOT}/results/fault_flows"
NUM_SUBSCRIBERS=2
SCENARIOS=(client_kill crash_publish crash_receive crash_release stall bad_address scribble
  scribble_protected reclaim fork_exit)

while [[ $# -gt 0 ]]; do
  case "$1" in
  --n) N="$2"; shift 2 ;;
  --n-baseline) N_BASELINE="$2"; shift 2 ;;
  --scenarios) IFS=',' read -r -a SCENARIOS <<<"$2"; shift 2 ;;
  --num-subscribers) NUM_SUBSCRIBERS="$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  -h | --help) sed -n '3,30p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
RUN="${SCRIPT_DIR}/run_fault_flows.bash"

trial() {
  # $1: cell name, $2: trial index, rest: options for run_fault_flows.bash
  local cell="$1" i="$2"
  shift 2
  local dir
  dir="${OUTPUT_DIR}/${cell}/trial_$(printf '%03d' "${i}")"
  if [[ -f "${dir}/metrics.json" ]]; then
    echo "skip ${cell} #${i} (done)"
    return 0
  fi
  echo "==> ${cell} #${i}"
  "${RUN}" --num-subscribers "${NUM_SUBSCRIBERS}" --output-dir "${dir}" "$@" 2>&1 |
    grep -E "^(PASS|FAIL|UNSUPPORTED) " || echo "    (no result; see ${dir}/logs)"
}

random_hit() { echo $((500 + RANDOM % 4501)); }

baseline_env() {
  case "$1" in
  fork_exit) echo "--daemon-env AGNOCAST_DAEMON_PIDFD=0" ;;
  stall) echo "--daemon-env AGNOCAST_STALL_ACTION=report --wait-timeout-sec 5" ;;
  reclaim) echo "--client-env AGNOCAST_PEER_RECLAIM=0" ;;
  *) echo "" ;;
  esac
}

for scenario in "${SCENARIOS[@]}"; do
  for i in $(seq 1 "${N}"); do
    trial "${scenario}" "${i}" --scenario "${scenario}" --fault-hit "$(random_hit)"
  done
  base="$(baseline_env "${scenario}")"
  if [[ -n "${base}" ]]; then
    for i in $(seq 1 "${N_BASELINE}"); do
      # shellcheck disable=SC2086
      trial "${scenario}__baseline" "${i}" --scenario "${scenario}" --fault-hit "$(random_hit)" ${base}
    done
  fi
done

python3 "${SCRIPT_DIR}/summarize_fault_flows.py" "${OUTPUT_DIR}"
