#!/usr/bin/env bash
set -euo pipefail
#
# Paper §VII TABLE IV sweeps (A/B/C) for both backends, with the host locked
# into the reproducible RT environment. Needs sudo (insmod, chrt, prep_repro_env).
#
#   source /opt/ros/humble/setup.bash
#   scripts/bench/build.bash
#   scripts/bench/run_paper_sweeps.bash
#   scripts/bench/run_paper_sweeps.bash --daemon-rt-priority 80
#
# Skip C-state lock (Turbo off / freq pin / mqueue limits still applied):
#   scripts/bench/run_paper_sweeps.bash --no-cstate
#
# Skip all prep (powersave governor, deep C-states, stock mqueue limits):
#   scripts/bench/run_paper_sweeps.bash --no-prep
#
# Daemon-only (reuse a previous kmod tree for plots):
#   scripts/bench/run_paper_sweeps.bash --daemon-rt-priority 80 --skip-kmod \
#     --kmod-data results/paper_sweeps/kmod

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.bash
source "${SCRIPT_DIR}/common.bash"

OUTPUT_DIR=""
SKIP_PREP=0
NO_CSTATE=0
DAEMON_RT_PRIORITY=0
SKIP_KMOD=0
KMOD_DATA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --no-prep) SKIP_PREP=1; shift ;;
  --no-cstate) NO_CSTATE=1; shift ;;
  --daemon-rt-priority) DAEMON_RT_PRIORITY="$2"; shift 2 ;;
  --skip-kmod) SKIP_KMOD=1; shift ;;
  --kmod-data) KMOD_DATA="$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  -h | --help)
    sed -n '3,24p' "${BASH_SOURCE[0]}"
    exit 0
    ;;
  *)
    OUTPUT_DIR="$1"
    shift
    ;;
  esac
done

if [[ -z "${OUTPUT_DIR}" ]]; then
  if [[ "${DAEMON_RT_PRIORITY}" -gt 0 ]]; then
    OUTPUT_DIR="${BENCH_ROOT}/results/paper_sweeps_fifo${DAEMON_RT_PRIORITY}"
  else
    OUTPUT_DIR="${BENCH_ROOT}/results/paper_sweeps"
  fi
fi

[[ -n "${ROS_DISTRO:-}" ]] || die "ROS is not sourced. Run: source /opt/ros/humble/setup.bash"

if ! lsmod | grep -q '^agnocast '; then
  echo "==> Loading agnocast.ko"
  sudo insmod "${AGNOCAST_ROOT}/agnocast_kmod/agnocast.ko"
fi

PREP_RAN=0
if [[ "${SKIP_PREP}" -eq 0 ]]; then
  echo "==> Locking measurement environment"
  PREP_ARGS=()
  [[ "${NO_CSTATE}" -eq 1 ]] && PREP_ARGS+=(--no-cstate)
  sudo bash "${SCRIPT_DIR}/prep_repro_env.sh" "${PREP_ARGS[@]}"
  PREP_RAN=1
else
  echo "==> Skipping prep_repro_env (--no-prep)"
fi

cleanup() {
  if [[ "${PREP_RAN}" -eq 1 ]]; then
    echo "==> Restoring environment"
    sudo bash "${SCRIPT_DIR}/prep_repro_env.sh" --restore || true
  fi
}
trap cleanup EXIT

COMPARE_ARGS=(
  --sweeps a,b,c
  --iterations 5
  --warmup 2
  --output-dir "${OUTPUT_DIR}"
)
if [[ "${SKIP_KMOD}" -eq 1 ]]; then
  COMPARE_ARGS+=(--skip-kmod)
  if [[ -z "${KMOD_DATA}" && -d "${BENCH_ROOT}/results/paper_sweeps/kmod" ]]; then
    KMOD_DATA="${BENCH_ROOT}/results/paper_sweeps/kmod"
  fi
  [[ -n "${KMOD_DATA}" ]] && COMPARE_ARGS+=(--kmod-data "${KMOD_DATA}")
fi
if [[ "${DAEMON_RT_PRIORITY}" -gt 0 ]]; then
  COMPARE_ARGS+=(--daemon-rt-priority "${DAEMON_RT_PRIORITY}")
fi

bash "${SCRIPT_DIR}/compare.bash" "${COMPARE_ARGS[@]}"

echo "==> IPC breakdown (representative configs, if present)"
BREAKDOWN_KMOD="${OUTPUT_DIR}/kmod"
[[ -d "${BREAKDOWN_KMOD}" ]] || BREAKDOWN_KMOD="${KMOD_DATA:-}"
python3 "${SCRIPT_DIR}/breakdown.py" \
  ${BREAKDOWN_KMOD:+"${BREAKDOWN_KMOD}"} "${OUTPUT_DIR}/daemon" || true
