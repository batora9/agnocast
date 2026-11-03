#!/usr/bin/env bash
set -euo pipefail
#
# Paper sweep for "rollback after a daemon crash" (user daemon backend).
#
#   source /opt/ros/humble/setup.bash
#   scripts/consistency/build.bash daemon
#   scripts/consistency/run_daemon_crash_paper.bash --output-dir results/daemon_crash
#
# Cells:
#   golden          --golden-repeats runs without a crash (reference membership)
#   kill            --n SIGKILLs of the daemon at a random time
#   fault/<point>   --n-fault crashes at each injected point (hit 1..5 after START)
#   baseline        --n-baseline SIGKILLs with client reconnection disabled
#                   (AGNOCAST_DAEMON_RECONNECT=0), i.e. without the rollback
#
# Smoke run:
#   scripts/consistency/run_daemon_crash_paper.bash --n 2 --n-fault 1 --n-baseline 1 \
#     --golden-repeats 1 --output-dir results/daemon_crash_smoke

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

N=100
N_FAULT=20
N_BASELINE=10
GOLDEN_REPEATS=3
OUTPUT_DIR="${BENCH_ROOT}/results/daemon_crash"
QOS_DEPTH=10
NUM_SUBSCRIBERS=4
OUTAGE_MS=300
FAULT_POINTS=(
  shm.add_subscriber
  shm.add_publisher
  shm.remove_subscriber
  shm.remove_subscriber_refs
  shm.remove_publisher
  reply.add_subscriber
  reply.add_publisher
  reply.remove_subscriber
  reply.remove_publisher
)

while [[ $# -gt 0 ]]; do
  case "$1" in
  --n) N="$2"; shift 2 ;;
  --n-fault) N_FAULT="$2"; shift 2 ;;
  --n-baseline) N_BASELINE="$2"; shift 2 ;;
  --golden-repeats) GOLDEN_REPEATS="$2"; shift 2 ;;
  --fault-points) IFS=',' read -r -a FAULT_POINTS <<<"$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  --qos-depth) QOS_DEPTH="$2"; shift 2 ;;
  --num-subscribers) NUM_SUBSCRIBERS="$2"; shift 2 ;;
  --outage-ms) OUTAGE_MS="$2"; shift 2 ;;
  -h | --help) sed -n '3,21p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"
COMMON=(--qos-depth "${QOS_DEPTH}" --num-subscribers "${NUM_SUBSCRIBERS}" --outage-ms "${OUTAGE_MS}")

trial() {
  # A trial that dies is recorded as such by the summary (no metrics.json).
  "${SCRIPT_DIR}/run_daemon_crash.bash" "${COMMON[@]}" "$@" || echo "  (trial exited with $?)"
}

echo "=== golden: ${GOLDEN_REPEATS} runs without a crash ==="
for i in $(seq 1 "${GOLDEN_REPEATS}"); do
  trial --mode none --output-dir "${OUTPUT_DIR}/golden/run_${i}"
done

echo "=== kill: ${N} SIGKILLs ==="
for i in $(seq 1 "${N}"); do
  echo "  kill ${i}/${N}"
  trial --mode kill --output-dir "${OUTPUT_DIR}/kill/run_${i}"
done

for point in "${FAULT_POINTS[@]}"; do
  echo "=== fault ${point}: ${N_FAULT} crashes ==="
  for i in $(seq 1 "${N_FAULT}"); do
    hit=$((1 + RANDOM % 5))
    echo "  ${point} ${i}/${N_FAULT} (hit ${hit})"
    trial --mode fault --fault-point "${point}" --fault-hit "${hit}" \
      --output-dir "${OUTPUT_DIR}/fault/${point}/run_${i}"
  done
done

echo "=== baseline: ${N_BASELINE} SIGKILLs without reconnection ==="
for i in $(seq 1 "${N_BASELINE}"); do
  echo "  baseline ${i}/${N_BASELINE}"
  trial --mode kill --no-reconnect --wait-timeout-sec 8 --recovery-timeout-ms 2000 \
    --output-dir "${OUTPUT_DIR}/baseline/run_${i}"
done

echo ""
echo "=== summary ==="
python3 "${SCRIPT_DIR}/summarize_daemon_crash.py" "${OUTPUT_DIR}" --qos-depth "${QOS_DEPTH}" \
  --csv "${OUTPUT_DIR}/summary.csv"
