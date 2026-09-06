#!/usr/bin/env bash
set -euo pipefail
#
# Paper sweep: {sub, pub} crash × {kmod, daemon}.
# For each cell, records SIGINT golden runs then N SIGKILL trials.
#
#   source /opt/ros/humble/setup.bash
#   scripts/consistency/build.bash
#   scripts/consistency/run_paper.bash --output-dir results/consistency

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../bench/common.bash
source "${SCRIPT_DIR}/../bench/common.bash"

BACKENDS=(kmod daemon)
ROLES=(sub pub)
N=100
GOLDEN_REPEATS=3
OUTPUT_DIR="${BENCH_ROOT}/results/consistency"
QOS_DEPTH=10
NUM_SUBSCRIBERS=4

while [[ $# -gt 0 ]]; do
  case "$1" in
  --backends) IFS=',' read -r -a BACKENDS <<<"$2"; shift 2 ;;
  --roles) IFS=',' read -r -a ROLES <<<"$2"; shift 2 ;;
  --n) N="$2"; shift 2 ;;
  --golden-repeats) GOLDEN_REPEATS="$2"; shift 2 ;;
  --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
  --qos-depth) QOS_DEPTH="$2"; shift 2 ;;
  --num-subscribers) NUM_SUBSCRIBERS="$2"; shift 2 ;;
  -h | --help) sed -n '3,12p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) die "unknown option: $1" ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"

for backend in "${BACKENDS[@]}"; do
  validate_backend "${backend}"
  for role in "${ROLES[@]}"; do
    [[ "${role}" == "sub" || "${role}" == "pub" ]] || die "unknown role ${role}"
    cell="${OUTPUT_DIR}/${backend}/${role}"
    golden_dir="${cell}/golden"
    echo "=== ${backend} ${role}: ${GOLDEN_REPEATS} SIGINT golden runs ==="
    for i in $(seq 1 "${GOLDEN_REPEATS}"); do
      "${SCRIPT_DIR}/run.bash" \
        --backend "${backend}" \
        --crash-role "${role}" \
        --signal int \
        --qos-depth "${QOS_DEPTH}" \
        --num-subscribers "${NUM_SUBSCRIBERS}" \
        --output-dir "${golden_dir}/run_${i}"
    done
    python3 "${SCRIPT_DIR}/summarize.py" --check-golden "${golden_dir}" \
      --crash-role "${role}" --qos-depth "${QOS_DEPTH}"

    echo "=== ${backend} ${role}: ${N} SIGKILL trials ==="
    for i in $(seq 1 "${N}"); do
      echo "  kill trial ${i}/${N}"
      "${SCRIPT_DIR}/run.bash" \
        --backend "${backend}" \
        --crash-role "${role}" \
        --signal kill \
        --qos-depth "${QOS_DEPTH}" \
        --num-subscribers "${NUM_SUBSCRIBERS}" \
        --output-dir "${cell}/kill/run_${i}"
    done
  done
done

echo ""
echo "=== summary ==="
python3 "${SCRIPT_DIR}/summarize.py" "${OUTPUT_DIR}" --qos-depth "${QOS_DEPTH}"
