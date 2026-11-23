#!/bin/bash
# Normal-path cost of the recovery variants: baseline push-release (orig) vs
# C (no persistence), A (persistent region + undo) and B (redo log).
# usage: run_overhead.bash "T S" ... (variants from VARIANTS, default all)
# RT=1 runs the clients at SCHED_FIFO 80 and the daemon at SCHED_FIFO 90 like
# the paper's latency sweeps (needs sudo; run_bench.bash asks for it) and
# writes to results/recovery_overhead_rt.
set -u
S="${RECOVERY_WORK:-/tmp/agnocast_recovery}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ ${RT:-0} == 1 ]]; then
  OUT=$REPO/results/recovery_overhead_rt
  SCHED_ARGS=(--daemon-rt-priority 90)
  sudo -v || exit 1
else
  OUT=$REPO/results/recovery_overhead
  SCHED_ARGS=(--no-rt)
fi
mkdir -p "$OUT"
ITER=${ITER:-3}
DUR=${DUR:-10}
set +u; source /opt/ros/humble/setup.bash; set -u
for cfg in "$@"; do
  read -r T N <<<"$cfg"
  for v in ${VARIANTS:-orig c a b}; do
    rm -f /dev/shm/agnocast_daemon.a /dev/shm/agnocast_daemon.b /tmp/agnocast_daemon.state 2>/dev/null ||
      sudo -n rm -f /dev/shm/agnocast_daemon.a /dev/shm/agnocast_daemon.b /tmp/agnocast_daemon.state
    if [[ $v == orig ]]; then root=$S/wt_orig; else root=$REPO; fi
    echo "=== $v T=$T S=$N"
    AGNOCAST_DAEMON_BIN=$S/bin/daemon_$v \
      bash "$root/scripts/bench/run_bench.bash" --backend daemon "${SCHED_ARGS[@]}" \
      --num-topics $T --num-subscribers $N --rate-hz 100 --duration $DUR --warmup 2 \
      --iterations $ITER --output-dir "$OUT/$v/t${T}_s${N}" > "$OUT.$v.t${T}_s${N}.log" 2>&1
    echo "  rc=$? $(grep -c 'Summary' "$OUT.$v.t${T}_s${N}.log") iterations"
    pkill -x daemon_$v 2>/dev/null || sudo -n pkill -x daemon_$v 2>/dev/null
  done
done
