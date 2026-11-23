#!/bin/bash
# Bytes persisted per publish by A and B (T=10, S=4, one iteration).
S="${RECOVERY_WORK:-/tmp/agnocast_recovery}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
set +u; source /opt/ros/humble/setup.bash; set -u
for v in a b; do
  rm -f /dev/shm/agnocast_daemon.a /dev/shm/agnocast_daemon.b /tmp/agnocast_daemon.state
  OUT=$S/bytes_$v; rm -rf $OUT
  AGNOCAST_DAEMON_BIN=$S/bin/daemon_$v bash $REPO/scripts/bench/run_bench.bash --backend daemon --no-rt \
    --num-topics 10 --num-subscribers 4 --rate-hz 100 --duration 5 --warmup 2 --iterations 1 \
    --output-dir $OUT > $OUT.log 2>&1
  bytes=$(grep -o "wrote [0-9]* bytes" $REPO/ws/daemon/agnocast_daemon.log | grep -o "[0-9]*")
  pubs=$(python3 -c "
import glob,pandas as pd
print(sum(int(pd.read_csv(f).seq.max())+1 for f in glob.glob('$OUT/iter_0/publisher_*_latencies.csv')))")
  echo "$v bytes=$bytes publishes=$pubs per_publish=$(python3 -c "print(round($bytes/$pubs))")"
  pkill -x daemon_$v 2>/dev/null
done
