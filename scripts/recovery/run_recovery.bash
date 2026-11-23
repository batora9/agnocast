#!/bin/bash
# Daemon-crash experiments for the recovery variants C, A and B.
S="${RECOVERY_WORK:-/tmp/agnocast_recovery}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/results/recovery_daemon_crash"
mkdir -p "$OUT"
R=$OUT/trials.csv
echo "variant,kind,nsub,param,result" > "$R"
rnd() { python3 -c "import random;print(round(random.uniform($1,$2),2))"; }
one() { # variant kind nsub param env...
  local v=$1 k=$2 n=$3 p=$4; shift 4
  local line
  line=$(env DAEMON=$S/bin/daemon_$v NSUB=$n "$@" timeout 90 $HERE/e2e_variant.bash "$S/rec/$v.$k.$n.$p" 2>&1 | tail -1)
  echo "$v,$k,$n,$p,\"$line\"" >> "$R"
  echo "$v $k n=$n $p: $line"
}
for v in c a b; do
  for i in $(seq 1 ${RANDOM_N:-20}); do one $v random 2 "$i" KILL_AT=$(rnd 1 3) OUTAGE=$(rnd 0.2 0.6); done
  for i in $(seq 1 ${FAULT_N:-10}); do
    h=$(python3 -c "import random;print(random.randint(2000,30000))")
    one $v fault 2 "$h" FAULT_POINT=publish_before_reply FAULT_HIT=$h
  done
  for i in 1 2 3; do one $v corrupt 2 "$i" CORRUPT_HIT=3000; done
  for n in 2 8 32; do for i in 1 2 3; do one $v scale $n "$i" KILL_AT=2 OUTAGE=0.3; done; done
done
for v in a b; do for t in 1.5 10 30; do one $v history 2 "$t" KILL_AT=$t OUTAGE=0.3; done; done
# Sequence numbers across a restart (bench publisher and two subscribers at 100 Hz).
for v in c a b; do
  for i in 1 2 3 4 5; do
    k=$(rnd 1.5 4); o=$(rnd 0.2 0.8)
    DAEMON=$S/bin/daemon_$v KILL_AT=$k OUTAGE=$o DURATION=6 timeout 60 $HERE/e2e_restart.bash "$S/rec/$v.seq.$i" >/dev/null 2>&1
    r=$(python3 $HERE/check_seqs.py "$S/rec/$v.seq.$i/res" | tail -1)
    echo "$v,seq,2,$i,\"$r\"" >> "$R"; echo "$v seq $i: $r"
  done
  for i in 1 2 3; do
    h=$(python3 -c "import random;print(random.randint(150,500))")
    DAEMON=$S/bin/daemon_$v FAULT_POINT=publish_before_reply FAULT_HIT=$h DURATION=6 timeout 60 $HERE/e2e_restart.bash "$S/rec/$v.seqf.$i" >/dev/null 2>&1
    r=$(python3 $HERE/check_seqs.py "$S/rec/$v.seqf.$i/res" | tail -1)
    echo "$v,seqfault,2,$h,\"$r\"" >> "$R"; echo "$v seqfault $h: $r"
  done
done
