# Daemon restart recovery experiments

Scripts behind the evaluation of the recovery variants: C (rebuild from the
client reports, `user-daemon-push-release-recovery`), A (persistent region with
an undo log, `user-daemon-push-release-persist-a`) and B (redo log,
`user-daemon-push-release-persist-b`). Build the clients once
(`scripts/bench/build.bash daemon`, `scripts/consistency/build.bash daemon`),
build each variant's daemon, and copy the binaries to
`$RECOVERY_WORK/bin/daemon_{c,a,b}` (default work directory
`/tmp/agnocast_recovery`); `daemon_orig` is the daemon of `user-daemon-push-release`
and `$RECOVERY_WORK/wt_orig` a worktree of that branch with its own `ws/`.

| Script | What it does |
|---|---|
| `e2e_variant.bash OUT` | one restart trial: tight-loop publisher, `NSUB` holding subscribers, the daemon killed at `KILL_AT` (or at `FAULT_POINT`/`FAULT_HIT`, or aborting on a reference corrupted at `CORRUPT_HIT`) and restarted; checks survival, rejected reports, duplicate answers and leaks |
| `e2e_restart.bash OUT` | one restart trial with the latency harness at 100 Hz; `check_seqs.py OUT/res` checks that every message arrives once and in order |
| `run_recovery.bash` | all daemon-crash trials for C, A and B into `results/recovery_daemon_crash/trials.csv` |
| `run_overhead.bash "T S" ...` | normal-path latency of the baseline, C, A and B (`--no-rt`); `pool_overhead.py results/recovery_overhead` pools the samples |
| `run_bytes.bash` | bytes A and B persist per publish |

Without root, `fs.mqueue.queues_max` (256) limits the bench to about
`4 * T * S < 256`.
