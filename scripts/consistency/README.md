# Metadata consistency harness (kmod vs user daemon)

Compares crash cleanup (`SIGKILL`) against the SIGINT / destructor `remove_*`
path. Latency benches under [`../bench/`](../bench/README.md) are unchanged.

This is the measurement behind the WiP paragraph "Metadata Consistency under
Process Crash": 1 publisher and 4 subscribers, N=100 kills per
`{sub, pub} × {kmod, daemon}` cell. Detection latency is not measured.

## What is compared

After the victim exits, the harness records:

1. **Membership** via the public query APIs (topic list, topic info, pub/sub
   counts). `SIGKILL` must match a `SIGINT` golden of the same topology.
2. **Leak.**
   - Subscriber crash: remaining holders pause first so process-exit cleanup
     can take the exclusive lock. The observer then waits until the victim
     node is gone from public membership (that erase also clears its entry
     ref bits). Holders drop their pinned `last_` (subscription stays) so
     those messages are not stuck outside `qos_depth`. Only then does the
     surviving publisher flush outstanding (`publish` count minus cumulative
     `ret_released_num`), which must return to `qos_depth` (one extra
     in-flight publish is allowed).
   - Publisher crash: after the membership snapshot, remaining subscribers
     drop their last `ipc_shared_ptr` and `remove_subscriber` (that is what
     actually reclaims orphaned publisher entries). The dead publisher must
     then disappear (`publisher_num == 0`). A leftover record means entries
     were not reclaimed. Membership itself still lists the publisher while
     holders keep refs — that is the SIGINT/SIGKILL shared correct answer,
     not a leak.

The victim process tight-loops `publish` or `receive_msg` + `release_sub_ref`
with no sleep so it can die mid-request. Remaining subscribers hold the latest
message.

Pub/sub processes register first, then wait for a `START` file so the observer
can query membership before the tight loops fill the daemon. Subscriber-crash
trials pause holders, wait for the victim to leave membership, `RELEASE_HELD`,
`FLUSH` outstanding while the publisher is still publishing, then `PAUSE` the
publisher for the snapshot. Publisher-crash trials wait 1 s, snapshot, then
write `DROP` so holders release their last message and the observer records
`after_drop.json`.

## Prerequisites

- ROS 2 Humble sourced
- For **kmod** runs: `sudo insmod agnocast_kmod/agnocast.ko`
- Dual-backend install trees (same `ws/kmod` and `ws/daemon` as the latency bench)

```bash
source /opt/ros/humble/setup.bash
scripts/consistency/build.bash
```

`build.bash` also builds `agnocastlib` into those trees. If you already ran
`scripts/bench/build.bash`, this is incremental.

## Run

One trial:

```bash
scripts/consistency/run.bash --backend daemon --crash-role sub --signal kill \
  --output-dir /tmp/consistency_trial
```

Paper sweep (SIGINT golden × 3, then N=100 SIGKILL per cell):

```bash
scripts/consistency/run_paper.bash --output-dir results/consistency
```

Smoke with fewer kills:

```bash
scripts/consistency/run_paper.bash --n 2 --golden-repeats 1 \
  --output-dir results/consistency_smoke
```

Summarize an existing tree:

```bash
python3 scripts/consistency/summarize.py results/consistency
```

Output columns: `backend,crash_role,n,membership_match,leak_count`.

Set `AGNOCAST_NO_DISCOVERY_AGENT=1` and `AGNOCAST_BRIDGE_MODE=off` are applied
by the runner so query snapshots only contain the experiment endpoints.
