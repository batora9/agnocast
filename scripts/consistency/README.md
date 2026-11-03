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

---

## Rollback after a daemon crash (user daemon only)

The counterpart of the measurement above for the daemon itself: the daemon is
killed while the topic is busy, a fresh daemon is started, and the harness
checks that nothing was lost or left half done. The kernel module has no
equivalent (a crash there takes the host down), so only the daemon backend is
measured; a run with client reconnection disabled serves as the baseline.

### How the rollback works

- **Message state survives.** Entries, references and per-subscriber positions
  live in the per-topic shared segment (`agnocast_daemon/topic_shm.hpp`), which
  every client keeps mapped. Publish, receive and release do not go through the
  daemon, so they go on while it is down.
- **Half-done segment operations are undone.** Every change made under the
  segment's robust mutex first logs the old value. The next locker that gets
  `EOWNERDEAD` (a client or the restarted daemon) replays the log backwards and
  rebuilds the derived indexes. This also covers a *client* dying mid-publish
  or mid-receive.
- **Registrations are replayed.** Each client remembers what it registered and
  keeps its segment descriptors. When the connection breaks it reconnects and
  sends `RESTORE_PROCESS / _MAPPINGS / _TOPIC / _PUBLISHER / _SUBSCRIBER /
  _DONE`; calls that were cut off are repeated.
- **Unacknowledged changes are rolled back.** The restarted daemon reads the
  previous daemon's client pids from `/tmp/agnocast_daemon.state`, holds new
  registrations back until each has restored or died, and then removes every
  id in a segment that no client claimed (a registration whose reply was lost,
  or a client that died during the outage).

### What a trial records

Topology: 1 tight-loop publisher, 4 holder subscribers (executor, keep the
latest message), and a churn process that keeps registering and removing a
subscriber (holding references when it is removed) and a publisher, so the
daemon is often inside a segment operation when it dies.

| metric | meaning |
| --- | --- |
| `survived` | every client process is alive at the end; the churn process exits cleanly on SIGINT |
| `flow` | publishes and deliveries to every subscriber continued during the outage (`--outage-ms`, default 300) |
| `restored` | membership after the restart equals the golden (no crash) runs |
| `consistent` | `DEBUG_CHECK_TOPIC` reports 0 violations: segment invariants (order, free list, reference counts, references only from subscribers, per-publisher counts, empty undo log) and agreement between the segment and the daemon's registrations |
| `leak_free` | once the holders let go, the publisher's outstanding entries return to `qos_depth` (+1 in flight) |
| `rolled_back` | trials where the undo log rolled back an operation whose holder died |
| `recovery_ms` | time from the restarted daemon's start to the end of its recovery |

Crash modes:

- `kill`: SIGKILL at a random time 0.2–1.0 s after START.
- `fault:<point>`: the daemon (built with `AGNOCAST_FAULT_INJECTION`, on by
  default) kills itself at a given point, on its 1st–5th visit after START.
  `shm.*` points are inside segment operations with the mutex held;
  `reply.*` points are after a change was committed but before the client was
  told.

  | point | where |
  | --- | --- |
  | `shm.add_subscriber` | id written, subscriber list and sequence not yet |
  | `shm.add_publisher` | id written, subscribers not yet told to map its memory |
  | `shm.remove_subscriber` | id cleared, references not yet |
  | `shm.remove_subscriber_refs` | some of the subscriber's references cleared |
  | `shm.remove_publisher` | entries dropped, id not yet cleared |
  | `reply.add_subscriber` / `reply.add_publisher` | registered, reply not sent |
  | `reply.remove_subscriber` / `reply.remove_publisher` | removed, reply not sent |

### Run

```bash
source /opt/ros/humble/setup.bash
scripts/consistency/build.bash daemon

# one trial
scripts/consistency/run_daemon_crash.bash --mode kill --output-dir /tmp/dc_kill
scripts/consistency/run_daemon_crash.bash --mode fault --fault-point shm.remove_subscriber \
  --fault-hit 2 --output-dir /tmp/dc_fault

# paper sweep: 3 golden, 100 kills, 20 per fault point, 10 baseline
scripts/consistency/run_daemon_crash_paper.bash --output-dir results/daemon_crash

# smoke
scripts/consistency/run_daemon_crash_paper.bash --n 2 --n-fault 1 --n-baseline 1 \
  --golden-repeats 1 --output-dir results/daemon_crash_smoke

python3 scripts/consistency/summarize_daemon_crash.py results/daemon_crash
```

Output columns: `cell,trials,crashed,survived,flow,restored,consistent,leak_free,rolled_back,orphan_pub,orphan_sub,recovery_ms_median,recovery_ms_max,incomplete`.
`orphan_*` count registrations the restarted daemon dropped because no client
claimed them. Fault trials whose point was never reached are not counted in
`crashed` or after.

Client-side knobs: `AGNOCAST_DAEMON_RECONNECT=0` (baseline: no reconnect),
`AGNOCAST_DAEMON_RECONNECT_TIMEOUT_MS` (how long a call waits for the daemon,
default 60000). Daemon-side: `AGNOCAST_DAEMON_RECOVERY_TIMEOUT_MS` (how long
the restarted daemon waits for its predecessor's clients, default 5000),
`AGNOCAST_DAEMON_STATE_PATH`.

Limitations: a client run with `AGNOCAST_TOPIC_SHM=0` keeps no segment and
cannot be restored, and its daemon-served publish may be applied twice if the
daemon dies after committing it. A client that does not restore within the
recovery timeout loses its registrations.
