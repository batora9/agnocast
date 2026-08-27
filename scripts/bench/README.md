# M0 latency benchmark (kmod vs user daemon)

Compares Agnocast metadata round-trip latency. Default is **M0** (daemon runs
with `SCHED_OTHER`). Pass `--daemon-rt-priority 80` for **M1** (daemon
`SCHED_FIFO`, same priority as the bench clients).

## Prerequisites

- ROS 2 Humble sourced
- For **kmod** runs: `sudo insmod agnocast_kmod/agnocast.ko`
- `pip install --user -r scripts/bench/requirements.txt` (plotting; needs `numpy<2` on Ubuntu 22.04)

## Build

Builds isolated colcon trees under `ws/kmod/` and `ws/daemon/` (compile-time backend switch):

```bash
source /opt/ros/humble/setup.bash
scripts/bench/build.bash              # both backends
scripts/bench/build.bash kmod         # kernel module client only
scripts/bench/build.bash daemon       # user daemon client only
```

Daemon backend also requires the metadata daemon:

```bash
scripts/run_daemon.bash               # foreground, or let run_bench start it
```

## Run

Single configuration smoke test:

```bash
scripts/bench/run_bench.bash --backend daemon \
  --num-topics 1 --num-subscribers 2 --rate-hz 100
```

Full sweeps follow ipc_shared_ptr paper §VII TABLE IV (`R = 100` Hz throughout):

| Sweep | Variable | Range | Fixed |
|-------|----------|-------|-------|
| A | `T` (topics) | 1, 25, …, 200 | `S=2` |
| B | `S` (subscribers) | 1, 4, …, 32 | `T=10` |
| C | `T × S` | `T`: 10–100, `S`: 2–16 | — |

The old rate axis (`T=10`, `S=4`, `R` in 10–1000 Hz) is not in the paper; pass `--sweeps rate` to run it under `sweep_rate/`.

```bash
source /opt/ros/humble/setup.bash
scripts/bench/build.bash
sudo scripts/bench/prep_repro_env.sh   # Turbo off, freq pin, C-state ≤ C1, MQ limits
scripts/bench/compare.bash --sweeps a,b,c --iterations 5 --warmup 2 \
  --output-dir results/paper_sweeps
sudo scripts/bench/prep_repro_env.sh --restore
```

Or the same sequence as one script: `scripts/bench/run_paper_sweeps.bash`.

M1 (daemon `SCHED_FIFO` 80, same as clients). Writes to
`results/paper_sweeps_fifo80/` so it does not overwrite M0. `--skip-kmod` reuses
the previous kmod tree for plots:

```bash
scripts/bench/run_paper_sweeps.bash --daemon-rt-priority 80 --skip-kmod \
  --kmod-data results/paper_sweeps/kmod
```

C-state restriction only can be skipped (Turbo off, freq pin, and mqueue limits still apply):

```bash
scripts/bench/run_paper_sweeps.bash --no-cstate --output-dir results/paper_sweeps_no_cstate
```

Skip all host prep (matches early smoke: powersave governor, deep C-states allowed):

```bash
scripts/bench/run_paper_sweeps.bash --no-prep --output-dir results/paper_sweeps_no_prep
```

Set `AGNOCAST_NO_DISCOVERY_AGENT=1` (handled by runner scripts) to avoid fork storms during multi-process sweeps.

### Event budget and skipped points

A configuration whose event rate exceeds `EVENT_BUDGET` is skipped, so the sweep
never reports numbers that are really OS saturation. The rate is

```text
T * (1 + S) * (R + 10)
```

The `+10` is the 100 ms control timer in every process. The paper's Xeon
(8C/16T) sustained ~140k events/s and used 60% of that (84k). This 8C/8T host
defaults to **42k**. Override with `--event-budget` or `EVENT_BUDGET=`.

At 42k the following TABLE IV points skip (they are not failures):

- A: `T=150–200` (`T=150` is 49.5k)
- C: `(T,S)` from about `(40,12)` upward, and `T≥80` with `S≥4`

That is the upper-right of the paper's Fig. 8. Raising the budget to force those
points makes p99.9 scheduler-dominated and is not comparable.

## Metrics

| Metric | Meaning |
|--------|---------|
| `publish_ipc` | `agnocast_ipc_publish_msg` round trip (ioctl vs UDS) |
| `receive_ipc` | `agnocast_ipc_receive_msg` round trip |
| `publish` | `borrow_loaned_message()` through `publish()` |
| `e2e` | publisher stamp to subscriber callback entry |

## IPC breakdown

The two `*_ipc` metrics are also split into non-overlapping segments that sum to
the round trip, so a slow backend can be attributed to scheduling rather than to
work. Both `summary.csv` (rows named `<metric>_<segment>`) and the raw
`ipc_*_ns` columns carry them.

| Segment | Covers |
|---------|--------|
| `req` | client marshals the request and zeroes the response struct |
| `up` | `sendmsg` until the daemon's `recv` returns: transport plus wakeup |
| `lock` | daemon dispatch, topic lookup and lock acquisition |
| `work` | daemon handler body and response fill |
| `down` | daemon's reply until the client's `recvmsg` returns |
| `post` | client unmarshals the response into the caller's args |
| `send_syscall` | `sendmsg` alone; nested inside `up`, never summed |

The split needs stamps taken inside the daemon and returned in a response
trailer, so `agnocast_daemon` must also be built with `AGNOCAST_BENCH_TIMING`
(`build.bash` does this). Under the kernel module the metadata work runs in the
caller's own context with no second scheduling entity, so the segments are all
zero and only the round trip is reported; use ftrace or bpftrace to look inside
the ioctl.

```bash
scripts/bench/breakdown.py results/kmod results/daemon --legend
```
