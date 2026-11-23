# Model checking the daemon restart recovery

`recovery_mc.py` is an explicit-state model checker for the recovery of
`agnocast_daemon` after it crashes (`agnocast_daemon/protocol.h`, "Daemon
restart recovery"). The daemon keeps its metadata on its heap and persists
only the pids of its clients; a restarted daemon rebuilds the metadata from
what the clients report. The script models one topic with one publisher and
two subscribers, enumerates every reachable state breadth first, and checks
the invariants in each. No tool beyond Python 3 is needed.

## Model

| Part | Modelled behaviour |
|---|---|
| Publisher | publishes into its own memory, at most one publish without a reply, records the address as pending before the request and the entry on the reply, frees an address only when a reply lists it and may then reuse it |
| Daemon | commits a publish, releases unreferenced entries beyond the depth, pushes into subscriber rings unless a subscriber is `ipc_pending`, drains release rings before a publish, serves receive requests; its reply can be lost |
| Subscribers | take pushed entries (held, receive position and newest-from-publisher recorded before use), release held entries into a ring (forgotten first), receive over a request when `ipc_pending` |
| Crash | the daemon's state and in-flight replies are lost; the rings (client memory) survive |
| Recovery | the restarted daemon waits for every live client's report and serves nothing else; a restoring subscriber drops its rings and becomes `ipc_pending`; after all reports, references are claimed, a pending address matching a subscriber's newest entry above the publisher's last answered id is taken as committed, and the repeated publish is answered from its entry |

## Invariants

| Name | Meaning |
|---|---|
| `MemSafe` | Every message a subscriber holds, or was pushed and has not taken, is still in the publisher's memory. |
| `NoDup` | No subscriber is handed one message twice. |
| `RightAnswer` | A repeated publish is answered with the entry of that message. |
| `RefAgree` | Outside a recovery, the daemon references every entry a subscriber holds. |

## Running

```bash
python3 recovery_mc.py --publishes 3 --crashes 1   # the protocol: no violation
python3 recovery_mc.py --publishes 3 --crashes 2
python3 recovery_mc.py --no-grace        # must violate (serve before all reported)
python3 recovery_mc.py --no-pending      # must violate (pending address not reported)
python3 recovery_mc.py --no-delivered    # must violate (newest delivery not reported)
python3 recovery_mc.py --no-id-bound     # must violate (an older message at a reused address)
```

Results on the bounds above (2 subscribers, 2 addresses, depth 1):

| Run | States | Result |
|---|---|---|
| 3 publishes, 1 crash | 178,632 | no violation |
| 3 publishes, 2 crashes | 493,523 | no violation |
| `--no-grace` | | NoDup violated |
| `--no-pending` | | RefAgree violated |
| `--no-delivered` | | NoDup violated |
| `--no-id-bound` | | RightAnswer violated |

Each broken variant prints the shortest trace that leads to its violation.

## Limits

The bounds are small, so this is bounded model checking, not a proof for
every size. The model abstracts the implementation: one topic, one publisher,
no client crashes (those are covered by the single-writer exit cleanup), and
the daemon's request handling is one atomic step per request, which holds
because a handler runs a request to completion under the topic lock.
