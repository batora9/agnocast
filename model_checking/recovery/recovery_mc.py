#!/usr/bin/env python3
"""Explicit-state model checking of the daemon restart recovery (C).

The daemon keeps the metadata on its heap and persists only the clients' pids;
a restarted daemon rebuilds the metadata from what the clients report
(agnocast_daemon/protocol.h, "Daemon restart recovery"). This script models
one topic with one publisher P and two subscribers, explores every reachable
state by breadth-first search, and checks the invariants in every state.

Modelled, as in the implementation:
  - P publishes into its own memory; it frees an address only when a publish
    reply lists it as released, and may then reuse it (address reuse)
  - at most one publish of P is without a reply; P records the address as
    pending before the request and the entry when the reply arrives
  - the daemon commits a publish, releases P's unreferenced entries beyond
    the depth, and pushes the entry into each subscriber's push ring (client
    memory) unless the subscriber is ipc_pending; the reply can be lost
  - a subscriber takes pushed entries (handing them to the user: recorded as
    held, as the newest from P, and in its receive position before use),
    releases held entries into its release ring (client memory, forgotten
    first), and receives over a request when ipc_pending
  - the daemon drains release rings before a publish
  - the daemon may crash at any step (its state and in-flight replies are
    lost; the rings survive) and restart: it then waits for every live
    client to report (grace period) and serves nothing else
  - reports: P its entries, its pending address and its last answered id; a
    subscriber its held entries, its receive position and its newest entry
    from P; a restoring subscriber drops its rings and becomes ipc_pending
  - after all reports: references are claimed, a pending address matching a
    subscriber's newest entry above P's last answered id is that publish, and
    the repeated publish of a pending address is answered from its entry

Invariants:
  MemSafe      every message a subscriber holds or has pushed but not taken is
               still in P's memory (no use-after-free)
  NoDup        no subscriber is handed one message twice
  RightAnswer  a repeated publish is answered with the entry of that message
  RefAgree     outside a recovery, the daemon references every held entry

Variants (each must violate an invariant, which shows the checks bite):
  --no-grace       serve publishes while clients are still reporting
  --no-pending     P does not report its pending address
  --no-delivered   subscribers do not report their newest entry from P
  --no-id-bound    accept a delivered entry at or below P's last answered id
"""

import argparse
import collections
import sys

# ---------------------------------------------------------------------------
# State (all tuples / frozensets, hashable)
#
# mem      tuple: message id stored at each address (-1: free)
# P        (free_addrs, entries, pending, last, next_msg, published, reply)
#            entries: frozenset of (eid, addr); pending: addr or -1
#            reply: None or (eid, released_addrs_tuple)   [lost on a crash]
# S[i]     (held, ring, rel, cursor, newest, delivered, ipc_pending, rreply)
#            held: frozenset of (eid, addr, msg)
#            ring: tuple of (eid, addr)          [client memory]
#            rel: tuple of eid                   [client memory]
#            newest: (eid, addr) or None
#            delivered: frozenset of msg ids handed to the user
#            rreply: None or tuple of (eid, addr) [lost on a crash]
# D        daemon: (alive, recovering, entries, cur, dcursor, pend_addrs,
#                   reported, reports, crashes)
#            entries: frozenset of (eid, addr, refs_frozenset)
#            dcursor: tuple per subscriber
#            pend_addrs: frozenset (repeated-publish detection)
#            reported: frozenset of client names that reported
#            reports: tuple holding the raw reports until the grace ends
# ghost    frozenset of (eid, msg): which message each committed entry holds
# ---------------------------------------------------------------------------

NSUB = 2
ADDRS = 2
DEPTH = 1


def entries_of(D):
    return {e: (a, refs) for (e, a, refs) in D[2]}


def mk_entries(m):
    return frozenset((e, a, refs) for e, (a, refs) in m.items())


class Model:
    def __init__(self, args):
        self.a = args

    def init(self):
        mem = tuple([-1] * ADDRS)
        P = (frozenset(range(ADDRS)), frozenset(), -1, -1, 0, 0, None)
        S = tuple(
            (frozenset(), (), (), -1, None, frozenset(), True, None) for _ in range(NSUB))
        D = (True, False, frozenset(), 0, tuple([-1] * NSUB), frozenset(), frozenset(), (), 0)
        return (mem, P, S, D, frozenset())

    # --- daemon helpers -----------------------------------------------------

    def drain(self, D, S):
        """Apply every release ring; returns (D, S)."""
        ents = entries_of(D)
        S = list(S)
        for i in range(NSUB):
            held, ring, rel, cur, newest, dlv, ipc, rr = S[i]
            for e in rel:
                if e in ents:
                    a, refs = ents[e]
                    ents[e] = (a, refs - {i})
            S[i] = (held, ring, (), cur, newest, dlv, ipc, rr)
        D = (D[0], D[1], mk_entries(ents)) + D[3:]
        return D, tuple(S)

    def receive_core(self, ents, i, dcur):
        """Entries newer than dcur within the depth; sets references."""
        if not ents:
            return ents, dcur, ()
        newest = max(ents)
        start = max(newest - DEPTH + 1, dcur + 1)
        got = []
        for e in sorted(ents):
            if e < start:
                continue
            a, refs = ents[e]
            ents[e] = (a, refs | {i})
            got.append((e, a))
        if got:
            dcur = got[-1][0]
        return ents, dcur, tuple(got)

    # --- successor generation ----------------------------------------------

    def succ(self, st):
        mem, P, S, D, ghost = st
        alive, recovering = D[0], D[1]
        out = []

        # P publishes a new message (records pending before the request).
        free, pents, pend, last, nmsg, published, preply = P
        if pend == -1 and free and published < self.a.publishes:
            for addr in sorted(free):
                mem2 = list(mem)
                mem2[addr] = nmsg
                P2 = (free - {addr}, pents, addr, last, nmsg + 1, published + 1, None)
                out.append(("P.publish", (tuple(mem2), P2, S, D, ghost)))

        # The daemon serves P's (possibly repeated) publish.
        serving = alive and (not recovering or self.a.no_grace)
        if serving and pend != -1 and preply is None and P_restored_or_normal(D, "P"):
            out.append(("D.publish", self.serve_publish(st)))

        # P gets the reply.
        if preply is not None:
            eid, released = preply
            ents2 = set(pents)
            ents2.add((eid, pend))
            free2 = set(free)
            for ra in released:
                for (e, a) in list(ents2):
                    if a == ra:
                        ents2.discard((e, a))
                free2.add(ra)
            mem2 = list(mem)
            for ra in released:
                mem2[ra] = -1  # freed: the next publish there overwrites it
            P2 = (frozenset(free2), frozenset(ents2), -1, max(last, eid), nmsg, published, None)
            out.append(("P.reply", (tuple(mem2), P2, S, D, ghost)))

        for i in range(NSUB):
            held, ring, rel, cur, newest, dlv, ipc, rr = S[i]
            # Take a pushed entry: hand it to the user.
            if ring:
                (e, a) = ring[0]
                msg = mem[a]
                S2 = list(S)
                S2[i] = (held | {(e, a, msg)}, ring[1:], rel, max(cur, e), (e, a)
                         if newest is None or e > newest[0] else newest,
                         dlv | {msg}, ipc, rr)
                out.append((f"S{i}.take", (mem, P, tuple(S2), D, ghost, ("deliver", i, msg, e))))
            # Release a held entry (forgotten first, then queued).
            for h in sorted(held):
                S2 = list(S)
                S2[i] = (held - {h}, ring, rel + (h[0],), cur, newest, dlv, ipc, rr)
                out.append((f"S{i}.release", (mem, P, tuple(S2), D, ghost)))
            # Receive over a request when ipc_pending.
            if ipc and rr is None and serving and P_restored_or_normal(D, f"S{i}"):
                ents = entries_of(D)
                dcur = list(D[4])
                ents, dcur_i, got = self.receive_core(ents, i, dcur[i])
                dcur[i] = dcur_i
                D2 = (D[0], D[1], mk_entries(ents), D[3], tuple(dcur)) + D[5:]
                S2 = list(S)
                S2[i] = (held, ring, rel, cur, newest, dlv, ipc, got)
                out.append((f"D.receive{i}", (mem, P, tuple(S2), D2, ghost)))
            if rr is not None:
                # The reply hands the entries to the user; pushing resumes.
                nh, nn, nd, nc = set(held), newest, set(dlv), cur
                events = []
                for (e, a) in rr:
                    msg = mem[a]
                    nh.add((e, a, msg))
                    nc = max(nc, e)
                    if nn is None or e > nn[0]:
                        nn = (e, a)
                    events.append(("deliver", i, msg, e))
                    nd.add(msg)
                S2 = list(S)
                S2[i] = (frozenset(nh), ring, rel, nc, nn, frozenset(nd), False, None)
                out.append((f"S{i}.rreply", (mem, P, tuple(S2), D, ghost) + tuple(events)))

        # The daemon crashes (bounded) or restarts.
        crashes = D[8]
        if alive and crashes < self.a.crashes:
            Pc = P[:6] + (None,)
            Sc = tuple(s[:7] + (None,) for s in S)
            D2 = (False, False, frozenset(), 0, tuple([-1] * NSUB), frozenset(), frozenset(), (),
                  crashes + 1)
            out.append(("D.crash", (mem, Pc, Sc, D2, ghost)))
        if not alive:
            D2 = (True, True, frozenset(), 0, tuple([-1] * NSUB), frozenset(), frozenset(), (),
                  crashes)
            out.append(("D.restart", (mem, P, S, D2, ghost)))

        # Clients report during the grace period.
        if alive and recovering:
            if "P" not in D[6]:
                pend_rep = pend if not self.a.no_pending else -1
                rep = ("P", pents, pend_rep, last)
                D2 = D[:6] + (D[6] | {"P"}, D[7] + (rep,), D[8])
                out.append(("P.restore", (mem, P, S, D2, ghost)))
            for i in range(NSUB):
                name = f"S{i}"
                if name in D[6]:
                    continue
                held, ring, rel, cur, newest, dlv, ipc, rr = S[i]
                nrep = None if self.a.no_delivered else newest
                rep = (name, i, held, cur, nrep)
                D2 = D[:6] + (D[6] | {name}, D[7] + (rep,), D[8])
                S2 = list(S)
                # A restoring subscriber drops its old channel (rings).
                S2[i] = (held, (), (), cur, newest, dlv, True, None)
                out.append((f"{name}.restore", (mem, P, tuple(S2), D2, ghost)))
            if len(D[6]) == NSUB + 1:
                out.append(("D.finish", (mem, P, S, self.finish(D), ghost)))
        return out

    def serve_publish(self, st):
        mem, P, S, D, ghost = st
        free, pents, pend, last, nmsg, published, _ = P
        D, S = self.drain(D, S)
        ents = entries_of(D)
        cur = D[3]
        pend_addrs = D[5]
        repeated = None
        if pend in pend_addrs:
            pend_addrs = pend_addrs - {pend}
            for e, (a, refs) in ents.items():
                if a == pend:
                    repeated = e
        events = []
        if repeated is not None:
            eid = repeated
            events.append(("answer", eid, mem[pend]))
        else:
            eid = cur
            cur += 1
            ents[eid] = (pend, frozenset())
            ghost = ghost | {(eid, mem[pend])}
        # Release P's unreferenced entries beyond the depth (oldest first).
        mine = sorted(ents)
        released = []
        excess = len(mine) - DEPTH
        for e in mine:
            if excess <= 0:
                break
            excess -= 1
            a, refs = ents[e]
            if refs:
                continue
            released.append(a)
            del ents[e]
        # Push to subscribers that are not ipc_pending.
        S2 = list(S)
        dcur = list(D[4])
        for i in range(NSUB):
            held, ring, rel, scur, newest, dlv, ipc, rr = S2[i]
            if ipc:
                continue
            ents, dcur[i], got = self.receive_core(ents, i, dcur[i])
            S2[i] = (held, ring + got, rel, scur, newest, dlv, ipc, rr)
        D2 = (D[0], D[1], mk_entries(ents), cur, tuple(dcur), pend_addrs) + D[6:]
        P2 = P[:6] + ((eid, tuple(released)),)
        return (mem, P2, tuple(S2), D2, ghost) + tuple(events)

    def finish(self, D):
        ents = {}
        cur = 0
        dcur = [-1] * NSUB
        pend_addrs = set()
        last = -1
        p_entries = frozenset()
        claims = []
        newest_reports = []
        for rep in D[7]:
            if rep[0] == "P":
                _, p_entries, pend, last = rep
                for (e, a) in p_entries:
                    ents[e] = (a, frozenset())
                    cur = max(cur, e + 1)
                if pend != -1:
                    pend_addrs.add(pend)
            else:
                _, i, held, scur, newest = rep
                dcur[i] = scur
                cur = max(cur, scur + 1)
                for (e, a, _m) in held:
                    claims.append((i, e, a))
                    cur = max(cur, e + 1)
                if newest is not None:
                    newest_reports.append(newest)
                    cur = max(cur, newest[0] + 1)
        for (i, e, a) in claims:
            if e in ents:
                if ents[e][0] == a:
                    ents[e] = (a, ents[e][1] | {i})
            elif a in pend_addrs:
                ents[e] = (a, frozenset({i}))
        for a in pend_addrs:
            if any(ea == a for ea, _ in ents.values()):
                continue
            cands = [e for (e, na) in newest_reports
                     if na == a and (self.a.no_id_bound or e > last) and e not in ents]
            if cands:
                ents[max(cands)] = (a, frozenset())
        return (True, False, mk_entries(ents), cur, tuple(dcur), frozenset(pend_addrs),
                frozenset(), (), D[8])


def P_restored_or_normal(D, name):
    """A client is served once it restored (or outside a recovery)."""
    return (not D[1]) or name in D[6]


# ---------------------------------------------------------------------------

def check(st, events, names):
    mem, P, S, D, ghost = st[:5]
    gmsg = dict(ghost)
    for i in range(NSUB):
        held, ring, rel, cur, newest, dlv, ipc, rr = S[i]
        for (e, a, m) in held:
            if mem[a] != m:
                return f"MemSafe: S{i} holds entry {e} (message {m}) but address {a} now holds {mem[a]}"
        for (e, a) in ring:
            if gmsg.get(e) is not None and mem[a] != gmsg[e]:
                return f"MemSafe: S{i} was pushed entry {e} whose memory was reused"
    for ev in events:
        if ev[0] == "deliver":
            _, i, m, e = ev
            if names.get((i, m), 0) > 1:
                return f"NoDup: S{i} was handed message {m} twice"
        if ev[0] == "answer":
            _, eid, m = ev
            if gmsg.get(eid) != m:
                return f"RightAnswer: a repeated publish of message {m} was answered with entry {eid}"
    if D[0] and not D[1]:
        ents = entries_of(D)
        for i in range(NSUB):
            for (e, a, m) in S[i][0]:
                if e not in ents or i not in ents[e][1]:
                    return f"RefAgree: S{i} holds entry {e} without a daemon reference"
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--publishes", type=int, default=3)
    ap.add_argument("--crashes", type=int, default=1)
    ap.add_argument("--no-grace", action="store_true")
    ap.add_argument("--no-pending", action="store_true")
    ap.add_argument("--no-delivered", action="store_true")
    ap.add_argument("--no-id-bound", action="store_true")
    args = ap.parse_args()
    m = Model(args)

    init = m.init()
    # Delivery counts travel with the state through the subscriber's
    # `delivered` set; a delivery of a message already in it is a duplicate.
    seen = {init: None}
    queue = collections.deque([init])
    transitions = 0
    while queue:
        st = queue.popleft()
        for (label, nxt) in m.succ(st):
            transitions += 1
            core, events = nxt[:5], nxt[5:]
            counts = {}
            for ev in events:
                if ev[0] == "deliver":
                    _, i, msg, e = ev
                    already = msg in st[2][i][5]
                    counts[(i, msg)] = 2 if already else 1
            err = check(core, events, counts)
            if err:
                trace = [label]
                p = st
                while seen[p] is not None:
                    prev, lab = seen[p]
                    trace.append(lab)
                    p = prev
                trace.reverse()
                print(f"VIOLATION after {len(seen)} states: {err}")
                print("  trace: " + " -> ".join(trace))
                return 1
            if core not in seen:
                seen[core] = (st, label)
                queue.append(core)
    print(f"OK: {len(seen)} states, {transitions} transitions, no violation "
          f"(publishes={args.publishes}, crashes={args.crashes}, subscribers={NSUB}, "
          f"addresses={ADDRS}, depth={DEPTH})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
