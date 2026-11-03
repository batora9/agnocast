#!/usr/bin/env python3
"""Summarize daemon-crash rollback trials (run_daemon_crash_paper.bash).

Usage:
  python3 scripts/consistency/summarize_daemon_crash.py results/daemon_crash
  python3 scripts/consistency/summarize_daemon_crash.py results/daemon_crash --csv out.csv

Per cell (kill, fault/<point>, baseline), counting trials whose daemon really
crashed:
  survived      every client process is alive at the end (and the churn
                process exited cleanly on SIGINT)
  flow          publishes and deliveries to every subscriber went on while the
                daemon was down
  restored      membership after the restart equals the golden runs'
  consistent    the segment invariants and its agreement with the daemon's
                registrations hold (DEBUG_CHECK_TOPIC: 0 violations)
  leak_free     the publisher's outstanding entries are back to qos_depth
                (+1 in flight) once the holders let go
  rolled_back   trials where some holder (daemon or client) died inside a
                segment operation and the undo log rolled it back
  recovery_ms   the restarted daemon's recovery time (median / max)
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Any

MEMBERSHIP_KEYS = ("topics", "publishers", "subscribers", "publisher_num", "subscriber_num")


def load(path: Path) -> dict[str, Any] | None:
    try:
        with path.open() as fp:
            return json.load(fp)
    except (OSError, ValueError):
        return None


def membership(snap: dict[str, Any]) -> dict[str, Any]:
    return {k: snap.get(k) for k in MEMBERSHIP_KEYS}


def outstanding_ok(trial: Path, qos_depth: int) -> bool:
    try:
        parts = (trial / "outstanding.txt").read_text().split()
        return qos_depth <= int(parts[2]) <= qos_depth + 1
    except (OSError, IndexError, ValueError):
        return False


def golden_membership(root: Path) -> dict[str, Any] | None:
    snaps = [load(p) for p in sorted((root / "golden").glob("run_*/snapshot.json"))]
    snaps = [s for s in snaps if s is not None]
    if not snaps:
        return None
    first = membership(snaps[0])
    for other in snaps[1:]:
        if membership(other) != first:
            print("WARNING: golden runs disagree on membership", file=sys.stderr)
    return first


def summarize_cell(name: str, cell: Path, golden: dict[str, Any] | None, qos_depth: int):
    trials = sorted(cell.glob("run_*"))
    row = {
        "cell": name, "trials": len(trials), "crashed": 0, "survived": 0, "flow": 0,
        "restored": 0, "consistent": 0, "leak_free": 0, "rolled_back": 0,
        "orphan_pub": 0, "orphan_sub": 0, "recovery_ms_median": "", "recovery_ms_max": "",
        "incomplete": 0,
    }
    recovery_ms = []
    for trial in trials:
        metrics = load(trial / "metrics.json")
        if metrics is None:
            row["incomplete"] += 1
            continue
        if not metrics.get("crashed"):
            continue
        row["crashed"] += 1
        churn_ok = not metrics.get("churn") or metrics.get("churn_exit") == 0
        if metrics.get("survivors") == metrics.get("clients") and churn_ok:
            row["survived"] += 1
        if metrics.get("outage_published", 0) > 0 and metrics.get("outage_received_min", 0) > 0:
            row["flow"] += 1
        snap = load(trial / "snapshot.json")
        if snap is not None and golden is not None and membership(snap) == golden:
            row["restored"] += 1
        check = (snap or {}).get("check") or {}
        if check.get("ok") and check.get("shm_violations") == 0 and \
                check.get("registry_violations") == 0:
            row["consistent"] += 1
        if check.get("rollback_count", 0) > 0:
            row["rolled_back"] += 1
        if outstanding_ok(trial, qos_depth):
            row["leak_free"] += 1
        rec = load(trial / "recovery.json") or {}
        row["orphan_pub"] += int(rec.get("orphan_publisher_num", 0))
        row["orphan_sub"] += int(rec.get("orphan_subscriber_num", 0))
        if rec.get("recovery_ms", -1) >= 0 and rec.get("restored_num", 0) > 0:
            recovery_ms.append(float(rec["recovery_ms"]))
    if recovery_ms:
        row["recovery_ms_median"] = f"{statistics.median(recovery_ms):.2f}"
        row["recovery_ms_max"] = f"{max(recovery_ms):.2f}"
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", type=Path)
    parser.add_argument("--qos-depth", type=int, default=10)
    parser.add_argument("--csv", type=Path)
    args = parser.parse_args()

    golden = golden_membership(args.root)
    if golden is None:
        print(f"WARNING: no golden snapshot under {args.root}/golden; 'restored' stays 0",
              file=sys.stderr)

    cells = []
    if (args.root / "kill").is_dir():
        cells.append(("kill", args.root / "kill"))
    for point in sorted((args.root / "fault").glob("*")) if (args.root / "fault").is_dir() else []:
        cells.append((f"fault:{point.name}", point))
    if (args.root / "baseline").is_dir():
        cells.append(("baseline(no-reconnect)", args.root / "baseline"))
    if not cells:
        print(f"ERROR: no trials under {args.root}", file=sys.stderr)
        return 1

    rows = [summarize_cell(name, cell, golden, args.qos_depth) for name, cell in cells]
    fields = list(rows[0].keys())
    writer = csv.DictWriter(sys.stdout, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    if args.csv:
        with args.csv.open("w", newline="") as fp:
            w = csv.DictWriter(fp, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
