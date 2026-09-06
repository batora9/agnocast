#!/usr/bin/env python3
"""Summarize metadata-consistency trials into the WiP table rows.

Usage:
  python3 scripts/consistency/summarize.py results/consistency
  python3 scripts/consistency/summarize.py --check-golden results/consistency/kmod/sub/golden
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


MEMBERSHIP_KEYS = ("topics", "publishers", "subscribers", "publisher_num", "subscriber_num")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fp:
        return json.load(fp)


def membership(snap: dict[str, Any]) -> dict[str, Any]:
    return {k: snap[k] for k in MEMBERSHIP_KEYS}


def membership_equal(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return membership(a) == membership(b)


def read_outstanding(path: Path) -> int | None:
    if not path.is_file():
        return None
    text = path.read_text().strip().split()
    if len(text) < 3:
        return None
    return int(text[2])


def publisher_lingering(path: Path) -> bool:
    """True if a publisher record remains after holders dropped their refs."""
    if not path.is_file():
        return True
    snap = load_json(path)
    if int(snap.get("publisher_num", 0)) > 0:
        return True
    return bool(snap.get("publishers"))


def check_golden(golden_dir: Path, crash_role: str, qos_depth: int) -> int:
    snaps = sorted(golden_dir.glob("run_*/snapshot.json"))
    if not snaps:
        print(f"ERROR: no golden snapshots under {golden_dir}", file=sys.stderr)
        return 1
    first = load_json(snaps[0])
    for snap_path in snaps[1:]:
        other = load_json(snap_path)
        if not membership_equal(first, other):
            print(f"ERROR: golden membership mismatch: {snaps[0]} vs {snap_path}", file=sys.stderr)
            return 1
    # Note: outstanding.txt (for sub crash) and after_drop.json (for pub crash)
    # are only meaningful for kill trials, not golden runs. Golden runs validate
    # membership consistency only.
    golden_out = golden_dir / "snapshot.json"
    golden_out.write_text(json.dumps(membership(first), indent=2) + "\n")
    print(f"golden ok ({len(snaps)} runs) -> {golden_out}")
    return 0


def summarize(root: Path, qos_depth: int) -> int:
    rows = []
    for backend in ("kmod", "daemon"):
        for crash_role in ("sub", "pub"):
            cell = root / backend / crash_role
            golden_path = cell / "golden" / "snapshot.json"
            if not golden_path.is_file():
                continue
            golden = load_json(golden_path)
            kill_snaps = sorted((cell / "kill").glob("run_*/snapshot.json"))
            n = len(kill_snaps)
            match = 0
            leak = 0
            for snap_path in kill_snaps:
                snap = load_json(snap_path)
                if membership_equal(golden, snap):
                    match += 1
                if crash_role == "sub":
                    outstanding = read_outstanding(snap_path.parent / "outstanding.txt")
                    # Sampled after the victim left membership (ref bits cleared).
                    # One extra in-flight publish is allowed (qos_depth or qos_depth+1).
                    if outstanding is None or outstanding < qos_depth or outstanding > qos_depth + 1:
                        leak += 1
                elif publisher_lingering(snap_path.parent / "after_drop.json"):
                    leak += 1
            rows.append((backend, crash_role, n, match, str(leak)))

    if not rows:
        print(f"ERROR: no completed cells under {root}", file=sys.stderr)
        return 1

    print("backend,crash_role,n,membership_match,leak_count")
    for backend, crash_role, n, match, leak_cell in rows:
        print(f"{backend},{crash_role},{n},{match},{leak_cell}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="results/consistency or a golden directory")
    parser.add_argument("--check-golden", action="store_true")
    parser.add_argument("--crash-role", choices=("sub", "pub"), default="sub")
    parser.add_argument("--qos-depth", type=int, default=10)
    args = parser.parse_args()
    if args.check_golden:
        return check_golden(args.root, args.crash_role, args.qos_depth)
    return summarize(args.root, args.qos_depth)


if __name__ == "__main__":
    sys.exit(main())
