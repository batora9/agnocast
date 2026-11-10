#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Summarize run_fault_flows_paper.bash results.

    scripts/consistency/summarize_fault_flows.py results/fault_flows

Writes summary.md and summary.csv into the results directory and prints the
table. One row per cell (scenario, or scenario__baseline): how many trials
passed every check, how many passed each check, and how long the scenario's
own recovery took (event_ms: from the failure until the scenario's signal --
the client removed, the faulting process gone, the new segment in place).
"""

import csv
import glob
import json
import os
import statistics
import sys


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def fmt(v, digits=0):
    if v is None:
        return "-"
    return f"{v:.{digits}f}"


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    root = sys.argv[1]
    cells = {}
    for path in sorted(glob.glob(os.path.join(root, "*", "trial_*", "metrics.json"))):
        cell = os.path.basename(os.path.dirname(os.path.dirname(path)))
        try:
            cells.setdefault(cell, []).append(json.load(open(path)))
        except (OSError, json.JSONDecodeError):
            cells.setdefault(cell, []).append(None)

    rows = []
    for cell, trials in sorted(cells.items()):
        ok = [t for t in trials if t is not None]
        unsupported = sum(1 for t in ok if not t.get("supported", True))
        supported = [t for t in ok if t.get("supported", True)]
        passed = sum(1 for t in supported if t.get("passed"))
        check_names = []
        for t in supported:
            for k in t.get("checks", {}):
                if k not in check_names:
                    check_names.append(k)
        per_check = {
            k: sum(1 for t in supported if t.get("checks", {}).get(k)) for k in check_names
        }
        event_ms = [t["event_ms"] for t in supported if t.get("event_ms", -1) >= 0]
        event_pub = [t["event_published"] for t in supported if "event_published" in t]
        post_recv = [t["post_received_min"] for t in supported if "post_received_min" in t]
        rows.append({
            "cell": cell,
            "trials": len(trials),
            "unsupported": unsupported,
            "passed": passed,
            "supported": len(supported),
            "checks": per_check,
            "event_ms_median": statistics.median(event_ms) if event_ms else None,
            "event_ms_p95": pct(event_ms, 0.95),
            "event_ms_max": max(event_ms) if event_ms else None,
            "event_published_median": statistics.median(event_pub) if event_pub else None,
            "post_received_min_median": statistics.median(post_recv) if post_recv else None,
        })

    lines = [
        "| cell | passed | event ms (median / p95 / max) | publishes during event "
        "(median) | min deliveries after (median) | per check |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        passed = f"{r['passed']}/{r['supported']}"
        if r["unsupported"]:
            passed += f" ({r['unsupported']} unsupported)"
        checks = ", ".join(f"{k} {v}/{r['supported']}" for k, v in r["checks"].items())
        lines.append(
            f"| {r['cell']} | {passed} | {fmt(r['event_ms_median'])} / {fmt(r['event_ms_p95'])}"
            f" / {fmt(r['event_ms_max'])} | {fmt(r['event_published_median'])} |"
            f" {fmt(r['post_received_min_median'])} | {checks} |")
    table = "\n".join(lines)
    print(table)
    with open(os.path.join(root, "summary.md"), "w") as f:
        f.write(table + "\n")
    with open(os.path.join(root, "summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cell", "trials", "supported", "passed", "unsupported", "event_ms_median",
                    "event_ms_p95", "event_ms_max", "event_published_median",
                    "post_received_min_median", "checks"])
        for r in rows:
            w.writerow([r["cell"], r["trials"], r["supported"], r["passed"], r["unsupported"],
                        r["event_ms_median"], r["event_ms_p95"], r["event_ms_max"],
                        r["event_published_median"], r["post_received_min_median"],
                        json.dumps(r["checks"])])
    return 0


if __name__ == "__main__":
    sys.exit(main())
