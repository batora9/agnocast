#!/usr/bin/env python3
"""Aggregate sweep results and render comparison figures.

  python3 plot_results.py \
    --data results/kmod   --label "kernel module" --color "#1f77b4" \
    --data results/daemon --label "user daemon"   --color "#d62728" \
    --output-dir figures/

Percentiles are pooled: every per-message sample from every iteration, topic,
publisher and subscriber of a configuration goes into one array, and the
percentile is taken from that array. Averaging per-process percentiles instead
would understate the tail, because a rare stall in one process gets diluted by
the well-behaved ones.
"""

import argparse
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.colors as mcolors  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# One size for titles, axes, ticks, and legends so line plots stay readable
# when inserted at column width. Heatmaps bump further in plot_heatmaps().
PLOT_FONT = 12

matplotlib.rcParams.update(
    {
        "font.family": "serif",
        "font.size": PLOT_FONT,
        "axes.labelsize": PLOT_FONT,
        "axes.titlesize": PLOT_FONT,
        "xtick.labelsize": PLOT_FONT,
        "ytick.labelsize": PLOT_FONT,
        "legend.fontsize": PLOT_FONT,
        "figure.figsize": (5.4, 3.8),
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.04,
        "lines.linewidth": 1.6,
        "lines.markersize": 6,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "grid.linewidth": 0.6,
    }
)


def comma_format(x, _pos):
    """Tick labels with a comma every three digits (1,000 not 1000)."""
    if not np.isfinite(x):
        return ""
    if abs(x - round(x)) < 1e-6:
        return f"{int(round(x)):,}"
    return f"{x:,.1f}"


COMMA_FORMATTER = mticker.FuncFormatter(comma_format)

DEFAULT_COLORS = ["#1f77b4", "#d62728", "#2ca02c", "#ff7f0e"]

# metric -> (csv glob, column holding nanosecond samples)
#
# Paper TABLE V names map as follows (ipc_shared_ptr §VII):
#   publish  = loan() through publish() completion (allocation, payload,
#              publish ioctl/UDS, and subscriber notification)
#   receive  = core receive-side ioctl / UDS receive_msg only
#   e2e      = subscriber-callback entry minus the publisher stamp
# publish_ipc / receive_ipc are the same columns as the metadata round trip;
# receive is the paper name for receive_ipc.
METRIC_COLUMNS = {
    "publish": ("publisher_*_latencies.csv", "publish_latency_ns"),
    "publish_ipc": ("publisher_*_latencies.csv", "publish_ipc_ns"),
    "e2e": ("subscriber_*_topic_*_latencies.csv", "e2e_latency_ns"),
    "receive_ipc": ("subscriber_*_topic_*_latencies.csv", "receive_ipc_ns"),
    "receive": ("subscriber_*_topic_*_latencies.csv", "receive_ipc_ns"),
}

METRIC_TITLES = {
    "publish": "Publish",
    "publish_ipc": "Publish IPC round trip",
    "e2e": "End-to-end",
    "receive_ipc": "Receive",
    "receive": "Receive",
}

DEFAULT_METRICS = ("publish", "publish_ipc", "e2e", "receive")
PAPER_METRICS = ("publish", "receive", "e2e")

# 1-D paper sweeps A/B. Sweep C is the T×S heatmap below. "rate" is optional
# and not in the paper; it lives under sweep_rate/.
SWEEPS = {
    "a": ("sweep_a", r"num_topics_(\d+)", "Number of topics", "topics"),
    "b": ("sweep_b", r"num_subscribers_(\d+)", "Number of subscribers", "subscribers"),
    "rate": ("sweep_rate", r"rate_hz_(\d+)", "Publish rate (Hz)", "rate"),
}


def pool_metric_us(config_dir: Path, metric: str) -> np.ndarray:
    """Concatenate every per-message sample of one metric under a config dir."""
    pattern, column = METRIC_COLUMNS[metric]
    chunks = []
    for iter_dir in sorted(config_dir.glob("iter_*")):
        for path in iter_dir.glob(pattern):
            try:
                df = pd.read_csv(path, usecols=[column])
            except (ValueError, KeyError):
                continue
            if not df.empty:
                chunks.append(df[column].to_numpy())
    if not chunks:
        return np.empty(0, dtype=np.float64)
    return np.concatenate(chunks) / 1000.0


def summarize(values_us: np.ndarray) -> dict:
    return {
        "count": int(values_us.size),
        "p50": float(np.percentile(values_us, 50)),
        "p99": float(np.percentile(values_us, 99)),
        "p999": float(np.percentile(values_us, 99.9)),
        "p9999": float(np.percentile(values_us, 99.99)),
        "max": float(values_us.max()),
    }


def pooled_stats(sweep_dir: Path, param_regex: str, metrics) -> pd.DataFrame:
    rows = []
    if not sweep_dir.is_dir():
        return pd.DataFrame()
    for config_dir in sorted(p for p in sweep_dir.iterdir() if p.is_dir()):
        match = re.search(param_regex, config_dir.name)
        if not match:
            continue
        for metric in metrics:
            values = pool_metric_us(config_dir, metric)
            if values.size == 0:
                continue
            rows.append(
                {"sweep_value": float(match.group(1)), "metric": metric, **summarize(values)}
            )
    return pd.DataFrame(rows)


def pooled_stats_2d(sweep_dir: Path, metrics) -> pd.DataFrame:
    rows = []
    if not sweep_dir.is_dir():
        return pd.DataFrame()
    for config_dir in sorted(p for p in sweep_dir.iterdir() if p.is_dir()):
        match = re.match(r"nt_(\d+)_ns_(\d+)$", config_dir.name)
        if not match:
            continue
        for metric in metrics:
            values = pool_metric_us(config_dir, metric)
            if values.size == 0:
                continue
            rows.append(
                {
                    "num_topics": int(match.group(1)),
                    "num_subscribers": int(match.group(2)),
                    "metric": metric,
                    **summarize(values),
                }
            )
    return pd.DataFrame(rows)


def save_figure(fig, name, output_dir):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / f"{name}.pdf")
    fig.savefig(out / f"{name}.png")
    plt.close(fig)
    print(f"  saved {name}.pdf / .png")


def plot_vs_sweep(datasets, metric, xlabel, filename, output_dir):
    fig, ax = plt.subplots()
    plotted = False

    for label, color, stats in datasets:
        data = stats[stats["metric"] == metric].sort_values("sweep_value")
        if data.empty:
            continue
        # to_numpy(): older matplotlib cannot index a pandas Series directly.
        prefix = f"{label} " if len(datasets) > 1 else ""
        x = data["sweep_value"].to_numpy()
        ax.plot(x, data["p50"].to_numpy(), label=f"{prefix}p50", color=color, marker="o")
        ax.plot(
            x,
            data["p999"].to_numpy(),
            label=f"{prefix}p99.9",
            color=color,
            linestyle="--",
            marker="v",
            alpha=0.85,
        )
        plotted = True

    if not plotted:
        plt.close(fig)
        return

    ax.set_xlabel(xlabel)
    ax.set_ylabel("Latency (us)")
    ax.set_title(METRIC_TITLES[metric])
    ax.yaxis.set_major_formatter(COMMA_FORMATTER)
    ax.legend(loc="best", ncol=1, framealpha=0.92)
    save_figure(fig, filename, output_dir)


def plot_cdf(datasets, output_dir, metrics, suffix=""):
    for metric in metrics:
        fig, ax = plt.subplots()
        plotted = False

        for label, color, samples in datasets:
            values = samples.get(metric)
            if values is None or values.size == 0:
                continue
            ordered = np.sort(values)
            cdf = np.arange(1, ordered.size + 1) / ordered.size
            ax.plot(ordered, cdf, label=label, color=color)
            plotted = True

        if not plotted:
            plt.close(fig)
            continue

        ax.set_xlabel(f"{METRIC_TITLES[metric]} latency (us)")
        ax.set_ylabel("CDF")
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(COMMA_FORMATTER)
        ax.legend(loc="lower right", ncol=1, framealpha=0.92)
        save_figure(fig, f"cdf_{metric}{suffix}", output_dir)


def plot_heatmaps(datasets_2d, metric, output_dir):
    pivots = []
    for label, stats in datasets_2d:
        if stats.empty:
            continue
        subset = stats[stats["metric"] == metric]
        if subset.empty:
            continue
        pivots.append(
            (
                label,
                subset.pivot(
                    index="num_topics", columns="num_subscribers", values="p999"
                ).sort_index(),
            )
        )

    if not pivots:
        print(f"  no 2D data for {metric}, skipping heatmap")
        return

    vmin = min(h.min().min() for _, h in pivots)
    vmax = max(h.max().max() for _, h in pivots)
    norm = mcolors.LogNorm(vmin=max(vmin, 0.01), vmax=vmax)

    # Same compact side-by-side layout as the original paper_fifo90_warmup5
    # figures; only the type is larger than the committed 8 pt.
    heatmap_font = 12
    with plt.rc_context(
        {
            "font.size": heatmap_font,
            "axes.labelsize": heatmap_font,
            "axes.titlesize": heatmap_font,
            "xtick.labelsize": heatmap_font,
            "ytick.labelsize": heatmap_font,
            "savefig.pad_inches": 0.03,
            "axes.grid": False,
        }
    ):
        fig, axes = plt.subplots(1, len(pivots), figsize=(2.6 * len(pivots), 2.8))
        if len(pivots) == 1:
            axes = [axes]

        image = None
        for ax, (label, heatmap) in zip(axes, pivots):
            ax.grid(False)
            image = ax.imshow(
                heatmap.values, aspect="auto", cmap="YlOrRd", origin="lower", norm=norm
            )
            ax.set_xticks(range(len(heatmap.columns)))
            ax.set_xticklabels(heatmap.columns.astype(int))
            ax.set_yticks(range(len(heatmap.index)))
            ax.set_yticklabels(heatmap.index.astype(int))
            ax.set_title(label, fontsize=heatmap_font)

        axes[0].set_ylabel("Number of topics")
        for ax in axes[1:]:
            ax.set_yticklabels([])

        cbar = fig.colorbar(image, ax=axes, shrink=0.8, label="p99.9 latency (us)")
        cbar.ax.tick_params(labelsize=heatmap_font)
        fig.text(
            0.42,
            -0.03,
            "Number of subscribers",
            ha="center",
            va="top",
            fontsize=heatmap_font,
        )
        save_figure(fig, f"heatmap_{metric}_p999", output_dir)


def write_stats_table(all_stats, output_dir, metrics):
    """Dump every pooled percentile to CSV so the numbers are inspectable."""
    if not all_stats:
        return
    combined = pd.concat(all_stats, ignore_index=True)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "pooled_stats.csv"
    combined.to_csv(path, index=False)
    print(f"  saved {path.name} ({len(combined)} rows)")

    for metric in metrics:
        subset = combined[combined["metric"] == metric]
        if subset.empty or subset["label"].nunique() < 2:
            continue
        print(f"\n  {METRIC_TITLES[metric]} (us), pooled:")
        pivot = subset[subset["sweep"] != "c"].pivot_table(
            index=["sweep", "sweep_value"], columns="label", values=["p50", "p999"]
        )
        print(pivot.to_string(float_format=lambda v: f"{v:8.3f}"))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--data", action="append", required=True, help="results root per backend")
    parser.add_argument("--label", action="append", help="label per --data")
    parser.add_argument("--color", action="append", help="color per --data")
    parser.add_argument("--output-dir", default="figures")
    parser.add_argument(
        "--sweep", choices=["a", "b", "c", "rate", "cdf", "all"], default="all"
    )
    parser.add_argument(
        "--metrics",
        default=",".join(DEFAULT_METRICS),
        help="comma-separated metrics, or 'paper' for publish,receive,e2e",
    )
    return parser.parse_args()


def selected_metrics(raw: str):
    if raw.strip().lower() == "paper":
        names = list(PAPER_METRICS)
    else:
        names = [m.strip() for m in raw.split(",") if m.strip()]
    unknown = [m for m in names if m not in METRIC_COLUMNS]
    if unknown:
        raise SystemExit(f"unknown metric(s): {', '.join(unknown)}")
    return names


def main():
    args = parse_args()

    count = len(args.data)
    labels = list(args.label or [])
    labels += [f"dataset {i}" for i in range(len(labels), count)]
    colors = list(args.color or [])
    colors += DEFAULT_COLORS[len(colors) : count]

    output_dir = Path(args.output_dir)
    metrics = selected_metrics(args.metrics)
    collected = []

    for key, (dirname, regex, xlabel, short) in SWEEPS.items():
        if args.sweep not in (key, "all"):
            continue
        print(f"Sweep {key.upper()}: {xlabel}")

        datasets = []
        for data_dir, label, color in zip(args.data, labels, colors):
            sweep_dir = Path(data_dir) / dirname
            if not sweep_dir.is_dir():
                sweep_dir = Path(data_dir)
            stats = pooled_stats(sweep_dir, regex, metrics)
            if stats.empty:
                print(f"  no data under {sweep_dir}")
                continue
            datasets.append((label, color, stats))
            collected.append(stats.assign(label=label, sweep=key))

        for metric in metrics:
            plot_vs_sweep(datasets, metric, xlabel, f"{metric}_vs_{short}", output_dir)

    if args.sweep in ("c", "all"):
        print("Sweep C: topics x subscribers")
        datasets_2d = []
        for data_dir, label in zip(args.data, labels):
            sweep_dir = Path(data_dir) / "sweep_c"
            if not sweep_dir.is_dir():
                sweep_dir = Path(data_dir)
            stats_2d = pooled_stats_2d(sweep_dir, metrics)
            datasets_2d.append((label, stats_2d))
            if not stats_2d.empty:
                collected.append(
                    stats_2d.assign(
                        label=label,
                        sweep="c",
                        sweep_value=stats_2d["num_topics"] * 1000
                        + stats_2d["num_subscribers"],
                    )
                )
        for metric in metrics:
            plot_heatmaps(datasets_2d, metric, output_dir)

    if args.sweep in ("cdf", "all"):
        print("Latency CDFs")
        # For "all", the CDF pools every configuration of every sweep, which is
        # a distribution over the whole experiment rather than one operating
        # point. Point --data at a single configuration directory for a CDF of
        # that configuration alone.
        datasets = []
        for data_dir, label, color in zip(args.data, labels, colors):
            root = Path(data_dir)
            config_dirs = (
                [root]
                if list(root.glob("iter_*"))
                else [p.parent for p in root.glob("*/*/iter_0")]
            )
            samples = {}
            for metric in metrics:
                chunks = [pool_metric_us(c, metric) for c in config_dirs]
                chunks = [c for c in chunks if c.size]
                samples[metric] = np.concatenate(chunks) if chunks else np.empty(0)
            if any(v.size for v in samples.values()):
                datasets.append((label, color, samples))
        plot_cdf(datasets, output_dir, metrics)

    write_stats_table(collected, output_dir, metrics)
    print("\nDone.")


if __name__ == "__main__":
    main()
