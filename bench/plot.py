#!/usr/bin/env python3
"""Turn results.csv from run_benchmarks.sh into plots + a text summary.

Outputs (next to the CSV by default):
  mem_scaling.png  -- peak memory vs file count (the "bounded vs growing" story)
  peak_by_bench.png -- grouped bars of peak memory per tool for each benchmark
  wall_by_bench.png -- grouped bars of wall time per tool for each benchmark

Also prints a compact table to stdout. Uses matplotlib (Agg); no pandas.
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TOOLS = ["copy2", "rsync", "rclone", "fpsync"]
TOOL_COLOR = {"copy2": "#1b9e77", "rsync": "#d95f02",
              "rclone": "#7570b3", "fpsync": "#e7298a"}


def load(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            for k in ("nfiles", "wall_s", "peak_pss_mb", "peak_rss_mb", "rc"):
                try:
                    r[k] = float(r[k])
                except (ValueError, KeyError):
                    r[k] = float("nan")
            rows.append(r)
    return rows


def latest_per_key(rows, keyfn):
    """Keep only the last row per key (CSV is append-only across reruns)."""
    out = {}
    for r in rows:
        out[keyfn(r)] = r
    return list(out.values())


def plot_mem_scaling(rows, outdir):
    scaling = [r for r in rows if r["bench"] == "mem_scaling"]
    if not scaling:
        return None
    scaling = latest_per_key(scaling, lambda r: (r["tool"], r["nfiles"]))
    by_tool = defaultdict(list)
    for r in scaling:
        by_tool[r["tool"]].append((r["nfiles"], r["peak_pss_mb"]))

    fig, ax = plt.subplots(figsize=(7, 5))
    for tool in TOOLS:
        pts = sorted(by_tool.get(tool, []))
        if not pts:
            continue
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax.plot(xs, ys, marker="o", label=tool, color=TOOL_COLOR.get(tool))
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("number of files")
    ax.set_ylabel("peak memory (PSS, MB)")
    ax.set_title("Memory scaling: peak RSS vs file count (local, empty dst)")
    ax.grid(True, which="both", ls=":", alpha=0.5)
    ax.legend()
    out = os.path.join(outdir, "mem_scaling.png")
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def _grouped_bars(rows, metric, title, ylabel, out, benches):
    rows = latest_per_key(rows, lambda r: (r["bench"], r["tool"]))
    data = defaultdict(dict)  # bench -> tool -> value
    for r in rows:
        if r["bench"] in benches:
            data[r["bench"]][r["tool"]] = r[metric]
    benches = [b for b in benches if b in data]
    if not benches:
        return None

    fig, ax = plt.subplots(figsize=(max(7, 1.5 * len(benches)), 5))
    n = len(TOOLS)
    width = 0.8 / n
    for i, tool in enumerate(TOOLS):
        xs = [j + (i - (n - 1) / 2) * width for j in range(len(benches))]
        ys = [data[b].get(tool, 0) for b in benches]
        ax.bar(xs, ys, width=width, label=tool, color=TOOL_COLOR.get(tool))
    ax.set_xticks(range(len(benches)))
    ax.set_xticklabels(benches, rotation=20, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, axis="y", ls=":", alpha=0.5)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


def print_table(rows):
    rows = latest_per_key(rows, lambda r: (r["bench"], r["nfiles"], r["tool"]))
    rows.sort(key=lambda r: (r["bench"], r["nfiles"], r["tool"]))
    print(f"\n{'bench':<13}{'nfiles':>9}  {'tool':<7}"
          f"{'wall_s':>8}{'files/s':>10}{'MiB/s':>9}{'peak_mb':>9}{'rc':>3}")
    print("-" * 70)
    for r in rows:
        wall = r["wall_s"] or float("nan")
        fps = r["nfiles"] / wall if wall else float("nan")
        try:
            fs = float(r.get("filesize_bytes", 0))
        except (TypeError, ValueError):
            fs = 0
        mibps = (r["nfiles"] * fs) / (1024 * 1024) / wall if wall else float("nan")
        print(f"{r['bench']:<13}{int(r['nfiles']):>9}  {r['tool']:<7}"
              f"{r['wall_s']:>8.2f}{fps:>10.0f}{mibps:>9.1f}"
              f"{r['peak_pss_mb']:>9.1f}{int(r['rc']):>3}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=os.path.join(os.path.dirname(__file__),
                                                  "results.csv"))
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        print(f"no results at {args.csv}", file=sys.stderr)
        return 1
    outdir = args.outdir or os.path.dirname(os.path.abspath(args.csv))
    rows = load(args.csv)

    made = []
    p = plot_mem_scaling(rows, outdir)
    if p:
        made.append(p)

    bar_benches = ["crawling", "metadata", "hardlinks", "transfer",
                   "mem_hlinks", "mem_flat", "mem_deep"]
    p = _grouped_bars(rows, "peak_pss_mb",
                      "Peak memory by benchmark", "peak PSS (MB)",
                      os.path.join(outdir, "peak_by_bench.png"), bar_benches)
    if p:
        made.append(p)
    p = _grouped_bars(rows, "wall_s",
                      "Wall time by benchmark", "wall time (s)",
                      os.path.join(outdir, "wall_by_bench.png"), bar_benches)
    if p:
        made.append(p)

    print_table(rows)
    print("\nwrote:")
    for m in made:
        print(f"  {m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
