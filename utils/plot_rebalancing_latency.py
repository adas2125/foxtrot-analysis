import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
from statistics import median

import numpy as np
import matplotlib.pyplot as plt

UTC = timezone.utc
OPERATIONS = ("Read", "Update")
RUN_DIR = (
    Path(__file__).resolve().parents[1]
    / "rebalancing-latency-experiment-artifacts/20260911-055959"
)
TRIM_S = 30

def read_migration(path):
    """returns the start and the end of the migration"""
    starts, ends = [], []
    for line in path.read_text().splitlines():
        timestamp, _, message = line.partition(" ")
        # Exact messages exclude the duplicate '+ echo ...' shell-trace lines.
        if re.fullmatch(r"=== Resharding \d+ slots ===", message):
            events = starts
        elif message == "=== Re-enabling full coverage ===":
            events = ends
        else:
            continue
        timestamp = re.sub(r"(\.\d{6})\d+", r"\1", timestamp)
        events.append(datetime.fromisoformat(timestamp.replace("Z", "+00:00")))
    
    # Unpacking deliberately requires exactly one migration start and end.
    (start,) = starts
    (end,) = ends
    assert start.tzinfo is not None and end.tzinfo is not None
    assert start < end, "Migration end must follow its start"
    return start.timestamp(), end.timestamp()


def read_latencies(run_dir):
    """returns latencies binned by second, the estimated UTC origin, and the last offset"""
    bins = {name: defaultdict(list) for name in OPERATIONS}
    counts = dict.fromkeys(OPERATIONS, 0)
    origins, total, last_offset = [], 0, 0.0

    # reading latency logs
    for path in sorted((run_dir / "nosqlmark/backend-logs").glob("timeseries*.log")):
        with path.open() as handle:
            for line in handle:
                wall, offset, operation, latency = line.strip().split(";")
                offset, latency = float(offset) / 1000, float(latency)
                assert math.isfinite(offset) and math.isfinite(latency) and latency >= 0
                
                # Sample throughout the export to estimate the UTC time origin.
                if total % 1000 == 0:
                    timestamp = datetime.strptime(wall, "%Y-%m-%d %H:%M:%S,%f")
                    origins.append(timestamp.replace(tzinfo=UTC).timestamp() - offset - latency / 1000)
                    
                # The first operation can have a tiny negative warm-up-reset offset.
                index = math.floor(max(0, offset))

                # bin the latency by its index (1 second intervals)
                bins[operation][index].append(latency)
                counts[operation] += 1
                total += 1
                last_offset = max(last_offset, offset)

    summary = json.loads((run_dir / "nosqlmark/result/summary.json").read_text())

    # asserting counts for read and write operations
    for name in OPERATIONS:
        assert counts[name] > 0 and counts[name] == int(summary[name]["Count"]), name
    assert total == int(summary["ALL"]["Count"]), "Raw and summary totals differ"
    assert not any(
        int(value.get("Count", 0))
        for name, value in summary.items()
        if "FAILED" in name or "TIMEDOUT" in name
    ), "Summary reports failed/timed-out operations"
    return bins, median(origins), last_offset

def bin_percentiles(bins):
    # rows correspond to the 3 percentiles: p50, p95, p99; columns correspond to the time bins
    curves = np.full((3, max(bins) + 1), np.nan)
    # looping over each second and its latencies
    for index, latencies in bins.items():
        values = np.sort(latencies)
        # Nearest-rank percentiles, computed from individual operations.
        ranks = np.ceil(np.array([0.50, 0.95, 0.99]) * len(values)).astype(int) - 1
        # p50, p95, p99 percentiles for this second
        curves[:, index] = values[ranks]
    return curves

def plot_run(run_dir):
    metadata = json.loads((run_dir / "run.json").read_text())
    start, end = read_migration(run_dir / "reshard.log")
    bins, origin, duration = read_latencies(run_dir)
    window_start = start - TRIM_S
    window_start = start - TRIM_S

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True, constrained_layout=True)
    for ax, operation in zip(axes, OPERATIONS):
        curves = bin_percentiles(bins[operation])
        seconds = np.arange(curves.shape[1]) + 0.5 + (origin - window_start)
        visible = (seconds >= 0) & (seconds <= end - window_start + TRIM_S)

        # plotting the lines over seconds
        for label, values in zip(("p50", "p95", "p99"), curves):
            # values one row correspond to the current percentile across all seconds
            ax.plot(seconds[visible], values[visible], label=label)
        
        # shading in the migration period
        ax.axvspan(start - window_start, end - window_start, color="gray", alpha=0.15)
        ax.axvline(start - window_start, color="black", linestyle="--", label="Migration start")
        ax.axvline(end - window_start, color="black", linestyle=":", label="Migration end")
        ax.set(title=operation, ylabel="Latency (ms)")
        ax.set_ylim(bottom=0)
        ax.grid(alpha=0.2)
    axes[-1].set(xlabel="Seconds since displayed window began", xlim=(0, end - window_start + TRIM_S))

    axes[-1].set(xlabel="Seconds since displayed window began", xlim=(0, end - window_start + TRIM_S))
    fig.suptitle(f"{metadata['RUN_ID']} — {metadata['TARGET_RPS']} RPS — 1s bins")
    fig.savefig(run_dir / "latency-over-time.png", dpi=150)
    plt.close(fig)
    print(f"Saved: {run_dir / 'latency-over-time.png'}")

if __name__ == "__main__":
    plot_run(RUN_DIR)
