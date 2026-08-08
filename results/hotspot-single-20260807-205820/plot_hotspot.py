import csv
import sys
from collections import defaultdict
from pathlib import Path
import matplotlib.pyplot as plt

# Constant for length of test
PLOT_END = 600

if len(sys.argv) != 2:
    raise SystemExit(f"Usage: {sys.argv[0]} RUN_DIRECTORY")
run_dir = Path(sys.argv[1])
print(f"Plotting results from: {run_dir}")

throughput_file = run_dir / "hotspot-timeseries.csv"
cpu_file = run_dir / "redis-cpu-timeseries.csv"

throughput_time = []
throughput = []
cumulative_errors = []

# processing the throughput data from the CSV file
# example row: {'epoch': '1786053441.000', 'elapsed_seconds': '5.0', 'total_successes': '10000', 
    # 'throughput_ops_per_sec': '1994.3', 'cumulative_errors': '0'}
with throughput_file.open() as f:
    for row in csv.DictReader(f):
        throughput_time.append(float(row["elapsed_seconds"]))
        throughput.append(float(row["throughput_ops_per_sec"]))
        cumulative_errors.append(int(row["cumulative_errors"]))

# Estimate the workload-start epoch from epoch - elapsed.
with throughput_file.open() as f:
    first = next(csv.DictReader(f))
    workload_start_epoch = (
        float(first["epoch"]) - float(first["elapsed_seconds"])
    )


# Read Prometheus CPU data
cpu_by_pod = defaultdict(list)
with cpu_file.open(encoding="utf-8-sig", newline="") as f:
    nonempty_lines = (line for line in f if line.strip())
    reader = csv.DictReader(nonempty_lines)

    reader.fieldnames = [
        field.strip() for field in reader.fieldnames
    ]

    # exmample row: {'epoch': '1786053376', 'pod': 'redis-cluster-0', 'cpu_percent': '0.269600924537124'}
    for row in reader:
        relative_time = float(row["epoch"]) - workload_start_epoch
        cpu_by_pod[row["pod"].strip()].append(
            (relative_time, float(row["cpu_percent"]))
        )

# sorting by the relative time
for pod in cpu_by_pod:
    cpu_by_pod[pod].sort()

# Creating the plot
fig, axes = plt.subplots(
    2,
    1,
    figsize=(14, 10),
    sharex=True,
    gridspec_kw={"height_ratios": [2.0, 2.0]},
)

throughput_ax, cpu_ax = axes
# Panel 1: throughput
throughput_ax.plot(
    throughput_time,
    throughput,
    color="#1769aa",
    linewidth=1.7,
    label="Client throughput",
)
throughput_ax.set_ylabel("Throughput (ops/s)")
throughput_ax.set_title(
    "Foxtrot Autoscaling During a Single-Key Redis Hotspot",
    fontsize=15,
    weight="bold",
    pad=60,
)


# Panel 2: Redis CPU
selected_pods = [
    "redis-cluster-0",  # Original overloaded master
    "redis-cluster-1",  # Unaffected master
    "redis-cluster-2",  # Receives slots during scale-down
    "redis-cluster-6",  # Standby activated during scale-up
]

pod_labels = {
    "redis-cluster-0": "Pod 0",
    "redis-cluster-1": "Pod 1",
    "redis-cluster-2": "Pod 2",
    "redis-cluster-6": "Pod 6",
}

pod_colors = {
    "redis-cluster-0": "#d62728",
    "redis-cluster-1": "#7f7f7f",
    "redis-cluster-2": "#2ca02c",
    "redis-cluster-6": "#ff7f0e",
}

for pod in selected_pods:
    points = cpu_by_pod.get(pod, [])

    if not points:
        continue

    x = [point[0] for point in points]
    y = [point[1] for point in points]

    cpu_ax.plot(
        x,
        y,
        linewidth=1.6,
        label=pod_labels[pod],
        color=pod_colors[pod],
    )

cpu_ax.set_ylabel("Redis CPU (%)")
cpu_ax.legend(
    loc="lower right",
    ncol=2,
    fontsize=9,
    frameon=True,
)

for ax in axes:
    ax.set_xlim(0, PLOT_END)
    ax.grid(True, alpha=0.25)

fig.tight_layout(rect=[0, 0, 1, 0.93])

png_output = run_dir / "hotspot-autoscaling-timeseries.png"
fig.savefig(png_output, dpi=220, bbox_inches="tight")
print(f"Wrote {png_output}")
