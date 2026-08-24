"""Reporting helpers: CSV output and matplotlib plots."""
from __future__ import annotations

import csv
from typing import List

import matplotlib

matplotlib.use("Agg")  # headless backend so scripts never block on plt.show()
import matplotlib.pyplot as plt

UPLOAD_HEADER = [
    "File Size (MB)",
    "Time Taken (s)",
    "Upload Speed (Mbps)",
    "Multipart Threshold (bytes)",
    "Max Concurrency",
    "Multipart Chunksize (bytes)",
    "Use Threads",
]

DOWNLOAD_HEADER = [
    "File Size (MB)",
    "Time Taken (s)",
    "Download Speed (Mbps)",
    "Multipart Threshold (bytes)",
    "Max Concurrency",
    "Multipart Chunksize (bytes)",
    "Use Threads",
]

TUNING_HEADER = [
    "Multipart Threshold (bytes)",
    "Max Concurrency",
    "Multipart Chunksize (bytes)",
    "Use Threads",
    "Time Taken (s)",
    "Download Speed (Mbps)",
]


def save_results_to_csv(results: List[list], filename: str, header: List[str]) -> None:
    """Write rows to a CSV file with the given header."""
    with open(filename, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(results)


def plot_size_speed(results: List[list], output_file: str, title: str,
                    ylabel: str) -> None:
    """Line plot: speed vs. file size (column 0 = size, column 2 = speed)."""
    sizes = [row[0] for row in results]
    speeds = [row[2] for row in results]
    plt.figure(figsize=(10, 6))
    plt.plot(sizes, speeds, marker="o")
    plt.title(title)
    plt.xlabel("File Size (MB)")
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()


def plot_tuning(results: List[list], output_file: str) -> None:
    """Scatter plot for tuning results (threshold vs. download speed)."""
    thresholds = [row[0] for row in results]
    concurrencies = [row[1] for row in results]
    use_threads = [row[3] for row in results]
    speeds = [row[5] for row in results]

    # use_threads is now stored as a bool; size markers accordingly.
    sizes = [100 if u else 50 for u in use_threads]

    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        thresholds,
        speeds,
        c=concurrencies,
        cmap="viridis",
        marker="o",
        edgecolor="k",
        s=sizes,
        alpha=0.7,
    )
    plt.colorbar(scatter, label="Max Concurrency")
    plt.title("Download Speed by Multipart Threshold")
    plt.xlabel("Multipart Threshold (bytes)")
    plt.ylabel("Download Speed (Mbps)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()


# Backwards-compatible alias matching the original module-level usage.
def plot_results(results: List[list], output_file: str, title: str = "Speed by File Size",
                 ylabel: str = "Speed (Mbps)") -> None:
    plot_size_speed(results, output_file, title, ylabel)
