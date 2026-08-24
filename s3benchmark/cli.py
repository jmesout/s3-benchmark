"""Command-line interface and orchestration for the upload/download benchmarks.

Provides a single ``s3-benchmark`` entry point with ``upload``, ``download``,
and ``tune`` subcommands, replacing the three standalone scripts while keeping
their behaviour.
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
import time
from datetime import datetime

from boto3.s3.transfer import TransferConfig

from .config import Config, ConfigError, TransferParams, load_config, load_tuning_ranges
from .io import calculate_speed, create_dummy_file
from .report import (
    DOWNLOAD_HEADER,
    TUNING_HEADER,
    UPLOAD_HEADER,
    plot_size_speed,
    plot_tuning,
    save_results_to_csv,
)
from .transfer import create_s3_client


def _transfer_config(params: TransferParams) -> TransferConfig:
    return TransferConfig(
        multipart_threshold=params.multipart_threshold,
        max_concurrency=params.max_concurrency,
        multipart_chunksize=params.multipart_chunksize,
        use_threads=params.use_threads,
    )


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_upload(cfg: Config) -> None:
    s3 = create_s3_client(cfg)
    print("Connected to S3")

    config = _transfer_config(cfg.transfer)

    results = []
    for size_mb in cfg.file_sizes_mb:
        file_name = f"dummy_{size_mb}mb.txt"
        object_key = f"example_{size_mb}mb.txt"

        print(f"Creating dummy file of size {size_mb}MB...")
        create_dummy_file(file_name, size_mb)
        print(f"Dummy file of size {size_mb}MB created.")

        print(f"Uploading {size_mb}MB file to S3...")
        start = time.perf_counter()
        s3.upload_file(
            Filename=file_name, Bucket=cfg.bucket, Key=object_key, Config=config
        )
        elapsed = time.perf_counter() - start

        file_size = os.path.getsize(file_name)
        speed = calculate_speed(elapsed, file_size)
        print(f"Uploaded {file_size} bytes in {elapsed:.2f} seconds.")
        print(f"Upload speed: {speed:.2f} Mbps")

        results.append([
            size_mb, elapsed, speed,
            cfg.transfer.multipart_threshold,
            cfg.transfer.max_concurrency,
            cfg.transfer.multipart_chunksize,
            cfg.transfer.use_threads,
        ])

        os.remove(file_name)
        print(f"Local file of size {size_mb}MB deleted.\n")

    ts = _timestamp()
    save_results_to_csv(results, f"upload_results_{ts}.csv", UPLOAD_HEADER)
    plot_size_speed(results, f"upload_speeds_{ts}.png",
                    "Upload Speed by File Size", "Upload Speed (Mbps)")


def run_download(cfg: Config) -> None:
    s3 = create_s3_client(cfg)
    print("Connected to S3")

    config = _transfer_config(cfg.transfer)

    results = []
    for size_mb in cfg.file_sizes_mb:
        object_key = f"example_{size_mb}mb.txt"
        download_path = f"downloaded_{size_mb}mb.txt"

        print(f"Downloading {size_mb}MB file from S3...")
        start = time.perf_counter()
        s3.download_file(
            Bucket=cfg.bucket, Key=object_key, Filename=download_path, Config=config
        )
        elapsed = time.perf_counter() - start

        file_size = os.path.getsize(download_path)
        speed = calculate_speed(elapsed, file_size)
        print(f"Downloaded {file_size} bytes in {elapsed:.2f} seconds.")
        print(f"Download speed: {speed:.2f} Mbps")

        results.append([
            size_mb, elapsed, speed,
            cfg.transfer.multipart_threshold,
            cfg.transfer.max_concurrency,
            cfg.transfer.multipart_chunksize,
            cfg.transfer.use_threads,
        ])

        os.remove(download_path)
        print(f"Downloaded file of size {size_mb}MB deleted.\n")

    ts = _timestamp()
    save_results_to_csv(results, f"download_results_{ts}.csv", DOWNLOAD_HEADER)
    plot_size_speed(results, f"download_speeds_{ts}.png",
                    "Download Speed by File Size", "Download Speed (Mbps)")


def run_tune(cfg: Config) -> None:
    ranges = load_tuning_ranges()
    s3 = create_s3_client(cfg)
    print("Connected to S3")

    tune_file_size = int(os.getenv("TUNE_FILE_SIZE", "1024"))
    object_key = f"example_{tune_file_size}mb.txt"
    download_path = f"downloaded_{tune_file_size}mb.txt"

    combinations = itertools.product(
        ranges["TUNE_MULTIPART_THRESHOLD"],
        ranges["TUNE_MAX_CONCURRENCY"],
        ranges["TUNE_MULTIPART_CHUNKSIZE"],
        ranges["TUNE_USE_THREADS"],
    )

    results = []
    for threshold, concurrency, chunksize, use_threads in combinations:
        params = TransferParams(
            multipart_threshold=threshold,
            max_concurrency=concurrency,
            multipart_chunksize=chunksize,
            use_threads=use_threads,
        )
        # Skip invalid combinations (chunksize below 5 MiB, etc.).
        try:
            params.validate()
        except ConfigError as exc:
            print(f"Skipping invalid config: {exc}")
            continue

        config = _transfer_config(params)

        print(
            f"Testing configuration: Threshold={threshold}, "
            f"Concurrency={concurrency}, Chunksize={chunksize}, "
            f"UseThreads={use_threads}"
        )
        start = time.perf_counter()
        s3.download_file(
            Bucket=cfg.bucket, Key=object_key,
            Filename=download_path, Config=config,
        )
        elapsed = time.perf_counter() - start

        file_size = os.path.getsize(download_path)
        speed = calculate_speed(elapsed, file_size)
        print(
            f"Downloaded {file_size} bytes in {elapsed:.2f} seconds. "
            f"Speed: {speed:.2f} Mbps"
        )

        results.append([
            threshold, concurrency, chunksize, use_threads, elapsed, speed,
        ])
        os.remove(download_path)
        print(f"Downloaded file of size {tune_file_size}MB deleted.\n")

    if not results:
        print("No valid configurations produced results.", file=sys.stderr)
        sys.exit(1)

    ts = _timestamp()
    save_results_to_csv(results, f"tuning_results_{ts}.csv", TUNING_HEADER)
    plot_tuning(results, f"tuning_plot_{ts}.png")

    best = max(results, key=lambda row: row[5])
    best_params = {
        "Multipart Threshold (bytes)": best[0],
        "Max Concurrency": best[1],
        "Multipart Chunksize (bytes)": best[2],
        "Use Threads": best[3],
        "Time Taken (s)": best[4],
        "Download Speed (Mbps)": best[5],
    }
    print("\nFastest Configuration:")
    for key, value in best_params.items():
        print(f"{key}: {value}")

    with open(f"fastest_configuration_{ts}.json", "w") as f:
        json.dump(best_params, f, indent=4)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="s3-benchmark",
        description="Benchmark S3 upload/download throughput across object sizes.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("upload", help="Measure upload throughput by object size.")
    sub.add_parser("download", help="Measure download throughput by object size.")
    sub.add_parser("tune", help="Grid-search TransferConfig parameters on a download.")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        cfg = load_config()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    try:
        if args.command == "upload":
            run_upload(cfg)
        elif args.command == "download":
            run_download(cfg)
        elif args.command == "tune":
            run_tune(cfg)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())