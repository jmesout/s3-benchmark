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
from .benchmark import BenchmarkEngine
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
    engine = BenchmarkEngine(cfg)
    print(f"Connected to S3 ({cfg.bucket})")
    print(f"repeats={engine.repeats} warmup={engine.warmup} verify={engine.verify}")

    report = engine.run_benchmark("upload", [float(s) for s in cfg.file_sizes_mb])
    engine.cleanup()

    ts = _timestamp()
    report.save(f"upload_report_{ts}.json")
    _print_report(report)


def run_download(cfg: Config) -> None:
    engine = BenchmarkEngine(cfg)
    print(f"Connected to S3 ({cfg.bucket})")
    print(f"repeats={engine.repeats} warmup={engine.warmup} verify={engine.verify}")

    report = engine.run_benchmark("download", [float(s) for s in cfg.file_sizes_mb])
    engine.cleanup()

    ts = _timestamp()
    report.save(f"download_report_{ts}.json")
    _print_report(report)


def _print_report(report) -> None:
    print("\nResults:")
    for r in report.results:
        s = r.summary()
        mbps_mean = s["throughput_mbps"]["mean"]
        mibs_mean = s["throughput_mibs"]["mean"]
        ok = "OK" if s["integrity_ok"] is not False else "INTEGRITY_FAIL"
        print(
            f"  {s['size_mb']:>8.0f} MB  {s['direction']:<9} "
            f"mean {mbps_mean:8.1f} Mbps ({mibs_mean:7.1f} MiB/s) "
            f"n={s['succeeded']}/{s['attempts']} failed={s['failed']} "
            f"{ok}"
        )


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