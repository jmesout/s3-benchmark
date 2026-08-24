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

from .config import Config, ConfigError, TransferParams, load_config, load_tuning_ranges, parse_bool, parse_int_list
from .configfile import ConfigFile, find_config_file
from .benchmark import BenchmarkEngine
from .io import calculate_speed, create_dummy_file
from .logging_setup import setup_logging
from .report import (
    DOWNLOAD_HEADER,
    TUNING_HEADER,
    UPLOAD_HEADER,
    plot_size_speed,
    plot_tuning,
    save_results_to_csv,
)
from .transfer import create_s3_client
from .workloads import ConcurrencyBenchmark, MixedWorkloadBenchmark, SmallObjectBenchmark


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


def run_concurrency(cfg: Config, args) -> None:
    bench = ConcurrencyBenchmark(cfg)
    print(f"Connected to S3 ({cfg.bucket})")
    print(f"concurrency benchmark: {args.direction} x{args.concurrency} "
          f"{args.size}MB x{args.ops} ops")
    result = bench.run(args.direction, float(args.size), args.concurrency, args.ops)
    bench.cleanup()
    print(f"\nAggregate: {result.aggregate_mbps():.1f} Mbps "
          f"({result.aggregate_mibs():.1f} MiB/s), "
          f"{result.ops_per_second:.1f} ops/s, errors={result.errors}")


def run_smallobj(cfg: Config, args) -> None:
    bench = SmallObjectBenchmark(cfg)
    print(f"Connected to S3 ({cfg.bucket})")
    print(f"small-object benchmark: {args.num} x {args.size}B (concurrency={args.concurrency})")
    result = bench.run(args.size, args.num, args.concurrency)
    for op in ("put", "get", "delete"):
        s = result[op]
        print(f"  {op.upper():<6} mean {s['mean']*1000:7.2f} ms  "
              f"p90 {s['p90']*1000:7.2f} ms  p99 {s['p99']*1000:7.2f} ms")
    ts = _timestamp()
    with open(f"smallobj_report_{ts}.json", "w") as f:
        json.dump(result, f, indent=2)


def run_mixed(cfg: Config, args) -> None:
    bench = MixedWorkloadBenchmark(cfg, read_ratio=args.read_ratio)
    sizes = [float(s) for s in cfg.file_sizes_mb]
    print(f"Connected to S3 ({cfg.bucket})")
    print(f"mixed workload: {args.ops} ops, read ratio={args.read_ratio}, "
          f"concurrency={args.concurrency}, sizes={sizes}")
    result = bench.run(sizes, args.ops, args.concurrency)
    bench.cleanup()
    for op, key in (("READ", "read_time_summary"), ("WRITE", "write_time_summary")):
        s = result[key]
        if s["n"]:
            print(f"  {op:<5} n={s['n']} mean {s['mean']*1000:7.2f} ms  "
                  f"p90 {s['p90']*1000:7.2f} ms  p99 {s['p99']*1000:7.2f} ms")
    print(f"  errors={result['errors']}")
    ts = _timestamp()
    with open(f"mixed_report_{ts}.json", "w") as f:
        json.dump(result, f, indent=2)


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
    # Global options (available before the subcommand).
    parser.add_argument("--config", help="Path to s3benchmark.toml config file")
    parser.add_argument("--profile", help="Named profile in the config file")
    parser.add_argument("--file-sizes", help="Override FILE_SIZES (comma-separated MB)")
    parser.add_argument("--multipart-threshold", type=int, help="Override multipart threshold (bytes)")
    parser.add_argument("--max-concurrency", type=int, help="Override max concurrency")
    parser.add_argument("--multipart-chunksize", type=int, help="Override chunk size (bytes)")
    parser.add_argument("--repeats", type=int, help="Override repeats")
    parser.add_argument("--warmup", type=int, help="Override warmup transfers")
    parser.add_argument("--keep-objects", action="store_true", help="Keep test objects")
    parser.add_argument("--no-verify", action="store_true", help="Disable integrity checks")
    parser.add_argument("-q", "--quiet", action="store_true", help="Minimal output")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--json-logs", action="store_true", help="JSON Lines logging")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without running")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("upload", help="Measure upload throughput by object size.")
    sub.add_parser("download", help="Measure download throughput by object size.")
    sub.add_parser("tune", help="Grid-search TransferConfig parameters on a download.")

    c = sub.add_parser("concurrency", help="Measure aggregate throughput with parallel transfers.")
    c.add_argument("--direction", choices=["upload", "download"], default="upload")
    c.add_argument("--size", type=int, default=1, help="Object size in MB")
    c.add_argument("-c", "--concurrency", type=int, default=4, help="Parallel workers")
    c.add_argument("--ops", type=int, default=3, help="Ops per worker")

    s = sub.add_parser("smallobj", help="Measure PUT/GET/DELETE latency for small objects.")
    s.add_argument("--size", type=int, default=1024, help="Object size in bytes")
    s.add_argument("-n", "--num", type=int, default=100, help="Number of objects")
    s.add_argument("-c", "--concurrency", type=int, default=1)

    m = sub.add_parser("mixed", help="Run a read/write mix across object sizes.")
    m.add_argument("--read-ratio", type=float, default=0.8, help="Fraction of reads (0-1)")
    m.add_argument("--ops", type=int, default=50, help="Total operations")
    m.add_argument("-c", "--concurrency", type=int, default=1)

    return parser


def _apply_config_file(cfg: Config, args) -> Config:
    """Merge values from a TOML config file (lower precedence than CLI/env)."""
    path = args.config or find_config_file()
    if path is None:
        return cfg
    cf = ConfigFile.load(path)
    sizes = cf.file_sizes_mb(args.profile)
    if sizes:
        cfg.file_sizes_mb = sizes
    t = cf.transfer(args.profile)
    # Only override transfer values that differ from defaults AND exist in the profile.
    cfg.transfer = TransferParams(
        multipart_threshold=t.multipart_threshold,
        max_concurrency=t.max_concurrency,
        multipart_chunksize=t.multipart_chunksize,
        use_threads=t.use_threads,
    )
    p = cf.profile(args.profile)
    for key in ("repeats", "warmup", "verify", "keep_objects"):
        if key in p:
            setattr(cfg, key, parse_bool(str(p[key])) if key in ("verify", "keep_objects") else int(p[key]))
    return cfg


def _apply_overrides(cfg: Config, args) -> Config:
    """Merge CLI overrides (highest precedence) onto the loaded config."""
    if args.file_sizes:
        cfg.file_sizes_mb = parse_int_list(args.file_sizes)
    if args.multipart_threshold is not None:
        cfg.transfer.multipart_threshold = args.multipart_threshold
    if args.max_concurrency is not None:
        cfg.transfer.max_concurrency = args.max_concurrency
    if args.multipart_chunksize is not None:
        cfg.transfer.multipart_chunksize = args.multipart_chunksize
    if args.repeats is not None:
        cfg.repeats = args.repeats
    if args.warmup is not None:
        cfg.warmup = args.warmup
    if args.keep_objects:
        cfg.keep_objects = True
    if args.no_verify:
        cfg.verify = False
    cfg.transfer.validate()
    return cfg


def _plan(cfg: Config, args) -> str:
    """Return a human-readable description of what would run."""
    lines = [f"Benchmark: {args.command}", f"Bucket: {cfg.bucket}",
             f"Endpoint: {cfg.endpoint_url}"]
    if args.command in ("upload", "download"):
        sizes = cfg.file_sizes_mb
        total_mb = sum(sizes)
        lines.append(f"File sizes (MB): {sizes}")
        lines.append(f"Repeats: {cfg.repeats}, Warmup: {cfg.warmup}, Verify: {cfg.verify}")
        lines.append(f"Approx data volume: {total_mb} MB x {(cfg.repeats + cfg.warmup)} = "
                     f"{total_mb * (cfg.repeats + cfg.warmup)} MB transferred")
    elif args.command == "concurrency":
        lines.append(f"Direction: {args.direction}, concurrency={args.concurrency}, "
                     f"size={args.size}MB, ops/worker={args.ops}")
        vol = args.concurrency * args.ops * args.size
        lines.append(f"Approx data volume: {vol} MB")
    elif args.command == "smallobj":
        lines.append(f"Objects: {args.num} x {args.size}B, concurrency={args.concurrency}")
    elif args.command == "mixed":
        lines.append(f"Ops: {args.ops}, read ratio={args.read_ratio}, concurrency={args.concurrency}")
    lines.append(f"Keep objects: {cfg.keep_objects}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    setup_logging(
        level="DEBUG" if args.verbose else ("ERROR" if args.quiet else "INFO"),
        json_output=args.json_logs,
    )

    try:
        cfg = load_config()
        cfg = _apply_config_file(cfg, args)
        cfg = _apply_overrides(cfg, args)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(_plan(cfg, args))
        return 0

    try:
        if args.command == "upload":
            run_upload(cfg)
        elif args.command == "download":
            run_download(cfg)
        elif args.command == "tune":
            run_tune(cfg)
        elif args.command == "concurrency":
            run_concurrency(cfg, args)
        elif args.command == "smallobj":
            run_smallobj(cfg, args)
        elif args.command == "mixed":
            run_mixed(cfg, args)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())