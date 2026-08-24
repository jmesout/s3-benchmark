"""Concurrency, small-object, and mixed-workload benchmarking.

Phase 1 capabilities that go beyond single-object streaming:
  * ``ConcurrencyBenchmark`` — K parallel uploads/downloads, reporting
    aggregate throughput plus per-worker stats.
  * ``SmallObjectBenchmark`` — PUT/GET/DELETE latency for many small objects.
  * ``MixedWorkloadBenchmark`` — configurable read/write ratio across an
    object-size distribution.
"""
from __future__ import annotations

import concurrent.futures
import os
import random
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from boto3.s3.transfer import TransferConfig

from .config import Config
from .io import create_dummy_file
from .results import RunMetadata, RunReport, TransferResult, TransferSample
from .stats import summarize
from .transfer import create_s3_client
from . import __version__

MB = 1024 * 1024


@dataclass
class WorkerTiming:
    """One worker's wall-clock and byte count over its assigned transfers."""

    elapsed_s: float
    bytes: int
    ops: int
    errors: int = 0


@dataclass
class ConcurrencyResult:
    """Aggregate result for a parallel-transfer benchmark."""

    direction: str
    concurrency: int
    size_mb: float
    total_bytes: int
    total_ops: int
    errors: int
    wall_seconds: float
    worker_timings: List[WorkerTiming] = field(default_factory=list)
    transfer_result: Optional[TransferResult] = None

    def aggregate_mbps(self) -> float:
        if self.wall_seconds <= 0:
            return 0.0
        return (self.total_bytes * 8) / self.wall_seconds / 1e6

    def aggregate_mibs(self) -> float:
        return self.aggregate_mbps() / 8.0 * 1e6 / (1024 * 1024)

    @property
    def ops_per_second(self) -> float:
        return self.total_ops / self.wall_seconds if self.wall_seconds > 0 else 0.0

    def per_worker_stats(self) -> dict:
        return {
            "worker_count": len(self.worker_timings),
            "worker_bytes": summarize([w.bytes / self.wall_seconds * 8 / 1e6 for w in self.worker_timings]) if self.wall_seconds > 0 and self.worker_timings else {"n": 0},
            "errors": self.errors,
        }

    def to_dict(self) -> dict:
        return {
            "direction": self.direction,
            "concurrency": self.concurrency,
            "size_mb": self.size_mb,
            "total_bytes": self.total_bytes,
            "total_ops": self.total_ops,
            "errors": self.errors,
            "wall_seconds": self.wall_seconds,
            "aggregate_mbps": self.aggregate_mbps(),
            "aggregate_mibs": self.aggregate_mibs(),
            "ops_per_second": self.ops_per_second,
        }


class ConcurrencyBenchmark:
    """Run K parallel transfers and measure aggregate throughput."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.s3 = create_s3_client(cfg)
        self.tmpdir = tempfile.TemporaryDirectory(prefix="s3benchmark-conc-")

    def _transfer_config(self) -> TransferConfig:
        return TransferConfig(
            multipart_threshold=self.cfg.transfer.multipart_threshold,
            max_concurrency=self.cfg.transfer.max_concurrency,
            multipart_chunksize=self.cfg.transfer.multipart_chunksize,
            use_threads=self.cfg.transfer.use_threads,
        )

    def run(
        self,
        direction: str,
        size_mb: float,
        concurrency: int,
        per_worker_ops: int = 3,
    ) -> ConcurrencyResult:
        config = self._transfer_config()
        size_bytes = int(size_mb * MB)
        total_ops = concurrency * per_worker_ops
        total_bytes = total_ops * size_bytes

        # For reads, provision a single shared object per worker (read-only).
        base_key = f"concurrency/{direction}/{size_mb}mb"
        if direction == "download":
            local = os.path.join(self.tmpdir.name, f"shared_{size_mb}mb.bin")
            create_dummy_file(local, int(size_mb))
            for w in range(concurrency):
                self.s3.upload_file(
                    Filename=local, Bucket=self.cfg.bucket,
                    Key=f"{base_key}_{w}", Config=config,
                )
            os.remove(local)

        worker_timings: List[WorkerTiming] = []
        errors = 0

        def worker_task(worker_id: int) -> WorkerTiming:
            t0 = time.perf_counter()
            transferred = 0
            werrors = 0
            for op in range(per_worker_ops):
                key = f"{base_key}_{worker_id}_{op}"
                try:
                    if direction == "upload":
                        local = os.path.join(
                            self.tmpdir.name, f"f_{size_mb}mb_{worker_id}_{op}.bin"
                        )
                        create_dummy_file(local, int(size_mb))
                        self.s3.upload_file(
                            Filename=local, Bucket=self.cfg.bucket,
                            Key=key, Config=config,
                        )
                        os.remove(local)
                    else:
                        dest = os.path.join(self.tmpdir.name, f"dl_{worker_id}_{op}.bin")
                        self.s3.download_file(
                            Bucket=self.cfg.bucket, Key=f"{base_key}_{worker_id}",
                            Filename=dest, Config=config,
                        )
                        os.remove(dest)
                    transferred += size_bytes
                except Exception:  # noqa: BLE001
                    werrors += 1
            return WorkerTiming(
                elapsed_s=time.perf_counter() - t0,
                bytes=transferred, ops=per_worker_ops, errors=werrors,
            )

        wall0 = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker_task, w) for w in range(concurrency)]
            for fut in concurrent.futures.as_completed(futures):
                r = fut.result()
                worker_timings.append(r)
                errors += r.errors
        wall = time.perf_counter() - wall0

        # Cleanup uploaded objects (download keys are read-only shared).
        if not getattr(self.cfg, "keep_objects", False):
            for w in range(concurrency):
                for op in range(per_worker_ops):
                    try:
                        self.s3.delete_object(Bucket=self.cfg.bucket, Key=f"{base_key}_{w}_{op}")
                    except Exception:  # noqa: BLE001
                        pass
                try:
                    self.s3.delete_object(Bucket=self.cfg.bucket, Key=f"{base_key}_{w}")
                except Exception:  # noqa: BLE001
                    pass

        return ConcurrencyResult(
            direction=direction, concurrency=concurrency, size_mb=size_mb,
            total_bytes=total_bytes, total_ops=total_ops, errors=errors,
            wall_seconds=wall, worker_timings=worker_timings,
        )

    def cleanup(self) -> None:
        try:
            self.tmpdir.cleanup()
        except Exception:  # noqa: BLE001
            pass


class SmallObjectBenchmark:
    """Measure PUT/GET/DELETE latency for many small objects."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.s3 = create_s3_client(cfg)

    def run(self, object_size_bytes: int, num_objects: int, concurrency: int = 1) -> dict:
        payload = os.urandom(object_size_bytes)
        keys = [f"smallobj/{i:08d}" for i in range(num_objects)]

        def put(key: str):
            self.s3.put_object(Bucket=self.cfg.bucket, Key=key, Body=payload)

        def get(key: str):
            self.s3.get_object(Bucket=self.cfg.bucket, Key=key)

        def delete(key: str):
            self.s3.delete_object(Bucket=self.cfg.bucket, Key=key)

        def _time_op(fn, arg) -> float:
            t0 = time.perf_counter()
            fn(arg)
            return time.perf_counter() - t0

        # PUT
        put_times = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            put_times = list(pool.map(lambda k: _time_op(put, k), keys))
        # GET
        get_times = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            get_times = list(pool.map(lambda k: _time_op(get, k), keys))
        # DELETE
        del_times = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            del_times = list(pool.map(lambda k: _time_op(delete, k), keys))

        return {
            "object_size_bytes": object_size_bytes,
            "num_objects": num_objects,
            "concurrency": concurrency,
            "put": summarize(put_times),
            "get": summarize(get_times),
            "delete": summarize(del_times),
        }


class MixedWorkloadBenchmark:
    """Run a read/write mix with a specified object-size distribution."""

    def __init__(self, cfg: Config, read_ratio: float = 0.8):
        self.cfg = cfg
        self.s3 = create_s3_client(cfg)
        self.read_ratio = read_ratio
        self.tmpdir = tempfile.TemporaryDirectory(prefix="s3benchmark-mix-")

    def run(self, sizes_mb: List[float], total_ops: int, concurrency: int = 1) -> dict:
        config = TransferConfig(
            multipart_threshold=self.cfg.transfer.multipart_threshold,
            max_concurrency=self.cfg.transfer.max_concurrency,
            multipart_chunksize=self.cfg.transfer.multipart_chunksize,
            use_threads=self.cfg.transfer.use_threads,
        )
        # Pre-provision objects for reads.
        for i, size_mb in enumerate(sizes_mb):
            local = os.path.join(self.tmpdir.name, f"seed_{size_mb}.bin")
            create_dummy_file(local, int(size_mb))
            self.s3.upload_file(
                Filename=local, Bucket=self.cfg.bucket,
                Key=f"mixed/{size_mb}mb", Config=config,
            )
            os.remove(local)

        rng = random.Random(42)

        results = {"reads": [], "writes": [], "errors": 0}

        def op():
            size_mb = rng.choice(sizes_mb)
            is_read = rng.random() < self.read_ratio
            t0 = time.perf_counter()
            try:
                if is_read:
                    dest = os.path.join(self.tmpdir.name, f"dl_{os.getpid()}_{rng.randrange(999999)}.bin")
                    self.s3.download_file(
                        Bucket=self.cfg.bucket, Key=f"mixed/{size_mb}mb",
                        Filename=dest, Config=config,
                    )
                    os.remove(dest)
                else:
                    local = os.path.join(self.tmpdir.name, f"up_{os.getpid()}_{rng.randrange(999999)}.bin")
                    create_dummy_file(local, int(size_mb))
                    self.s3.upload_file(
                        Filename=local, Bucket=self.cfg.bucket,
                        Key=f"mixed/write_{rng.randrange(999999)}", Config=config,
                    )
                    os.remove(local)
                elapsed = time.perf_counter() - t0
                results["reads" if is_read else "writes"].append(elapsed)
            except Exception:  # noqa: BLE001
                results["errors"] += 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(lambda _: op(), range(total_ops)))

        # cleanup
        if not getattr(self.cfg, "keep_objects", False):
            # delete seed objects and written objects
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.cfg.bucket, Prefix="mixed/"):
                for obj in page.get("Contents", []):
                    self.s3.delete_object(Bucket=self.cfg.bucket, Key=obj["Key"])

        return {
            "read_ratio": self.read_ratio,
            "total_ops": total_ops,
            "concurrency": concurrency,
            "read_time_summary": summarize(results["reads"]),
            "write_time_summary": summarize(results["writes"]),
            "errors": results["errors"],
        }

    def cleanup(self) -> None:
        try:
            self.tmpdir.cleanup()
        except Exception:  # noqa: BLE001
            pass