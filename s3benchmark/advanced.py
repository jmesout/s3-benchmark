"""Phase 6 advanced benchmarks: in-memory transfer, multi-client, cost estimator.

These are stretch capabilities:

  * :class:`InMemoryBenchmark` — generate data in memory (no disk) to isolate
    network/object-store performance from local disk I/O. Uses ``BytesIO``.
  * :class:`MultiClientBenchmark` — simulate N distinct clients hitting the
    bucket simultaneously (separate boto3 clients / processes).
  * :func:`estimate_cost` — rough cost estimate for a planned grid (storage,
    request, and egress) before running.
"""
from __future__ import annotations

import concurrent.futures
import io
import time
from dataclasses import dataclass
from typing import List

from boto3.s3.transfer import TransferConfig

from .config import Config
from .stats import summarize
from .transfer import create_s3_client

MB = 1024 * 1024


class InMemoryBenchmark:
    """Upload/download from memory (BytesIO) to isolate network performance."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.s3 = create_s3_client(cfg)

    def run(self, size_mb: float, repeats: int = 3) -> dict:
        size_bytes = int(size_mb * MB)
        if size_bytes > 256 * MB:
            raise ValueError(
                "In-memory benchmark caps objects at 256MB to avoid exhausting "
                "RAM. Use the file-based upload/download benchmark for larger sizes."
            )
        data = b"x" * size_bytes
        key = f"inmemory/{size_mb}mb"

        up_times = []
        for _ in range(repeats):
            buf = io.BytesIO(data)
            t0 = time.perf_counter()
            self.s3.upload_fileobj(buf, Bucket=self.cfg.bucket, Key=key)
            up_times.append(time.perf_counter() - t0)

        dl_times = []
        for _ in range(repeats):
            out = io.BytesIO()
            t0 = time.perf_counter()
            self.s3.download_fileobj(Bucket=self.cfg.bucket, Key=key, Fileobj=out)
            dl_times.append(time.perf_counter() - t0)

        # cleanup
        if not getattr(self.cfg, "keep_objects", False):
            self.s3.delete_object(Bucket=self.cfg.bucket, Key=key)

        return {
            "size_mb": size_mb,
            "repeats": repeats,
            "upload": summarize(up_times),
            "download": summarize(dl_times),
            "upload_mbps": summarize(
                [size_bytes * 8 / t / 1e6 for t in up_times if t > 0]
            ),
            "download_mbps": summarize(
                [size_bytes * 8 / t / 1e6 for t in dl_times if t > 0]
            ),
        }


class MultiClientBenchmark:
    """Simulate N distinct clients (separate boto3 clients) hitting the bucket."""

    def __init__(self, cfg: Config, clients: int = 4):
        self.cfg = cfg
        self.clients_count = clients
        self._clients = [create_s3_client(cfg) for _ in range(clients)]

    def run(self, size_mb: float, ops_per_client: int = 3) -> dict:
        size_bytes = int(size_mb * MB)
        if size_bytes > 64 * MB:
            raise ValueError(
                "multi-client caps objects at 64MB per client to bound memory "
                "(clients x size). Use the concurrency benchmark for larger objects."
            )
        key_prefix = f"multiclient/{size_mb}mb"

        # Pre-provision one object per client for reads.
        config = TransferConfig(
            multipart_threshold=self.cfg.transfer.multipart_threshold,
            max_concurrency=self.cfg.transfer.max_concurrency,
            multipart_chunksize=self.cfg.transfer.multipart_chunksize,
            use_threads=self.cfg.transfer.use_threads,
        )

        def client_task(client_id: int) -> float:
            client = self._clients[client_id]
            key = f"{key_prefix}_{client_id}"
            data = b"y" * size_bytes
            buf = io.BytesIO(data)
            client.upload_fileobj(buf, Bucket=self.cfg.bucket, Key=key, Config=config)
            t0 = time.perf_counter()
            for _ in range(ops_per_client):
                out = io.BytesIO()
                client.download_fileobj(Bucket=self.cfg.bucket, Key=key, Fileobj=out)
            elapsed = time.perf_counter() - t0
            return elapsed

        wall0 = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self._clients)) as pool:
            per_client = list(pool.map(client_task, range(len(self._clients))))
        wall = time.perf_counter() - wall0

        # cleanup
        if not getattr(self.cfg, "keep_objects", False):
            for i in range(len(self._clients)):
                try:
                    self._clients[i].delete_object(
                        Bucket=self.cfg.bucket, Key=f"{key_prefix}_{i}"
                    )
                except Exception:  # noqa: BLE001
                    pass

        return {
            "clients": len(self._clients),
            "size_mb": size_mb,
            "ops_per_client": ops_per_client,
            "wall_seconds": wall,
            "per_client_seconds": per_client,
            "aggregate_ops_per_sec": (len(self._clients) * ops_per_client) / wall
            if wall > 0 else 0.0,
        }


@dataclass
class CostEstimate:
    """Rough cost estimate for a planned benchmark grid."""

    storage_gb: float = 0.0
    requests: int = 0
    egress_gb: float = 0.0
    # Price inputs (all per-unit USD); user-overridable.
    storage_price_per_gb_month: float = 0.02
    put_price_per_1k: float = 0.005
    get_price_per_1k: float = 0.0004
    egress_price_per_gb: float = 0.09

    def total_usd(self) -> float:
        return (
            self.storage_gb * self.storage_price_per_gb_month
            + (self.requests / 1000.0) * self.put_price_per_1k
            + (self.requests / 1000.0) * self.get_price_per_1k
            + self.egress_gb * self.egress_price_per_gb
        )

    def breakdown(self) -> dict:
        return {
            "storage_gb": self.storage_gb,
            "requests": self.requests,
            "egress_gb": self.egress_gb,
            "total_usd": round(self.total_usd(), 4),
        }


def estimate_cost(sizes_mb: List[float], repeats: int, warmup: int,
                  direction: str = "upload") -> CostEstimate:
    """Estimate cost for a single-direction benchmark over the given sizes."""
    total_mb = sum(sizes_mb)
    total_transfers = len(sizes_mb) * (repeats + warmup)
    est = CostEstimate()
    if direction == "upload":
        # Storage of the largest object (approx, since we upload/delete each).
        est.egress_gb = 0.0  # uploads don't incur egress
        est.requests = total_transfers  # PUTs
    else:
        est.egress_gb = total_mb * (repeats + warmup) / 1024.0
        est.requests = total_transfers  # GETs
    est.storage_gb = max(sizes_mb) / 1024.0
    return est
