"""Benchmark engine: HTTP-transfer timing with repeats, warmup, and integrity."""
from __future__ import annotations

import hashlib
import os
import tempfile
import time
from datetime import datetime, timezone
from typing import Callable, List

from boto3.s3.transfer import TransferConfig

from . import __version__
from .config import Config
from .io import create_dummy_file
from .results import RunMetadata, RunReport, TransferResult, TransferSample
from .transfer import create_s3_client

MB = 1024 * 1024


def _md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * MB), b""):
            h.update(chunk)
    return h.hexdigest()


def _object_etag(s3, bucket: str, key: str) -> str:
    resp = s3.head_object(Bucket=bucket, Key=key)
    etag = resp.get("ETag", "").strip('"').lower()
    # Multipart ETags are "<md5>-<n>" — strip the part count suffix.
    if "-" in etag and etag.split("-")[-1].isdigit():
        etag = etag.split("-")[0]
    return etag


def _parts_count(size_bytes: int, threshold: int, chunksize: int) -> int:
    if size_bytes <= threshold:
        return 1
    return -(-size_bytes // chunksize)  # ceiling division


class BenchmarkEngine:
    """Runs upload/download throughput measurements with statistics."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.s3 = create_s3_client(cfg)
        self.repeats = getattr(cfg, "repeats", 3)
        self.warmup = getattr(cfg, "warmup", 1)
        self.verify = getattr(cfg, "verify", True)
        self.keep_objects = getattr(cfg, "keep_objects", False)
        self.tmpdir = tempfile.TemporaryDirectory(prefix="s3benchmark-")
        self._provisioned_keys: set = set()

    # -- object helpers -----------------------------------------------------
    def _object_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.cfg.bucket, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False

    # -- low-level transfer wrapper -----------------------------------------
    def _measured_transfer(
        self,
        fn: Callable[[], None],
        size_bytes: int,
    ) -> TransferSample:
        """Run fn() once, timing it with a monotonic clock.

        TTFB (time-to-first-byte) is not observable through boto3's high-level
        upload_file/download_file APIs; it is tracked in the result model for
        future stream-based measurement but defaults to 0.0 here.
        """
        t0 = time.perf_counter()
        try:
            fn()
            elapsed = time.perf_counter() - t0
            return TransferSample(elapsed_s=elapsed, bytes=size_bytes)
        except Exception as exc:  # noqa: BLE001
            elapsed = time.perf_counter() - t0
            return TransferSample(
                elapsed_s=elapsed, bytes=size_bytes, error=f"{type(exc).__name__}: {exc}"
            )

    # -- file path helpers ---------------------------------------------------
    def _dummy_path(self, size_mb: float) -> str:
        return os.path.join(self.tmpdir.name, f"dummy_{size_mb}mb.bin")

    # -- upload --------------------------------------------------------------
    def run_upload(self, size_mb: float, config: TransferConfig) -> TransferResult:
        path = self._dummy_path(size_mb)
        object_key = f"benchmark/{size_mb}mb.txt"
        size_bytes = int(size_mb * MB)

        create_dummy_file(path, int(size_mb))
        local_md5 = _md5_of_file(path)

        result = TransferResult(
            size_mb=size_mb, direction="upload", bytes=size_bytes,
            multipart_threshold=config.multipart_threshold,
            max_concurrency=config.max_concurrency,
            multipart_chunksize=config.multipart_chunksize,
            use_threads=config.use_threads,
            parts=_parts_count(size_bytes, config.multipart_threshold, config.multipart_chunksize),
        )

        total_attempts = self.warmup + self.repeats

        for i in range(total_attempts):
            is_warmup = i < self.warmup
            sample = self._measured_transfer(
                lambda: self.s3.upload_file(
                    Filename=path, Bucket=self.cfg.bucket,
                    Key=object_key, Config=config,
                ),
                size_bytes,
            )
            result.total_samples += 1
            if sample.error is not None:
                result.failed += 1
            elif not is_warmup:
                result.samples.append(sample)

        # Integrity check against the stored object (compare local MD5 to ETag).
        if self.verify and result.succeeded > 0:
            try:
                etag = _object_etag(self.s3, self.cfg.bucket, object_key)
                result.integrity_ok = (etag == local_md5)
            except Exception as exc:  # noqa: BLE001
                result.integrity_ok = False
                result.integrity_error = str(exc)

        # Cleanup
        if not self.keep_objects:
            self.s3.delete_object(Bucket=self.cfg.bucket, Key=object_key)
        os.remove(path)
        return result

    # -- download ------------------------------------------------------------
    def run_download(self, size_mb: float, config: TransferConfig) -> TransferResult:
        object_key = f"benchmark/{size_mb}mb.txt"
        download_path = self._dummy_path(size_mb)
        size_bytes = int(size_mb * MB)

        # Provision the object in the bucket if it does not already exist, so
        # the download benchmark is self-contained.
        if not self._object_exists(object_key):
            local = self._dummy_path(size_mb)
            create_dummy_file(local, int(size_mb))
            self.s3.upload_file(
                Filename=local, Bucket=self.cfg.bucket, Key=object_key, Config=config
            )
            os.remove(local)
            self._provisioned_keys.add(object_key)

        result = TransferResult(
            size_mb=size_mb, direction="download", bytes=size_bytes,
            multipart_threshold=config.multipart_threshold,
            max_concurrency=config.max_concurrency,
            multipart_chunksize=config.multipart_chunksize,
            use_threads=config.use_threads,
            parts=_parts_count(size_bytes, config.multipart_threshold, config.multipart_chunksize),
        )

        total_attempts = self.warmup + self.repeats

        for i in range(total_attempts):
            is_warmup = i < self.warmup
            sample = self._measured_transfer(
                lambda: self.s3.download_file(
                    Bucket=self.cfg.bucket, Key=object_key,
                    Filename=download_path, Config=config,
                ),
                size_bytes,
            )
            result.total_samples += 1
            if sample.error is not None:
                result.failed += 1
            elif not is_warmup:
                result.samples.append(sample)

        if os.path.exists(download_path):
            os.remove(download_path)
        return result

    # -- orchestration -------------------------------------------------------
    def _transfer_config(self) -> TransferConfig:
        return TransferConfig(
            multipart_threshold=self.cfg.transfer.multipart_threshold,
            max_concurrency=self.cfg.transfer.max_concurrency,
            multipart_chunksize=self.cfg.transfer.multipart_chunksize,
            use_threads=self.cfg.transfer.use_threads,
        )

    def run_benchmark(self, direction: str, sizes_mb: List[float]) -> RunReport:
        config = self._transfer_config()
        results: List[TransferResult] = []

        started_at = datetime.now(timezone.utc).isoformat()

        for size_mb in sizes_mb:
            if direction == "upload":
                results.append(self.run_upload(size_mb, config))
            else:
                results.append(self.run_download(size_mb, config))

        finished_at = datetime.now(timezone.utc).isoformat()

        meta = RunMetadata(
            tool_version=__version__,
            benchmark=direction,
            bucket=self.cfg.bucket,
            endpoint_url=self.cfg.endpoint_url,
            started_at=started_at,
            finished_at=finished_at,
            params={
                "repeats": self.repeats,
                "warmup": self.warmup,
                "verify": self.verify,
                "keep_objects": self.keep_objects,
                "multipart_threshold": config.multipart_threshold,
                "max_concurrency": config.max_concurrency,
                "multipart_chunksize": config.multipart_chunksize,
                "use_threads": config.use_threads,
            },
        )
        return RunReport(metadata=meta, results=results)

    def cleanup(self) -> None:
        # Remove objects we provisioned for download (unless keep_objects).
        if not self.keep_objects:
            for key in self._provisioned_keys:
                try:
                    self.s3.delete_object(Bucket=self.cfg.bucket, Key=key)
                except Exception:  # noqa: BLE001
                    pass
        try:
            self.tmpdir.cleanup()
        except Exception:  # noqa: BLE001
            pass
