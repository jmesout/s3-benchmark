"""Configuration loading and validation."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional

from dotenv import load_dotenv

# AWS limits for multipart upload parts.
MIN_CHUNKSIZE = 5 * 1024 * 1024       # 5 MiB
MAX_CHUNKSIZE = 5 * 1024 * 1024 * 1024  # 5 GiB

DEFAULT_FILE_SIZES = "100,500,1024,5120,10240,20480,51200,102400"


class ConfigError(ValueError):
    """Raised when configuration is invalid."""


def parse_bool(value: str) -> bool:
    """Parse a boolean-ish string from the environment or CLI."""
    return str(value).strip().lower() in ("true", "1", "t", "y", "yes", "on")


def parse_int_list(value: str) -> List[int]:
    """Parse a comma-separated list of integers."""
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def _getenv_or_raise(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise ConfigError(
            f"Missing required environment variable '{name}'. "
            "Copy .env-example to .env and fill it in."
        )
    return value


@dataclass
class TransferParams:
    """Validated ``TransferConfig`` parameters."""

    multipart_threshold: int = 50 * 1024 * 1024
    max_concurrency: int = 10
    multipart_chunksize: int = 50 * 1024 * 1024
    use_threads: bool = True

    def validate(self) -> None:
        if self.multipart_threshold < 0:
            raise ConfigError("multipart_threshold must be >= 0")
        if self.max_concurrency <= 0:
            raise ConfigError("max_concurrency must be > 0")
        if self.multipart_chunksize < MIN_CHUNKSIZE:
            raise ConfigError(
                f"multipart_chunksize must be >= {MIN_CHUNKSIZE} bytes (5 MiB)"
            )
        if self.multipart_chunksize > MAX_CHUNKSIZE:
            raise ConfigError(
                f"multipart_chunksize must be <= {MAX_CHUNKSIZE} bytes (5 GiB)"
            )
        if self.multipart_threshold < self.multipart_chunksize:
            raise ConfigError(
                "multipart_threshold should be >= multipart_chunksize; "
                "otherwise every multipart transfer uses a single part."
            )


@dataclass
class Config:
    """All configuration needed to run a benchmark."""

    access_key: Optional[str]
    secret_key: Optional[str]
    endpoint_url: Optional[str]
    bucket: str
    signature_version: str = "s3v4"
    request_checksum_calculation: str = "when_required"
    addressing_style: str = "path"
    file_sizes_mb: List[int] = field(default_factory=list)
    transfer: TransferParams = field(default_factory=TransferParams)
    repeats: int = 3
    warmup: int = 1
    verify: bool = True
    keep_objects: bool = False


def load_config(env: dict | None = None) -> Config:
    """Load and validate configuration from the environment (via ``.env``)."""
    load_dotenv()

    # Provider preset (optional): fills endpoint + signature defaults.
    provider_name = os.getenv("S3_PROVIDER", "").strip().lower()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    signature_version = os.getenv("SIGNATURE_VERSION")

    addressing_style = os.getenv("ADDRESSING_STYLE", "path")
    checksum = os.getenv("REQUEST_CHECKSUM_CALCULATION", "when_required")

    if provider_name:
        from .presets import get_preset

        preset = get_preset(provider_name)
        # Preset provides defaults; explicit env values still win.
        if endpoint_url is None:
            endpoint_url = preset.endpoint_url
        if signature_version is None:
            signature_version = preset.signature_version
        addressing_style = preset.addressing_style
        checksum = preset.request_checksum_calculation

    bucket = _getenv_or_raise("S3_BUCKET_NAME")

    transfer = TransferParams(
        multipart_threshold=int(os.getenv("MULTIPART_THRESHOLD", 50 * 1024 * 1024)),
        max_concurrency=int(os.getenv("MAX_CONCURRENCY", 10)),
        multipart_chunksize=int(os.getenv("MULTIPART_CHUNKSIZE", 50 * 1024 * 1024)),
        use_threads=parse_bool(os.getenv("USE_THREADS", "true")),
    )
    transfer.validate()

    file_sizes = parse_int_list(os.getenv("FILE_SIZES", DEFAULT_FILE_SIZES))
    if not file_sizes:
        raise ConfigError("FILE_SIZES must contain at least one value")
    if any(s <= 0 for s in file_sizes):
        raise ConfigError("FILE_SIZES values must all be positive")

    return Config(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        bucket=bucket,
        signature_version=signature_version or "s3v4",
        addressing_style=addressing_style,
        request_checksum_calculation=checksum,
        file_sizes_mb=file_sizes,
        transfer=transfer,
        repeats=int(os.getenv("REPEATS", "3")),
        warmup=int(os.getenv("WARMUP", "1")),
        verify=parse_bool(os.getenv("VERIFY", "true")),
        keep_objects=parse_bool(os.getenv("KEEP_OBJECTS", "false")),
    )


def load_tuning_ranges() -> dict:
    """Load and validate the grid-search ranges for ``tune-multipart``."""
    load_dotenv()
    keys = ("TUNE_MULTIPART_THRESHOLD", "TUNE_MAX_CONCURRENCY", "TUNE_MULTIPART_CHUNKSIZE")
    ranges = {}
    for key in keys:
        value = os.getenv(key)
        if value is None or value == "":
            raise ConfigError(f"Missing required environment variable '{key}'.")
        ranges[key] = parse_int_list(value)

    threads_raw = os.getenv("TUNE_USE_THREADS")
    if threads_raw is None or threads_raw == "":
        raise ConfigError("Missing required environment variable 'TUNE_USE_THREADS'.")
    ranges["TUNE_USE_THREADS"] = [
        parse_bool(part) for part in threads_raw.split(",") if part.strip()
    ]
    return ranges
