"""File I/O and speed helpers."""
from __future__ import annotations

import os

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB per write for streaming file generation


def create_dummy_file(file_name: str, size_in_mb: int) -> None:
    """Create a dummy file of the given size, streaming so as not to exhaust RAM.

    Previous versions allocated the whole file in memory with ``os.urandom``,
    which OOM-killed at multi-GB sizes. We now write in fixed-size chunks.
    """
    total_bytes = size_in_mb * 1024 * 1024
    remaining = total_bytes
    # A single random chunk reused across writes keeps memory flat while still
    # defeating any per-block compression/zero detection.
    block = _random_block(min(CHUNK_SIZE, total_bytes))
    with open(file_name, "wb") as f:
        while remaining > 0:
            chunk = block if remaining >= len(block) else block[:remaining]
            f.write(chunk)
            remaining -= len(chunk)


def _random_block(size: int) -> bytes:
    return os.urandom(size)


def calculate_speed(time_taken: float, file_size: int) -> float:
    """Return throughput in Mbps (megabits per second), or 0.0 if time is 0."""
    if time_taken <= 0:
        return 0.0
    return (file_size * 8) / time_taken / 1e6