"""Result data model and JSON serialization for benchmark runs."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from .stats import summarize


@dataclass
class TransferSample:
    """A single timed transfer observation."""

    elapsed_s: float
    bytes: int
    ttf_s: float = 0.0
    error: Optional[str] = None


@dataclass
class TransferResult:
    """Aggregated result for one (size, config) combination over N repeats."""

    size_mb: float
    direction: str  # "upload" | "download"
    bytes: int
    samples: List[TransferSample] = field(default_factory=list)
    multipart_threshold: Optional[int] = None
    max_concurrency: Optional[int] = None
    multipart_chunksize: Optional[int] = None
    use_threads: Optional[bool] = None
    parts: int = 0  # number of parts transferred (multipart)
    failed: int = 0  # number of samples that errored
    total_samples: int = 0  # number of attempts including failures
    integrity_ok: Optional[bool] = None  # True if local MD5 matched object ETag
    integrity_error: Optional[str] = None

    @property
    def succeeded(self) -> int:
        return len(self.elapsed_times())

    def elapsed_times(self) -> List[float]:
        return [s.elapsed_s for s in self.samples if s.error is None]

    def speeds_mbps(self) -> List[float]:
        return [
            (s.bytes * 8) / s.elapsed_s / 1e6
            for s in self.samples if s.error is None and s.elapsed_s > 0
        ]

    def summary(self) -> Dict[str, Any]:
        times = self.elapsed_times()
        speeds = self.speeds_mbps()
        t_stats = summarize(times)
        s_stats = summarize(speeds)
        return {
            "size_mb": self.size_mb,
            "direction": self.direction,
            "bytes": self.bytes,
            "attempts": self.total_samples or len(self.samples),
            "succeeded": len(times),
            "failed": self.failed,
            "success_rate": (len(times) / max(1, self.total_samples)) * 100.0,
            "time_s": t_stats,
            "throughput_mbps": s_stats,
            "throughput_mibs": {
                k: (v / 8.0 * 1e6 / (1024 * 1024))  # convert Mbps -> MiB/s
                for k, v in s_stats.items()
            },
            "ttfb_s": summarize([s.ttf_s for s in self.samples if s.error is None]),
            "parts": self.parts,
            "integrity_ok": self.integrity_ok,
            "multipart_threshold": self.multipart_threshold,
            "max_concurrency": self.max_concurrency,
            "multipart_chunksize": self.multipart_chunksize,
            "use_threads": self.use_threads,
        }

    def to_dict(self) -> Dict[str, Any]:
        return self.summary()


@dataclass
class RunMetadata:
    """Metadata describing a single benchmark run."""

    tool_version: str
    benchmark: str
    bucket: str
    endpoint_url: Optional[str]
    started_at: str
    finished_at: str
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RunReport:
    """Top-level report produced by a benchmark run."""

    metadata: RunMetadata
    results: List[TransferResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "results": [r.to_dict() for r in self.results],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_json())
