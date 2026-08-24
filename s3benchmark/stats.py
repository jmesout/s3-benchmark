"""Statistics helpers for aggregating repeated benchmark samples."""
from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Sequence
from typing import List


def percentile(sorted_values: Sequence[float], p: float) -> float:
    """Return the p-th percentile (0–100) using linear interpolation."""
    if not sorted_values:
        return 0.0
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def summarize(values: Iterable[float]) -> dict:
    """Compute mean, median, stddev, min, max, and p90/p99 percentiles."""
    data = sorted(float(v) for v in values)
    if not data:
        return {
            "n": 0, "mean": 0.0, "median": 0.0, "stddev": 0.0,
            "min": 0.0, "max": 0.0, "p90": 0.0, "p99": 0.0,
        }
    return {
        "n": len(data),
        "mean": statistics.fmean(data),
        "median": statistics.median(data),
        "stddev": statistics.pstdev(data) if len(data) > 1 else 0.0,
        "min": data[0],
        "max": data[-1],
        "p90": percentile(data, 90),
        "p99": percentile(data, 99),
    }


def summarize_times(samples: List[float]) -> dict:
    """Convenience: summarize a list of elapsed-time samples."""
    return summarize(samples)
