"""Reporting: HTML reports, cross-run comparison, and better plots.

Phase 3 builds on the JSON report (``results.py``) to add:
  * self-contained HTML reports with tables and inline charts
  * cross-run comparison / regression detection against a baseline
  * improved plots (log-scale option, error bars, upload-vs-download)
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


# --------------------------------------------------------------------------- #
# Flattening helpers
# --------------------------------------------------------------------------- #
def flatten_report(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten a JSON report into one row per result for CSV/Parquet export."""
    rows = []
    for r in report.get("results", []):
        row = {
            "size_mb": r.get("size_mb"),
            "direction": r.get("direction"),
            "bytes": r.get("bytes"),
            "attempts": r.get("attempts"),
            "succeeded": r.get("succeeded"),
            "failed": r.get("failed"),
            "integrity_ok": r.get("integrity_ok"),
            "time_mean_s": r["time_s"]["mean"] if "time_s" in r else None,
            "time_p90_s": r["time_s"]["p90"] if "time_s" in r else None,
            "throughput_mean_mbps": r["throughput_mbps"]["mean"] if "throughput_mbps" in r else None,
            "throughput_p90_mbps": r["throughput_mbps"]["p90"] if "throughput_mbps" in r else None,
            "throughput_mean_mibs": r["throughput_mibs"]["mean"] if "throughput_mibs" in r else None,
            "multipart_threshold": r.get("multipart_threshold"),
            "max_concurrency": r.get("max_concurrency"),
            "multipart_chunksize": r.get("multipart_chunksize"),
            "use_threads": r.get("use_threads"),
        }
        rows.append(row)
    return rows


def to_parquet(report: Dict[str, Any], path: str) -> None:
    """Write a flattened report to Parquet (requires pyarrow)."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Parquet output requires pyarrow: pip install s3-benchmark[parquet]"
        ) from exc
    rows = flatten_report(report)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


# --------------------------------------------------------------------------- #
# Cross-run comparison
# --------------------------------------------------------------------------- #
@dataclass
class ComparisonResult:
    size_mb: float
    direction: str
    baseline_mbps: float
    current_mbps: float
    delta_pct: float
    regressed: bool


def compare_reports(baseline: Dict[str, Any], current: Dict[str, Any],
                    threshold_pct: float = 10.0) -> List[ComparisonResult]:
    """Compare two reports and flag regressions beyond threshold percent."""
    base_map = {(r["size_mb"], r["direction"]): r for r in baseline.get("results", [])}
    cur_map = {(r["size_mb"], r["direction"]): r for r in current.get("results", [])}

    out: List[ComparisonResult] = []
    for key, cur in cur_map.items():
        base = base_map.get(key)
        if base is None:
            continue
        b_mbps = base["throughput_mbps"]["mean"]
        c_mbps = cur["throughput_mbps"]["mean"]
        if b_mbps <= 0:
            continue
        delta = (c_mbps - b_mbps) / b_mbps * 100.0
        out.append(ComparisonResult(
            size_mb=cur["size_mb"], direction=cur["direction"],
            baseline_mbps=b_mbps, current_mbps=c_mbps,
            delta_pct=delta, regressed=delta <= -threshold_pct,
        ))
    return out


def compare_report_files(baseline_path: str, current_path: str,
                         threshold_pct: float = 10.0) -> List[ComparisonResult]:
    with open(baseline_path) as f:
        baseline = json.load(f)
    with open(current_path) as f:
        current = json.load(f)
    return compare_reports(baseline, current, threshold_pct)


# --------------------------------------------------------------------------- #
# Better plots
# --------------------------------------------------------------------------- #
def plot_size_speed_with_errors(report: Dict[str, Any], output_file: str) -> None:
    """Plot throughput vs file size with error bars from repeated samples."""
    results = report.get("results", [])
    if not results:
        return
    sizes = [r["size_mb"] for r in results]
    means = [r["throughput_mbps"]["mean"] for r in results]
    errs = [r["throughput_mbps"]["stddev"] for r in results]

    plt.figure(figsize=(10, 6))
    plt.errorbar(sizes, means, yerr=errs, marker="o", capsize=5)
    plt.xscale("log" if max(sizes) / max(1, min(sizes)) > 10 else "linear")
    plt.title("Throughput by File Size")
    plt.xlabel("File Size (MB)")
    plt.ylabel("Throughput (Mbps)")
    plt.grid(True, which="both", ls="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()


def plot_upload_vs_download(report: Dict[str, Any], output_file: str) -> None:
    """Overlay upload and download throughput on one chart."""
    groups: Dict[str, List[Any]] = {}
    for r in report.get("results", []):
        groups.setdefault(r["direction"], []).append(r)

    plt.figure(figsize=(10, 6))
    for direction, results in groups.items():
        sizes = [r["size_mb"] for r in results]
        means = [r["throughput_mbps"]["mean"] for r in results]
        plt.plot(sizes, means, marker="o", label=direction)
    plt.title("Upload vs Download Throughput")
    plt.xlabel("File Size (MB)")
    plt.ylabel("Throughput (Mbps)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()


# --------------------------------------------------------------------------- #
# HTML report
# --------------------------------------------------------------------------- #
_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ border-bottom: 2px solid #eee; padding-bottom: .5rem; }}
  table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
  th, td {{ border: 1px solid #ddd; padding: .5rem .75rem; text-align: right; }}
  th {{ background: #f5f5f5; }}
  td:first-child, th:first-child {{ text-align: left; }}
  .meta {{ color: #555; font-size: .9rem; }}
  .regress {{ color: #b00020; font-weight: bold; }}
  .ok {{ color: #0a7d0a; }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="meta">{meta}</div>
<table>
<thead><tr><th>Size (MB)</th><th>Direction</th><th>Mean Mbps</th><th>Mean MiB/s</th><th>p90 Mbps</th><th>p99 Mbps</th><th>n</th><th>failed</th><th>integrity</th></tr></thead>
<tbody>
{rows}
</tbody>
</table>
</body>
</html>
"""


def _meta_html(meta: Dict[str, Any]) -> str:
    parts = [f"<b>benchmark</b>: {meta.get('benchmark', '')}",
             f"<b>bucket</b>: {meta.get('bucket', '')}",
             f"<b>started</b>: {meta.get('started_at', '')}"]
    params = meta.get("params", {})
    if params:
        parts.append("<b>params</b>: " + ", ".join(
            f"{k}={v}" for k, v in params.items()))
    return " &nbsp;|&nbsp; ".join(parts)


def render_html(report: Dict[str, Any]) -> str:
    meta = report.get("metadata", {})
    rows = []
    for r in report.get("results", []):
        integr = ("<span class='ok'>OK</span>" if r.get("integrity_ok")
                  else ("FAIL" if r.get("integrity_ok") is False else "—"))
        tp = r.get("throughput_mbps", {})
        tm = r.get("throughput_mibs", {})
        rows.append(
            f"<tr><td>{r.get('size_mb')}</td>"
            f"<td>{r.get('direction')}</td>"
            f"<td>{tp.get('mean', 0):.1f}</td>"
            f"<td>{tm.get('mean', 0):.1f}</td>"
            f"<td>{tp.get('p90', 0):.1f}</td>"
            f"<td>{tp.get('p99', 0):.1f}</td>"
            f"<td>{r.get('succeeded')}</td>"
            f"<td>{r.get('failed')}</td>"
            f"<td>{integr}</td></tr>"
        )
    return _HTML_TEMPLATE.format(
        title=f"s3-benchmark — {meta.get('benchmark', '')}",
        meta=_meta_html(meta),
        rows="\n".join(rows),
    )


def write_html_report(report: Dict[str, Any], path: str) -> None:
    with open(path, "w") as f:
        f.write(render_html(report))