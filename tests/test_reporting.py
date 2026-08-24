"""Tests for Phase 3 reporting: flatten, compare, HTML, plots."""

import pytest

from s3benchmark.reporting import (
    compare_reports,
    flatten_report,
    render_html,
)


def _report(sizes_mbps):
    results = []
    for size_mb, mbps in sizes_mbps:
        results.append({
            "size_mb": size_mb,
            "direction": "upload",
            "bytes": int(size_mb * 1024 * 1024),
            "attempts": 3, "succeeded": 3, "failed": 0,
            "integrity_ok": True,
            "time_s": {"mean": 0.1, "p90": 0.15},
            "throughput_mbps": {"mean": mbps, "p90": mbps * 0.9},
            "throughput_mibs": {"mean": mbps / 8.0 * 1e6 / (1024 * 1024)},
            "multipart_threshold": 50 * 1024 * 1024,
            "max_concurrency": 10,
            "multipart_chunksize": 50 * 1024 * 1024,
            "use_threads": True,
        })
    return {"metadata": {"benchmark": "upload", "bucket": "b"}, "results": results}


def test_flatten_report():
    rep = _report([(1.0, 100.0), (2.0, 200.0)])
    rows = flatten_report(rep)
    assert len(rows) == 2
    assert rows[0]["size_mb"] == 1.0
    assert rows[0]["throughput_mean_mbps"] == 100.0


def test_compare_reports_no_regression():
    base = _report([(1.0, 100.0)])
    cur = _report([(1.0, 105.0)])
    res = compare_reports(base, cur, threshold_pct=10.0)
    assert len(res) == 1
    assert res[0].regressed is False
    assert res[0].delta_pct == pytest.approx(5.0)


def test_compare_reports_detects_regression():
    base = _report([(1.0, 100.0)])
    cur = _report([(1.0, 80.0)])
    res = compare_reports(base, cur, threshold_pct=10.0)
    assert res[0].regressed is True
    assert res[0].delta_pct == pytest.approx(-20.0)


def test_compare_reports_handles_missing():
    base = _report([(1.0, 100.0)])
    cur = _report([(2.0, 200.0)])  # size 2 not in baseline
    assert compare_reports(base, cur) == []


def test_render_html_contains_rows():
    rep = _report([(1.0, 100.0)])
    html = render_html(rep)
    assert "<table>" in html
    assert "100.0" in html
    assert "OK" in html
