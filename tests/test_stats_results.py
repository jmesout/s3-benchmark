"""Tests for Phase 0 measurement-correctness modules."""
import math

from s3benchmark.results import TransferResult, TransferSample
from s3benchmark.stats import percentile, summarize


def test_summarize_empty():
    assert summarize([])["n"] == 0


def test_summarize_stats():
    s = summarize([1.0, 2.0, 3.0, 4.0, 5.0])
    assert s["n"] == 5
    assert s["mean"] == 3.0
    assert s["median"] == 3.0
    assert s["min"] == 1.0
    assert s["max"] == 5.0
    assert math.isclose(s["p90"], 4.6, rel_tol=1e-9)
    assert math.isclose(s["p99"], 4.96, rel_tol=1e-9)


def test_percentile_edges():
    data = [1.0, 2.0, 3.0]
    assert percentile(data, 0) == 1.0
    assert percentile(data, 100) == 3.0
    assert percentile(data, 50) == 2.0


def test_transfer_result_summary():
    r = TransferResult(size_mb=1.0, direction="upload", bytes=1048576)
    r.samples = [
        TransferSample(elapsed_s=0.1, bytes=1048576),
        TransferSample(elapsed_s=0.2, bytes=1048576),
    ]
    r.total_samples = 2
    r.failed = 0
    r.integrity_ok = True
    s = r.summary()
    assert s["succeeded"] == 2
    assert s["failed"] == 0
    assert s["integrity_ok"] is True
    # Speeds: 0.1s -> 83.886 Mbps, 0.2s -> 41.943 Mbps; mean = 62.9146
    expected_mbps = ((1048576 * 8 / 0.1) / 1e6 + (1048576 * 8 / 0.2) / 1e6) / 2
    assert math.isclose(s["throughput_mbps"]["mean"], expected_mbps, rel_tol=1e-6)


def test_transfer_result_counts_failures():
    r = TransferResult(size_mb=1.0, direction="download", bytes=1048576)
    r.samples = [TransferSample(elapsed_s=0.1, bytes=1048576)]
    r.failed = 2
    r.total_samples = 3
    s = r.summary()
    assert s["succeeded"] == 1
    assert s["failed"] == 2


def test_throughput_mibs_conversion():
    r = TransferResult(size_mb=1.0, direction="upload", bytes=1048576)
    r.samples = [TransferSample(elapsed_s=1.0, bytes=1048576)]
    r.total_samples = 1
    s = r.summary()
    # 1048576 bytes = 1 MiB, in 1s -> 1 MiB/s
    assert math.isclose(s["throughput_mibs"]["mean"], 1.0, rel_tol=1e-6)


def test_report_to_json(tmp_path):
    from s3benchmark.results import RunMetadata, RunReport

    meta = RunMetadata(
        tool_version="1.0.0", benchmark="upload", bucket="b",
        endpoint_url=None, started_at="", finished_at="",
    )
    rep = RunReport(metadata=meta, results=[])
    p = tmp_path / "report.json"
    rep.save(str(p))
    assert p.exists()
    import json

    d = json.loads(p.read_text())
    assert d["metadata"]["bucket"] == "b"
