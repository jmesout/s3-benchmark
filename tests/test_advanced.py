"""Tests for Phase 6 advanced features (mostly pure logic + cost)."""
import math

import pytest

from s3benchmark.advanced import CostEstimate, InMemoryBenchmark, estimate_cost


def test_cost_estimate_upload():
    est = estimate_cost([1.0, 2.0, 4.0], repeats=3, warmup=1, direction="upload")
    b = est.breakdown()
    assert b["requests"] == 3 * (3 + 1)  # 3 sizes x 4 transfers
    assert b["egress_gb"] == 0.0  # uploads don't incur egress
    assert b["storage_gb"] == pytest.approx(4.0 / 1024.0)


def test_cost_estimate_download():
    est = estimate_cost([100.0, 500.0, 1024.0], repeats=3, warmup=1, direction="download")
    b = est.breakdown()
    assert b["requests"] == 3 * (3 + 1)
    # egress = total MB x transfers / 1024
    expected_egress = (100 + 500 + 1024) * 4 / 1024.0
    assert b["egress_gb"] == pytest.approx(expected_egress)
    assert b["total_usd"] > 0


def test_cost_estimate_breakdown_math():
    est = CostEstimate(storage_gb=1.0, requests=1000, egress_gb=2.0)
    expected = (
        1.0 * 0.02          # storage
        + (1000 / 1000) * 0.005   # put
        + (1000 / 1000) * 0.0004  # get
        + 2.0 * 0.09        # egress
    )
    assert math.isclose(est.total_usd(), expected, rel_tol=1e-9)


def test_inmem_raises_on_large_object():
    from s3benchmark.config import Config

    cfg = Config(access_key="AK", secret_key="SK", endpoint_url=None, bucket="b")
    bench = InMemoryBenchmark.__new__(InMemoryBenchmark)  # skip __init__ (no client)
    bench.cfg = cfg
    with pytest.raises(ValueError):
        bench.run(300.0)  # > 256MB cap


def test_cost_cli_default_sizes(monkeypatch, capsys):
    from s3benchmark.cli import run_cost
    from s3benchmark.config import Config

    cfg = Config(access_key="AK", secret_key="SK", endpoint_url=None, bucket="b",
                 file_sizes_mb=[1, 2, 4])
    args = type("Args", (), {"sizes": None, "direction": "upload"})()
    run_cost(cfg, args)
    out = capsys.readouterr().out
    assert "Cost estimate" in out
    assert "12" in out  # 3 sizes x 4 transfers
