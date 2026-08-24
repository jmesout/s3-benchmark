"""Unit tests for Phase 1 workload benchmarks (mocked S3)."""
import math

import pytest

from s3benchmark.config import Config
from s3benchmark.workloads import ConcurrencyResult, SmallObjectBenchmark


@pytest.fixture
def cfg():
    return Config(
        access_key="AK", secret_key="SK",
        endpoint_url="https://x.example.com", bucket="b",
        file_sizes_mb=[1, 2],
    )


def test_concurrency_result_metrics():
    r = ConcurrencyResult(
        direction="upload", concurrency=4, size_mb=1.0,
        total_bytes=4 * 1024 * 1024, total_ops=4, errors=0, wall_seconds=2.0,
    )
    # 4 MiB in 2s = 16.777... Mbps
    assert math.isclose(r.aggregate_mbps(), (4 * 1024 * 1024 * 8) / 2.0 / 1e6, rel_tol=1e-6)
    assert math.isclose(r.ops_per_second, 2.0, rel_tol=1e-9)


def test_concurrency_result_zero_wall():
    r = ConcurrencyResult(
        direction="upload", concurrency=1, size_mb=1.0,
        total_bytes=0, total_ops=0, errors=0, wall_seconds=0.0,
    )
    assert r.aggregate_mbps() == 0.0
    assert r.ops_per_second == 0.0


def test_smallobj_benchmark(monkeypatch, cfg, tmp_path):
    """Small-object benchmark PUT/GET/DELETE with a fake client."""
    calls = []

    class FakeS3:
        def __init__(self, **kwargs):
            pass

        def put_object(self, **kw):
            calls.append(("put", kw["Key"]))

        def get_object(self, **kw):
            calls.append(("get", kw["Key"]))
            return {"Body": b"x"}

        def delete_object(self, **kw):
            calls.append(("delete", kw["Key"]))

    import s3benchmark.workloads as wl

    monkeypatch.setattr(wl, "create_s3_client", lambda c: FakeS3())
    bench = SmallObjectBenchmark(cfg)
    result = bench.run(object_size_bytes=16, num_objects=10, concurrency=2)

    assert result["num_objects"] == 10
    assert result["put"]["n"] == 10
    assert result["get"]["n"] == 10
    assert result["delete"]["n"] == 10
    assert len(calls) == 30