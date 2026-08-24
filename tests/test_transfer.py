"""Tests for client creation and transfer config."""
import boto3

from s3benchmark.config import Config
from s3benchmark.transfer import create_s3_client


def test_client_sets_checksum_calculation(monkeypatch):
    """S3-compatible stores (Civo/Linode/MinIO/R2) reject boto3's default
    extra CRC32 checksums. We must force 'when_required'."""
    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            config = kwargs["config"]
            captured["request_checksum_calculation"] = (
                config.request_checksum_calculation
            )
            captured["response_checksum_validation"] = (
                config.response_checksum_validation
            )
            captured["signature_version"] = config.signature_version

    monkeypatch.setattr(boto3, "client", lambda **kw: FakeClient(**kw))

    cfg = Config(
        access_key="AK",
        secret_key="SK",
        endpoint_url="https://x.example.com",
        bucket="b",
    )
    create_s3_client(cfg)

    assert captured["request_checksum_calculation"] == "when_required"
    assert captured["response_checksum_validation"] == "when_required"
    assert captured["signature_version"] == "s3v4"
