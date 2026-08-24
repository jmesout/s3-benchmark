"""Tests for Phase 5: provider presets + credential best practices."""
import os

import pytest

from s3benchmark.presets import (
    PROVIDERS,
    get_preset,
    list_presets,
    provider_help,
)


def test_presets_available():
    names = list_presets()
    assert "civo" in names
    assert "aws" in names
    assert "minio" in names
    assert len(names) >= 8


def test_get_preset_civo():
    p = get_preset("civo")
    assert p.endpoint_url == "https://objectstore.lon1.civo.com"
    assert p.signature_version == "s3v4"
    assert p.addressing_style == "path"
    # Civo (like other S3-compat stores) needs checksum calc disabled.
    assert p.request_checksum_calculation == "when_required"


def test_get_preset_aws_uses_supported_checksums():
    p = get_preset("aws")
    assert p.signature_version == "s3v4"
    assert p.request_checksum_calculation == "when_supported"


def test_get_preset_case_insensitive():
    assert get_preset("CIVO") == get_preset("civo")


def test_get_preset_unknown():
    with pytest.raises(KeyError):
        get_preset("nonexistent-provider")


def test_provider_help_lists_names():
    h = provider_help()
    assert "civo" in h
    assert "minio" in h


def test_load_config_with_provider(monkeypatch):
    from s3benchmark.config import load_config

    monkeypatch.setenv("S3_BUCKET_NAME", "my-bucket")
    monkeypatch.setenv("S3_PROVIDER", "civo")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    cfg = load_config()
    assert cfg.endpoint_url == "https://objectstore.lon1.civo.com"
    assert cfg.signature_version == "s3v4"
    assert cfg.addressing_style == "path"


def test_load_config_env_overrides_preset(monkeypatch):
    from s3benchmark.config import load_config

    monkeypatch.setenv("S3_BUCKET_NAME", "my-bucket")
    monkeypatch.setenv("S3_PROVIDER", "civo")
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://custom.example.com")
    cfg = load_config()
    assert cfg.endpoint_url == "https://custom.example.com"