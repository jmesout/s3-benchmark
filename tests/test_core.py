"""Tests for s3benchmark helpers."""
import os

import pytest

from s3benchmark.io import calculate_speed, create_dummy_file
from s3benchmark.config import (
    ConfigError,
    TransferParams,
    parse_bool,
    parse_int_list,
    load_config,
)


def test_parse_bool():
    assert parse_bool("True") is True
    assert parse_bool("false") is False
    assert parse_bool("1") is True
    assert parse_bool("yes") is True
    assert parse_bool("no") is False


def test_parse_int_list():
    assert parse_int_list("1,2,3") == [1, 2, 3]
    assert parse_int_list(" 5 , 10 ") == [5, 10]
    assert parse_int_list("") == []


def test_calculate_speed():
    assert calculate_speed(1.0, 1_000_000) == pytest.approx(8.0)
    # Zero/negative time must not raise.
    assert calculate_speed(0.0, 100) == 0.0
    assert calculate_speed(-1.0, 100) == 0.0


def test_create_dummy_file_streams(tmp_path):
    path = tmp_path / "dummy.bin"
    create_dummy_file(str(path), 1)
    assert path.stat().st_size == 1 * 1024 * 1024


def test_transfer_params_validation():
    TransferParams().validate()  # defaults are valid

    with pytest.raises(ConfigError):
        TransferParams(multipart_chunksize=1024).validate()  # < 5 MiB

    with pytest.raises(ConfigError):
        TransferParams(max_concurrency=0).validate()


def test_load_config_requires_bucket(monkeypatch):
    # Override with empty string so a local .env file cannot satisfy this test.
    monkeypatch.setenv("S3_BUCKET_NAME", "")
    with pytest.raises(ConfigError):
        load_config()


def test_load_config_valid(monkeypatch):
    monkeypatch.setenv("S3_BUCKET_NAME", "my-bucket")
    monkeypatch.setenv("FILE_SIZES", "10,20")
    cfg = load_config()
    assert cfg.bucket == "my-bucket"
    assert cfg.file_sizes_mb == [10, 20]