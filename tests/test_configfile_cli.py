"""Tests for Phase 2: config file, profiles, CLI overrides, dry-run."""
import pytest

from s3benchmark.config import Config, TransferParams
from s3benchmark.configfile import ConfigFile, find_config_file


def test_configfile_profile_merge(tmp_path):
    p = tmp_path / "s3benchmark.toml"
    p.write_text(
        """
file_sizes = "10,20"
multipart_threshold = 10485760

[profiles.fast]
multipart_threshold = 5242880
max_concurrency = 32
"""
    )
    cf = ConfigFile.load(str(p))

    # top-level defaults only
    assert cf.file_sizes_mb(None) == [10, 20]
    assert cf.transfer(None).multipart_threshold == 10485760
    assert cf.transfer(None).max_concurrency == 10

    # profile overrides top-level
    t = cf.transfer("fast")
    assert t.multipart_threshold == 5242880
    assert t.max_concurrency == 32
    assert cf.file_sizes_mb("fast") == [10, 20]


def test_configfile_missing_profile(tmp_path):
    p = tmp_path / "s3benchmark.toml"
    p.write_text("file_sizes = '1'\n")
    cf = ConfigFile.load(str(p))
    with pytest.raises(Exception):
        cf.profile("nope")


def test_configfile_list_sizes(tmp_path):
    p = tmp_path / "s3benchmark.toml"
    p.write_text("file_sizes = [1, 2, 3]\n")
    cf = ConfigFile.load(str(p))
    assert cf.file_sizes_mb(None) == [1, 2, 3]


def test_find_config_file_walks_up(tmp_path):
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    cfg = tmp_path / "s3benchmark.toml"
    cfg.write_text("file_sizes = '5'\n")
    assert find_config_file(str(sub)) == str(cfg)


def test_cli_overrides(monkeypatch):
    from s3benchmark.cli import build_parser, _apply_overrides

    cfg = Config(
        access_key="AK", secret_key="SK", endpoint_url=None, bucket="b",
        file_sizes_mb=[1, 2],
    )
    args = build_parser().parse_args(
        ["--file-sizes", "5,6", "--repeats", "7", "--no-verify", "upload"]
    )
    cfg = _apply_overrides(cfg, args)
    assert cfg.file_sizes_mb == [5, 6]
    assert cfg.repeats == 7
    assert cfg.verify is False


def test_cli_overrides_invalid_chunksize(monkeypatch):
    from s3benchmark.cli import build_parser, _apply_overrides
    from s3benchmark.config import ConfigError

    cfg = Config(access_key="AK", secret_key="SK", endpoint_url=None, bucket="b")
    args = build_parser().parse_args(["--multipart-chunksize", "1024", "upload"])
    with pytest.raises(ConfigError):
        _apply_overrides(cfg, args)