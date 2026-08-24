"""S3 client creation."""
from __future__ import annotations

import boto3
from botocore.client import Config as BotoConfig

from .config import Config


def create_s3_client(cfg: Config):
    """Create a boto3 S3 client from a :class:`Config`.

    Checksum calculation defaults to ``when_required`` so that uploads work
    against S3-compatible object stores (Civo, Linode, MinIO, R2, …) that
    reject the extra CRC32 checksums newer boto3 computes by default. Providers
    that support them (e.g. AWS) override this via their preset.

    Credentials are optional: when absent, boto3 falls back to the default
    credential chain (env, ~/.aws/credentials, IAM role, …).
    """
    kwargs = {
        "service_name": "s3",
        "endpoint_url": cfg.endpoint_url,
        "config": BotoConfig(
            signature_version=cfg.signature_version,
            s3={"addressing_style": cfg.addressing_style},
            request_checksum_calculation=cfg.request_checksum_calculation,
            response_checksum_validation="when_required",
        ),
    }
    if cfg.access_key:
        kwargs["aws_access_key_id"] = cfg.access_key
    if cfg.secret_key:
        kwargs["aws_secret_access_key"] = cfg.secret_key

    return boto3.client(**kwargs)
