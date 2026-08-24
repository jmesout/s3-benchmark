"""S3 client creation."""
from __future__ import annotations

import boto3
from botocore.client import Config as BotoConfig

from .config import Config


def create_s3_client(cfg: Config):
    """Create a boto3 S3 client from a :class:`Config`.

    ``request_checksum_calculation="when_required"`` is set so that uploads
    work against S3-compatible object stores (Civo, Linode, MinIO, R2, …)
    that reject the extra CRC32 checksums newer boto3 computes by default.
    See boto/boto3#3738. AWS S3 itself is unaffected either way.
    """
    kwargs = {
        "service_name": "s3",
        "endpoint_url": cfg.endpoint_url,
        "config": BotoConfig(
            signature_version=cfg.signature_version,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    }
    # boto3 accepts None for these and falls back to the default credential
    # chain, but only include them when explicitly set to keep that behaviour.
    if cfg.access_key:
        kwargs["aws_access_key_id"] = cfg.access_key
    if cfg.secret_key:
        kwargs["aws_secret_access_key"] = cfg.secret_key

    return boto3.client(**kwargs)