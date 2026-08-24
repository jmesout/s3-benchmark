"""S3 client creation."""
from __future__ import annotations

import boto3
from botocore.client import Config as BotoConfig

from .config import Config


def create_s3_client(cfg: Config):
    """Create a boto3 S3 client from a :class:`Config`."""
    kwargs = {
        "service_name": "s3",
        "endpoint_url": cfg.endpoint_url,
        "config": BotoConfig(signature_version=cfg.signature_version),
    }
    # boto3 accepts None for these and falls back to the default credential
    # chain, but only include them when explicitly set to keep that behaviour.
    if cfg.access_key:
        kwargs["aws_access_key_id"] = cfg.access_key
    if cfg.secret_key:
        kwargs["aws_secret_access_key"] = cfg.secret_key

    return boto3.client(**kwargs)