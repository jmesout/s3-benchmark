"""Provider presets for common S3-compatible object stores.

Each preset encodes the correct ``signature_version``, default endpoint,
addressing style, and known quirks so users can target a provider by name
instead of hand-writing ``.env`` values.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ProviderPreset:
    """A known S3-compatible provider's connection defaults."""

    name: str
    signature_version: str = "s3v4"
    endpoint_url: Optional[str] = None
    addressing_style: str = "path"  # many S3-compat stores require path-style
    request_checksum_calculation: str = "when_required"
    response_checksum_validation: str = "when_required"
    notes: str = ""


# Provider table. Endpoints are templates; region may be substituted.
PROVIDERS: Dict[str, ProviderPreset] = {
    "aws": ProviderPreset(
        name="aws",
        signature_version="s3v4",
        addressing_style="virtual",
        request_checksum_calculation="when_supported",
        response_checksum_validation="when_supported",
        notes="Default AWS S3. Uses the standard credential chain.",
    ),
    "civo": ProviderPreset(
        name="civo",
        signature_version="s3v4",
        endpoint_url="https://objectstore.lon1.civo.com",
        addressing_style="path",
        notes="Civo Object Store. Rejects extra CRC32 checksums; we disable them.",
    ),
    "cloudflare-r2": ProviderPreset(
        name="cloudflare-r2",
        signature_version="s3v4",
        endpoint_url="https://<account_id>.r2.cloudflarestorage.com",
        addressing_style="path",
        notes="Cloudflare R2. Requires S3-compatible API keys (not account API tokens).",
    ),
    "backblaze-b2": ProviderPreset(
        name="backblaze-b2",
        signature_version="s3v4",
        endpoint_url="https://s3.<region>.backblazeb2.com",
        addressing_style="path",
        notes="Backblaze B2 S3 API. Uses keyID+applicationKey as access/secret.",
    ),
    "minio": ProviderPreset(
        name="minio",
        signature_version="s3v4",
        endpoint_url="http://localhost:9000",
        addressing_style="path",
        notes="MinIO (self-hosted). Endpoint is your MinIO server.",
    ),
    "digitalocean": ProviderPreset(
        name="digitalocean",
        signature_version="s3v4",
        endpoint_url="https://<region>.digitaloceanspaces.com",
        addressing_style="path",
        notes="DigitalOcean Spaces. Bucket is your Space name.",
    ),
    "gcs": ProviderPreset(
        name="gcs",
        signature_version="s3v4",
        endpoint_url="https://storage.googleapis.com",
        addressing_style="path",
        notes="Google Cloud Storage via S3-compat (HMAC keys).",
    ),
    "oracle": ProviderPreset(
        name="oracle",
        signature_version="s3v4",
        endpoint_url="https://<namespace>.compat.objectstorage.<region>.oraclecloud.com",
        addressing_style="path",
        notes="OCI Object Storage S3 API (customer secret keys).",
    ),
    "scaleway": ProviderPreset(
        name="scaleway",
        signature_version="s3v4",
        endpoint_url="https://s3.<region>.scw.cloud",
        addressing_style="path",
        notes="Scaleway Object Storage.",
    ),
    "wasabi": ProviderPreset(
        name="wasabi",
        signature_version="s3v4",
        endpoint_url="https://s3.<region>.wasabisys.com",
        addressing_style="path",
        notes="Wasabi Hot Cloud Storage.",
    ),
}


def get_preset(name: str) -> ProviderPreset:
    key = name.lower()
    if key not in PROVIDERS:
        available = ", ".join(sorted(PROVIDERS))
        raise KeyError(f"Unknown provider '{name}'. Available: {available}")
    return PROVIDERS[key]


def list_presets() -> List[str]:
    return sorted(PROVIDERS)


def provider_help() -> str:
    """Human-readable table of provider presets for the CLI/README."""
    lines = []
    for name in list_presets():
        p = PROVIDERS[name]
        endpoint = p.endpoint_url or "(region-based)"
        lines.append(f"  {name:<14} {endpoint}")
    return "\n".join(lines)
