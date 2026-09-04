"""Centralised configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Real S3 bucket details (INT environment)
# ---------------------------------------------------------------------------
S3_BUCKET_NAME = os.getenv(
    "S3_BUCKET_NAME", "ac-odh-derived-event-storage-int-cac-1"
)
S3_KEY_PREFIX = os.getenv(
    "S3_KEY_PREFIX", "CDM/processed/CDM-AIRCRAFT-ODH-INT/"
)


@dataclass(frozen=True)
class Settings:
    """Immutable application settings resolved from environment variables.

    Environment variables
    ---------------------
    AWS_REGION            – AWS region (default: ca-central-1)
    AWS_ENDPOINT_URL      – custom endpoint, e.g. http://localhost:4566 for LocalStack
    AWS_ACCESS_KEY_ID     – access key (None → boto3 uses ~/.aws/credentials)
    AWS_SECRET_ACCESS_KEY – secret key (None → boto3 uses ~/.aws/credentials)
    AWS_SESSION_TOKEN     – session token for temporary STS credentials (optional)
    AWS_PROFILE           – named CLI profile to use (optional)
    S3_BUCKET_NAME        – target S3 bucket (default: ac-odh-derived-event-storage-int-cac-1)
    S3_KEY_PREFIX         – key prefix in bucket (default: CDM/processed/CDM-AIRCRAFT-ODH-INT/)
    """

    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "ca-central-1"))
    aws_endpoint_url: str | None = field(default_factory=lambda: os.getenv("AWS_ENDPOINT_URL"))
    aws_access_key_id: str | None = field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID")
    )
    aws_secret_access_key: str | None = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    aws_session_token: str | None = field(
        default_factory=lambda: os.getenv("AWS_SESSION_TOKEN")
    )
    aws_profile: str | None = field(
        default_factory=lambda: os.getenv("AWS_PROFILE")
    )
    s3_bucket_name: str = field(default_factory=lambda: S3_BUCKET_NAME)
    s3_key_prefix: str = field(default_factory=lambda: S3_KEY_PREFIX)

    @property
    def is_localstack(self) -> bool:
        """Return True when targeting a LocalStack endpoint."""
        return self.aws_endpoint_url is not None

    @property
    def uses_cli_profile(self) -> bool:
        """Return True when no explicit credentials are set (boto3 uses ~/.aws/credentials)."""
        return self.aws_access_key_id is None and self.aws_secret_access_key is None
