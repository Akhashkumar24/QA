"""AWS credential/endpoint settings resolved from environment variables.

Target-specific values (bucket names, prefixes, Lambda names, schemas) live in
``conf/usecases/<id>.yaml``, not here. This class only carries how to
*authenticate* and *reach* AWS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Immutable AWS connection settings.

    Environment variables
    ---------------------
    AWS_REGION            – default region (default: ca-central-1)
    AWS_ENDPOINT_URL      – override the service endpoint (VPC endpoint, gov cloud, …)
    AWS_ACCESS_KEY_ID     – access key (unset → boto3 default credential chain / SSO)
    AWS_SECRET_ACCESS_KEY – secret key (unset → boto3 default credential chain / SSO)
    AWS_SESSION_TOKEN     – session token for temporary STS credentials (optional)
    AWS_PROFILE           – named CLI/SSO profile (optional; usually set per use case)
    """

    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "ca-central-1"))
    aws_endpoint_url: str | None = field(default_factory=lambda: os.getenv("AWS_ENDPOINT_URL"))
    aws_access_key_id: str | None = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: str | None = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    aws_session_token: str | None = field(default_factory=lambda: os.getenv("AWS_SESSION_TOKEN"))
    aws_profile: str | None = field(default_factory=lambda: os.getenv("AWS_PROFILE"))
