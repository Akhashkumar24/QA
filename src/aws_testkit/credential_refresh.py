"""Retry AWS API calls once after refreshing SSO credentials when tokens expire.

Typical flow: long-running pytest session or cached SSO token hits expiry
(``ExpiredTokenException``). This module runs ``aws sso login --profile …`` and
retries the operation with a new boto3 client.

Environment
------------
CDM_DISABLE_SSO_REFRESH  If ``1``/``true``/``yes``, skip ``aws sso login`` (CI / debugging).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from collections.abc import Callable
from typing import TypeVar

from botocore.exceptions import ClientError, TokenRetrievalError

logger = logging.getLogger(__name__)

T = TypeVar("T")

try:
    from botocore.exceptions import SSOTokenLoadError
except ImportError:
    SSOTokenLoadError = None  # type: ignore[misc, assignment]

_EXPIRED_ERROR_CODES = frozenset(
    {
        "ExpiredTokenException",
        "ExpiredToken",
        "RequestExpired",
        "InvalidClientTokenId",
    }
)


def is_expired_token_error(exc: BaseException) -> bool:
    """Return True if ``exc`` indicates AWS credentials must be refreshed."""
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        if code in _EXPIRED_ERROR_CODES:
            return True
    if isinstance(exc, TokenRetrievalError):
        return True
    if SSOTokenLoadError is not None and isinstance(exc, SSOTokenLoadError):
        return True
    err_msg = str(exc).lower()
    if "expiredtoken" in err_msg.replace("_", ""):
        return True
    if "security token included in the request is expired" in err_msg:
        return True
    if "token has expired" in err_msg and "sso" in err_msg:
        return True
    return False


def try_refresh_sso_credentials(profile: str) -> bool:
    """Run ``aws sso login --profile <profile>``. Return True if the CLI exits 0."""
    if os.getenv("CDM_DISABLE_SSO_REFRESH", "").lower() in ("1", "true", "yes"):
        logger.warning("CDM_DISABLE_SSO_REFRESH set — skipping aws sso login")
        return False
    aws_cli = shutil.which("aws")
    if not aws_cli:
        logger.warning("AWS CLI not found on PATH — cannot run sso login")
        return False
    try:
        r = subprocess.run(
            [aws_cli, "sso", "login", "--profile", profile],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if r.returncode != 0:
            logger.warning(
                "aws sso login failed (exit %s): %s",
                r.returncode,
                (r.stderr or r.stdout or "")[:2000],
            )
            return False
        logger.info("aws sso login succeeded for profile %s", profile)
        return True
    except subprocess.TimeoutExpired:
        logger.warning("aws sso login timed out after 180s")
        return False
    except OSError as exc:
        logger.warning("aws sso login could not run: %s", exc)
        return False


def call_with_credential_refresh(
    profile: str,
    operation: Callable[[], T],
    *,
    max_attempts: int = 2,
) -> T:
    """Run ``operation``; on expired token, run ``aws sso login`` and retry.

    Each call to ``operation`` must construct a fresh boto3 client/session so
    that renewed credentials are picked up after login.
    """
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as exc:
            if attempt >= max_attempts - 1 or not is_expired_token_error(exc):
                raise
            logger.warning(
                "AWS credentials expired or invalid (attempt %d/%d): %s — "
                "attempting aws sso login for profile %s",
                attempt + 1,
                max_attempts,
                exc,
                profile,
            )
            if not try_refresh_sso_credentials(profile):
                raise
