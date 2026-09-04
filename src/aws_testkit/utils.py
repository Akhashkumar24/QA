"""Shared utility helpers."""

from __future__ import annotations

import uuid


def unique_name(prefix: str = "testkit") -> str:
    """Return a deterministic-length unique name safe for S3 bucket naming.

    Example output: ``testkit-a1b2c3d4``
    """
    short_id = uuid.uuid4().hex[:8]
    return f"{prefix}-{short_id}"
