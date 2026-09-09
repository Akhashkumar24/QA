"""Read-only AWS reachability gate, one case per use case.

Deselected by default (``-m preflight`` to run). Equivalent to
``aws-testkit preflight`` but as pytest, for CI pipelines that key off markers.
"""

from __future__ import annotations

import pytest

from aws_testkit.aws_clients import S3Client, StsClient
from aws_testkit.config import Settings
from aws_testkit.usecase import list_usecases, load_usecase

pytestmark = pytest.mark.preflight


@pytest.mark.parametrize("usecase_id", list_usecases())
def test_usecase_is_reachable(usecase_id):
    cfg = load_usecase(usecase_id)
    st = Settings(aws_region=cfg["region"])

    profiles = {
        blk.get("profile")
        for key in ("lambda", "glue", "s3", "sqs")
        if isinstance(blk := cfg.get(key), dict)
    }
    for prof in profiles:
        ident = StsClient(st, profile=prof, region=cfg["region"]).caller_identity()
        assert ident.get("Account"), f"no caller identity for profile {prof!r}"

    s3 = cfg.get("s3")
    if isinstance(s3, dict) and s3.get("bucket"):
        client = S3Client(st, profile=s3.get("profile"), region=cfg["region"])
        assert client.bucket_exists(s3["bucket"]), f"cannot reach s3://{s3['bucket']}"
