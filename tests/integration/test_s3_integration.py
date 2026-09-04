"""Integration tests that run against LocalStack (requires Docker).

Start LocalStack before running:

    docker compose -f docker-compose.localstack.yml up -d

Then run:

    AWS_ENDPOINT_URL=http://localhost:4566 pytest -m integration
"""

from __future__ import annotations

import pytest

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings
from aws_testkit.utils import unique_name

pytestmark = pytest.mark.integration


@pytest.fixture()
def ls_s3() -> S3Client:
    """Create an S3Client pointing at LocalStack."""
    settings = Settings(
        aws_region="us-east-1",
        aws_endpoint_url="http://localhost:4566",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    return S3Client(settings)


@pytest.fixture()
def bucket(ls_s3: S3Client):
    """Create a temporary bucket and delete it after the test."""
    name = unique_name("integ")
    ls_s3.create_bucket(name)
    yield name
    # cleanup — delete all objects then the bucket
    try:
        for key in _list_keys(ls_s3, name):
            ls_s3.delete_object(name, key)
        ls_s3.delete_bucket(name)
    except Exception:
        pass  # best-effort cleanup


def _list_keys(client: S3Client, bucket_name: str) -> list[str]:
    """Return all object keys in a bucket (via raw client)."""
    resp = client._client.list_objects_v2(Bucket=bucket_name)
    return [obj["Key"] for obj in resp.get("Contents", [])]


# ---------- tests ----------------------------------------------------------


class TestS3Integration:
    """S3 integration tests against LocalStack."""

    def test_create_bucket_and_verify(self, ls_s3: S3Client, bucket: str) -> None:
        assert ls_s3.bucket_exists(bucket)

    def test_put_and_get_object(self, ls_s3: S3Client, bucket: str) -> None:
        key = "integration-test.txt"
        body = "Integration test content"
        ls_s3.put_object(bucket, key, body)

        data = ls_s3.get_object(bucket, key)
        assert data.decode("utf-8") == body

    def test_delete_object(self, ls_s3: S3Client, bucket: str) -> None:
        key = "to-delete.txt"
        ls_s3.put_object(bucket, key, "bye")
        ls_s3.delete_object(bucket, key)

        with pytest.raises(Exception):
            ls_s3.get_object(bucket, key)

    def test_list_buckets(self, ls_s3: S3Client, bucket: str) -> None:
        buckets = ls_s3.list_buckets()
        assert bucket in buckets
