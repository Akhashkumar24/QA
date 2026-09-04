"""Unit tests for S3 operations using moto (no Docker required)."""

from __future__ import annotations

import pytest

from aws_testkit.aws_clients import S3Client
from aws_testkit.utils import unique_name


# All tests in this file are unit tests
pytestmark = pytest.mark.unit


class TestS3BucketCreation:
    """Bucket lifecycle tests backed by moto."""

    def test_create_bucket(self, moto_s3: S3Client) -> None:
        bucket = unique_name("unit")
        moto_s3.create_bucket(bucket)
        assert moto_s3.bucket_exists(bucket)

    def test_bucket_does_not_exist(self, moto_s3: S3Client) -> None:
        assert not moto_s3.bucket_exists("no-such-bucket-ever")

    def test_list_buckets_includes_created(self, moto_s3: S3Client) -> None:
        bucket = unique_name("unit")
        moto_s3.create_bucket(bucket)
        assert bucket in moto_s3.list_buckets()

    def test_delete_bucket(self, moto_s3: S3Client) -> None:
        bucket = unique_name("unit")
        moto_s3.create_bucket(bucket)
        moto_s3.delete_bucket(bucket)
        assert not moto_s3.bucket_exists(bucket)


class TestS3Objects:
    """Object put/get tests backed by moto."""

    def test_put_and_get_object(self, moto_s3: S3Client) -> None:
        bucket = unique_name("unit")
        moto_s3.create_bucket(bucket)

        key = "greeting.txt"
        body = "Hello from moto!"
        moto_s3.put_object(bucket, key, body)

        data = moto_s3.get_object(bucket, key)
        assert data.decode("utf-8") == body

    def test_put_bytes_object(self, moto_s3: S3Client) -> None:
        bucket = unique_name("unit")
        moto_s3.create_bucket(bucket)

        key = "binary.bin"
        body = b"\x00\x01\x02\x03"
        moto_s3.put_object(bucket, key, body)

        assert moto_s3.get_object(bucket, key) == body

    def test_delete_object(self, moto_s3: S3Client) -> None:
        bucket = unique_name("unit")
        moto_s3.create_bucket(bucket)
        moto_s3.put_object(bucket, "tmp.txt", "temp")
        moto_s3.delete_object(bucket, "tmp.txt")

        with pytest.raises(Exception):
            moto_s3.get_object(bucket, "tmp.txt")
