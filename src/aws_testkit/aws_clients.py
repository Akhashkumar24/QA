"""Thin wrapper around boto3 clients with LocalStack/endpoint support."""

from __future__ import annotations

import time
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from aws_testkit.config import Settings

# Default retry configuration for boto3 client
_RETRY_CONFIG = BotoConfig(retries={"max_attempts": 3, "mode": "standard"})


class S3Client:
    """Lightweight S3 helper that wraps a boto3 S3 client.

    Supports LocalStack via ``Settings.aws_endpoint_url``.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self._client = self._build_client()

    # -- client construction ---------------------------------------------------

    def _build_client(self) -> Any:
        kwargs: dict[str, Any] = {
            "service_name": "s3",
            "region_name": self.settings.aws_region,
            "config": _RETRY_CONFIG,
        }
        # When explicit credentials are provided (moto / LocalStack), pass them.
        # Otherwise let boto3 use its default chain (~/.aws/credentials, IAM role, etc.)
        if self.settings.aws_access_key_id is not None:
            kwargs["aws_access_key_id"] = self.settings.aws_access_key_id
        if self.settings.aws_secret_access_key is not None:
            kwargs["aws_secret_access_key"] = self.settings.aws_secret_access_key
        # STS temporary credentials require a session token
        if self.settings.aws_session_token is not None:
            kwargs["aws_session_token"] = self.settings.aws_session_token
        if self.settings.aws_endpoint_url:
            kwargs["endpoint_url"] = self.settings.aws_endpoint_url
        # Support named AWS CLI profiles (e.g. AWS_PROFILE=my-profile)
        if self.settings.aws_profile:
            session = boto3.Session(profile_name=self.settings.aws_profile)
            return session.client(**kwargs)
        return boto3.client(**kwargs)

    # -- bucket operations -----------------------------------------------------

    def create_bucket(self, bucket_name: str) -> dict[str, Any]:
        """Create an S3 bucket. Handles LocationConstraint for non-us-east-1."""
        params: dict[str, Any] = {"Bucket": bucket_name}
        if self.settings.aws_region != "us-east-1":
            params["CreateBucketConfiguration"] = {
                "LocationConstraint": self.settings.aws_region,
            }
        return self._client.create_bucket(**params)

    def bucket_exists(self, bucket_name: str) -> bool:
        """Return True if the bucket exists (head_bucket succeeds)."""
        try:
            self._client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False

    def delete_bucket(self, bucket_name: str) -> None:
        """Delete a bucket (must be empty)."""
        try:
            self._client.delete_bucket(Bucket=bucket_name)
        except ClientError:
            pass  # best-effort cleanup

    def list_buckets(self) -> list[str]:
        """Return a list of bucket names."""
        resp = self._client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]

    # -- object operations -----------------------------------------------------

    def put_object(self, bucket_name: str, key: str, body: bytes | str) -> dict[str, Any]:
        """Upload an object to S3."""
        if isinstance(body, str):
            body = body.encode("utf-8")
        return self._client.put_object(Bucket=bucket_name, Key=key, Body=body)

    def get_object(self, bucket_name: str, key: str) -> bytes:
        """Download an object and return its body bytes."""
        resp = self._client.get_object(Bucket=bucket_name, Key=key)
        return resp["Body"].read()

    def delete_object(self, bucket_name: str, key: str) -> None:
        """Delete a single object."""
        self._client.delete_object(Bucket=bucket_name, Key=key)

    # -- helpers ---------------------------------------------------------------

    def wait_for_bucket(
        self, bucket_name: str, *, timeout: float = 30.0, interval: float = 2.0
    ) -> bool:
        """Poll until the bucket exists or timeout is reached.

        Useful for eventual-consistency scenarios in real AWS.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.bucket_exists(bucket_name):
                return True
            time.sleep(interval)
        return False


class CloudWatchLogsClient:
    """Lightweight CloudWatch Logs helper that wraps a boto3 logs client.

    Supports LocalStack via ``Settings.aws_endpoint_url``.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self._client = self._build_client()

    def _build_client(self) -> Any:
        kwargs: dict[str, Any] = {
            "service_name": "logs",
            "region_name": self.settings.aws_region,
            "config": _RETRY_CONFIG,
        }
        if self.settings.aws_access_key_id is not None:
            kwargs["aws_access_key_id"] = self.settings.aws_access_key_id
        if self.settings.aws_secret_access_key is not None:
            kwargs["aws_secret_access_key"] = self.settings.aws_secret_access_key
        if self.settings.aws_session_token is not None:
            kwargs["aws_session_token"] = self.settings.aws_session_token
        if self.settings.aws_endpoint_url:
            kwargs["endpoint_url"] = self.settings.aws_endpoint_url
        if self.settings.aws_profile:
            session = boto3.Session(profile_name=self.settings.aws_profile)
            return session.client(**kwargs)
        return boto3.client(**kwargs)

    # -- log group operations --------------------------------------------------

    def log_group_exists(self, log_group_name: str) -> bool:
        """Return True if the given CloudWatch log group exists."""
        try:
            resp = self._client.describe_log_groups(logGroupNamePrefix=log_group_name)
            return any(
                lg["logGroupName"] == log_group_name
                for lg in resp.get("logGroups", [])
            )
        except ClientError:
            return False

    def get_recent_log_streams(
        self, log_group_name: str, *, limit: int = 5
    ) -> list[dict[str, Any]]:
        """Return the most recent log streams for the given log group."""
        resp = self._client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy="LastEventTime",
            descending=True,
            limit=limit,
        )
        return resp.get("logStreams", [])

    def get_log_events(
        self,
        log_group_name: str,
        log_stream_name: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return log events from a specific stream."""
        resp = self._client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            limit=limit,
            startFromHead=False,
        )
        return resp.get("events", [])

    def filter_log_events(
        self,
        log_group_name: str,
        *,
        filter_pattern: str = "",
        limit: int = 100,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        """Filter log events across all streams in the log group.

        Parameters
        ----------
        filter_pattern : str
            CloudWatch Logs filter pattern (e.g. ``'"completed successfully"'``).
        limit : int
            Maximum events to return.
        start_time / end_time : int, optional
            Epoch milliseconds to bound the query window.
        """
        kwargs: dict[str, Any] = {
            "logGroupName": log_group_name,
            "limit": limit,
            "interleaved": True,
        }
        if filter_pattern:
            kwargs["filterPattern"] = filter_pattern
        if start_time is not None:
            kwargs["startTime"] = start_time
        if end_time is not None:
            kwargs["endTime"] = end_time

        all_events: list[dict[str, Any]] = []
        while True:
            resp = self._client.filter_log_events(**kwargs)
            all_events.extend(resp.get("events", []))
            next_token = resp.get("nextToken")
            if not next_token or len(all_events) >= limit:
                break
            kwargs["nextToken"] = next_token

        return all_events[:limit]
