"""Quick connectivity test for AWS S3 using current credentials.

Run:
    py -m pytest tests/test_aws_connectivity.py -v

Credentials are read from environment variables:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN  (for temporary STS credentials)
"""

from __future__ import annotations

import json
import logging
import os

import boto3
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

# ---------------------------------------------------------------------------
# Logger — output captured by pytest-html and Allure reports
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Target S3 bucket and object
# ---------------------------------------------------------------------------
BUCKET = "ac-odh-derived-event-storage-int-cac1"
REGION = "ca-central-1"
PREFIX = "CDM/processed/CDM-AIRCRAFT-ODH-INT/"


@pytest.fixture(scope="module")
def s3_client():
    """Create a boto3 S3 client using environment variable credentials.

    Supports both long-lived IAM keys and temporary STS session tokens.
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=REGION,
    )
    return session.client("s3")


@pytest.fixture(scope="module")
def latest_file_key(s3_client):
    """Find the most recently modified JSON file under the prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    all_objects = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                all_objects.append(obj)

    assert len(all_objects) > 0, f"No JSON files found under '{PREFIX}'"

    # Sort by LastModified descending — pick the newest file
    latest = max(all_objects, key=lambda o: o["LastModified"])
    logger.info("Latest file: %s (modified: %s, %d bytes)",
                latest["Key"], latest["LastModified"], latest["Size"])
    return latest["Key"]


# =========================================================================
# Connectivity Tests
# =========================================================================


class TestAWSConnectivity:
    """Verify basic AWS S3 connectivity and access."""

    def test_credentials_are_set(self):
        """Ensure AWS credentials are present in environment."""
        assert os.getenv("AWS_ACCESS_KEY_ID"), "AWS_ACCESS_KEY_ID is not set"
        assert os.getenv("AWS_SECRET_ACCESS_KEY"), "AWS_SECRET_ACCESS_KEY is not set"
        logger.info("Access Key: ...%s", os.getenv("AWS_ACCESS_KEY_ID")[-4:])
        logger.info("Session Token: %s", "SET" if os.getenv("AWS_SESSION_TOKEN") else "NOT SET")

    def test_sts_caller_identity(self):
        """Verify credentials are valid by calling STS GetCallerIdentity."""
        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            region_name=REGION,
        )
        sts = session.client("sts")
        identity = sts.get_caller_identity()

        assert "Account" in identity, "Failed to get caller identity"
        logger.info("Account:  %s", identity["Account"])
        logger.info("ARN:      %s", identity["Arn"])
        logger.info("User ID:  %s", identity["UserId"])

    def test_bucket_exists(self, s3_client):
        """Verify the target S3 bucket is accessible."""
        response = s3_client.head_bucket(Bucket=BUCKET)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        assert status == 200, f"Bucket HEAD returned status {status}"
        logger.info("Bucket '%s' is accessible (HTTP %s)", BUCKET, status)

    def test_list_objects_under_prefix(self, s3_client):
        """List JSON files under the target prefix."""
        response = s3_client.list_objects_v2(
            Bucket=BUCKET, Prefix=PREFIX, MaxKeys=10
        )
        contents = response.get("Contents", [])
        assert len(contents) > 0, (
            f"No objects found under prefix '{PREFIX}' in bucket '{BUCKET}'"
        )
        logger.info("Found %d file(s) under '%s':", len(contents), PREFIX)
        for obj in contents[:5]:
            logger.info("  - %s  (%d bytes)", obj["Key"], obj["Size"])
        if len(contents) > 5:
            logger.info("  ... and %d more", len(contents) - 5)

    def test_read_latest_json_file(self, s3_client, latest_file_key):
        """Fetch and parse the latest aircraft JSON file from S3."""
        response = s3_client.get_object(Bucket=BUCKET, Key=latest_file_key)
        body = response["Body"].read().decode("utf-8")
        data = json.loads(body)

        assert isinstance(data, (dict, list)), "JSON content is not a dict or list"
        logger.info("File: %s", latest_file_key)
        logger.info("Size: %d bytes", len(body))
        logger.info("Type: %s", type(data).__name__)

        # Handle both list and dict formats
        if isinstance(data, list):
            logger.info("Records: %d", len(data))
            for i, record in enumerate(data):
                logger.info("Record[%d] keys: %s", i, list(record.keys()))
                logger.info("Record[%d] data:\n%s", i, json.dumps(record, indent=4))
        else:
            logger.info("Top-level keys: %s", list(data.keys()))
            logger.info("Content:\n%s", json.dumps(data, indent=4))
