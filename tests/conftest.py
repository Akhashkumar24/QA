"""Root conftest — shared fixtures available to all test modules."""

from __future__ import annotations

import os
import shutil
from datetime import datetime

import pytest
from moto import mock_aws

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings


# ---------------------------------------------------------------------------
# Register the @skip marker so pytest-bdd scenarios tagged @skip are skipped
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(items):
    for item in items:
        if "skip" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="Marked @skip in feature file"))


# ---------------------------------------------------------------------------
# Report naming hook: Test_Report_YYYY-MM-DD_HH-MM-SS_Passed/Failed.html
# ---------------------------------------------------------------------------

def pytest_sessionfinish(session, exitstatus):
    """Rename the HTML report after all tests complete.

    Final name format: Test_Report_2026-02-12_22-30-45_Passed.html
    """
    # Find the generated report (written to a temp name via --html flag)
    config = session.config
    html_path = config.getoption("htmlpath", default=None)
    if html_path is None or not os.path.exists(html_path):
        return

    # Determine pass/fail status
    status = "Passed" if exitstatus == 0 else "Failed"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_name = f"Test_Report_{timestamp}_{status}.html"

    report_dir = os.path.dirname(html_path) or "."
    new_path = os.path.join(report_dir, report_name)

    shutil.copy2(html_path, new_path)
    # Keep the original as well so --html flag doesn't error on next run
    print(f"\n>> Report saved: {new_path}")


@pytest.fixture()
def moto_s3():
    """Yield an S3Client backed by moto's in-memory AWS mock.

    Every test that uses this fixture gets an isolated, empty S3 service.
    No Docker or network access required.
    """
    with mock_aws():
        settings = Settings(
            aws_region="us-east-1",
            aws_endpoint_url=None,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        yield S3Client(settings)


@pytest.fixture()
def real_s3_client():
    """S3Client using the default AWS CLI profile (~/.aws/credentials).

    Used by s3_validation.feature tests to read from the real S3 bucket.
    No explicit credentials — boto3 resolves from the credential chain:
      1. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
      2. ~/.aws/credentials (default or named profile via AWS_PROFILE)
      3. IAM instance role (if on EC2/Lambda)
    """
    settings = Settings()
    return S3Client(settings)
