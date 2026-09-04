"""Step definitions for CDM derived-event publisher — Lambda, CloudWatch, S3 (UAT).

Maps acceptance criteria from the [FlightStatus] CDM event publisher story:
  - Lambda reads derived-store S3 prefixes and publishes toward Kafka/CEP
  - Node.js runtime, Dynatrace tags, log retention (non-prod 15 days)
  - S3: CDM-FLIGHT-ODH / CDM-AIRCRAFT-ODH / CDM-FLIGHTGROUNDTIME-ODH sources
  - S3: CDM/processed/CDM-*-ODH-UAT processed paths

Environment
-----------
CDM_DERIVED_EVENT_PUBLISHER_LAMBDA  Override Lambda name (default: ac-odh-CDM-Event-Processor-uatca1)
CDM_AWS_PROFILE                     Override profile (default: ODH_UAT for Lambda/Logs/S3)
CDM_DISABLE_SSO_REFRESH             Set to 1/true to skip automatic ``aws sso login`` on expiry
CDM_DLQ_WAIT_SECONDS               Max seconds to poll DLQ for the test message (default: 300)
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from pytest_bdd import given, when, then, scenarios, parsers

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings
from aws_testkit.credential_refresh import call_with_credential_refresh

scenarios("cdm_derived_event_publisher.feature")

logger = logging.getLogger(__name__)

_DEFAULT_LAMBDA_NAME = "ac-odh-CDM-Event-Processor-uatca1"
_DEFAULT_CDM_PROFILE = "ODH_UAT"

_NODE_RUNTIME_RE = re.compile(r"^nodejs(\d+)\.x$", re.IGNORECASE)


def _cdm_lambda_name() -> str:
    return os.getenv("CDM_DERIVED_EVENT_PUBLISHER_LAMBDA", _DEFAULT_LAMBDA_NAME).strip()


def _cdm_aws_profile() -> str:
    """Profile for CDM suite: CDM_AWS_PROFILE, else AWS_PROFILE, else ODH_UAT."""
    return (
        os.getenv("CDM_AWS_PROFILE")
        or os.getenv("AWS_PROFILE")
        or _DEFAULT_CDM_PROFILE
    ).strip()


def _lambda_boto_client(service: str, region: str):
    """Build boto3 client using the CDM profile (default ODH_UAT)."""
    profile = _cdm_aws_profile()
    kwargs: dict[str, Any] = {"service_name": service, "region_name": region}
    session = boto3.Session(profile_name=profile)
    return session.client(**kwargs)


@pytest.fixture()
def cdm_state() -> dict:
    return {}


@pytest.fixture()
def cdm_s3_client() -> S3Client:
    return S3Client(Settings(aws_profile=_cdm_aws_profile()))


# ---------------------------------------------------------------------------
# GIVEN
# ---------------------------------------------------------------------------


@given(parsers.parse('the CDM derived event publisher Lambda in region "{region}"'))
def _given_cdm_lambda(cdm_state: dict, region: str) -> None:
    name = _cdm_lambda_name()
    cdm_state["lambda_function"] = name
    cdm_state["lambda_region"] = region
    cdm_state["log_group"] = f"/aws/lambda/{name}"
    logger.info("CDM Lambda: %s  Region: %s", name, region)


@given(parsers.parse('the CloudWatch log group for the CDM Lambda in region "{region}"'))
def _given_cdm_log_group(cdm_state: dict, region: str) -> None:
    assert cdm_state.get("log_group"), "Set CDM Lambda first (log group path)"
    cdm_state["cw_region"] = region


@given(parsers.parse('the CDM S3 bucket "{bucket}" in region "{region}"'))
def _given_cdm_bucket(cdm_state: dict, bucket: str, region: str) -> None:
    cdm_state["s3_bucket"] = bucket
    cdm_state["s3_region"] = region
    logger.info("CDM S3 bucket: %s  Region: %s", bucket, region)


@given(parsers.parse('the CDM S3 key prefix "{prefix}"'))
def _given_cdm_prefix(cdm_state: dict, prefix: str) -> None:
    cdm_state["s3_prefix"] = prefix
    logger.info("CDM S3 prefix: %s", prefix)


# ---------------------------------------------------------------------------
# WHEN — Lambda
# ---------------------------------------------------------------------------


@when("I retrieve the CDM Lambda function configuration")
def _when_get_lambda_config(cdm_state: dict) -> None:
    region = cdm_state["lambda_region"]
    name = cdm_state["lambda_function"]
    profile = _cdm_aws_profile()

    def _get_function() -> dict:
        client = _lambda_boto_client("lambda", region)
        return client.get_function(FunctionName=name)

    try:
        resp = call_with_credential_refresh(profile, _get_function)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        pytest.fail(
            f"Cannot load Lambda '{name}' in {region}: {code} {exc}. "
            f"Set CDM_DERIVED_EVENT_PUBLISHER_LAMBDA to the deployed function name."
        )
    cdm_state["lambda_config"] = resp.get("Configuration", {})
    cdm_state["lambda_arn"] = resp.get("Configuration", {}).get("FunctionArn", "")
    cdm_state["lambda_get_function_response"] = resp
    logger.info("Retrieved configuration for Lambda '%s'", name)


@when("I list tags for the CDM Lambda function")
def _when_list_lambda_tags(cdm_state: dict) -> None:
    arn = cdm_state.get("lambda_arn")
    assert arn, "Retrieve CDM Lambda configuration first"
    region = cdm_state["lambda_region"]
    profile = _cdm_aws_profile()

    def _list_tags() -> dict:
        client = _lambda_boto_client("lambda", region)
        return client.list_tags(Resource=arn)

    resp = call_with_credential_refresh(profile, _list_tags)
    cdm_state["lambda_tags"] = resp.get("Tags", {})
    logger.info("Lambda tags count: %d", len(cdm_state["lambda_tags"]))


@when("I describe the CDM CloudWatch log group")
def _when_describe_log_group(cdm_state: dict) -> None:
    log_group = cdm_state["log_group"]
    region = cdm_state.get("cw_region") or cdm_state["lambda_region"]
    profile = _cdm_aws_profile()

    def _describe() -> dict:
        client = _lambda_boto_client("logs", region)
        return client.describe_log_groups(logGroupNamePrefix=log_group, limit=50)

    resp = call_with_credential_refresh(profile, _describe)
    groups = [g for g in resp.get("logGroups", []) if g.get("logGroupName") == log_group]
    assert len(groups) == 1, (
        f"Expected exactly one log group '{log_group}', found {len(groups)}"
    )
    cdm_state["cw_log_group_detail"] = groups[0]
    logger.info(
        "Log group retentionInDays: %s",
        groups[0].get("retentionInDays"),
    )


@when("I retrieve event source mappings for the CDM Lambda")
def _when_list_event_source_mappings(cdm_state: dict) -> None:
    region = cdm_state["lambda_region"]
    name = cdm_state["lambda_function"]
    profile = _cdm_aws_profile()

    def _list() -> list[dict[str, Any]]:
        client = _lambda_boto_client("lambda", region)
        mappings: list[dict[str, Any]] = []
        paginator = client.get_paginator("list_event_source_mappings")
        for page in paginator.paginate(FunctionName=name):
            mappings.extend(page.get("EventSourceMappings", []))
        return mappings

    cdm_state["lambda_event_source_mappings"] = call_with_credential_refresh(
        profile, _list
    )
    logger.info(
        "Found %d event source mapping(s) for Lambda '%s'",
        len(cdm_state["lambda_event_source_mappings"]),
        name,
    )


@given(parsers.parse('the CDM SQS queue "{queue_name}" in region "{region}"'))
def _given_cdm_sqs_queue(cdm_state: dict, queue_name: str, region: str) -> None:
    cdm_state["cdm_sqs_queue_name"] = queue_name
    cdm_state["cdm_sqs_region"] = region
    logger.info("CDM SQS queue: %s  Region: %s", queue_name, region)


@when("I retrieve RedrivePolicy for the CDM SQS queue")
def _when_get_sqs_redrive(cdm_state: dict) -> None:
    queue_name = cdm_state["cdm_sqs_queue_name"]
    region = cdm_state["cdm_sqs_region"]
    profile = _cdm_aws_profile()

    def _get_attrs() -> dict[str, str]:
        client = boto3.Session(profile_name=profile).client(
            "sqs", region_name=region
        )
        url = client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        return client.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["QueueArn", "RedrivePolicy"],
        )["Attributes"]

    cdm_state["cdm_sqs_queue_attributes"] = call_with_credential_refresh(
        profile, _get_attrs
    )
    rp = cdm_state["cdm_sqs_queue_attributes"].get("RedrivePolicy")
    logger.info(
        "Queue '%s' RedrivePolicy present: %s",
        queue_name,
        bool(rp),
    )


@given(parsers.parse('the CDM SQS DLQ "{queue_name}" in region "{region}"'))
def _given_cdm_sqs_dlq(cdm_state: dict, queue_name: str, region: str) -> None:
    cdm_state["cdm_sqs_dlq_name"] = queue_name
    cdm_state["cdm_sqs_dlq_region"] = region
    logger.info("CDM SQS DLQ: %s  Region: %s", queue_name, region)


@when("I send a CDM QA test message that should fail Lambda processing")
def _when_send_dlq_test_message(cdm_state: dict) -> None:
    """Send a non-JSON body so typical handlers fail; correlation token is in the body."""
    region = cdm_state["cdm_sqs_region"]
    queue_name = cdm_state["cdm_sqs_queue_name"]
    profile = _cdm_aws_profile()
    corr = f"cdm-qa-dlq-{uuid.uuid4().hex}"
    cdm_state["cdm_sqs_test_correlation"] = corr
    body = f"cdm_qa_dlq_invalid_payload:{corr}"

    def _send() -> dict[str, Any]:
        client = boto3.Session(profile_name=profile).client("sqs", region_name=region)
        url = client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        return client.send_message(QueueUrl=url, MessageBody=body)

    try:
        call_with_credential_refresh(profile, _send)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "AccessDenied":
            pytest.skip(
                "sqs:SendMessage is not allowed on the primary queue for this role — "
                "grant sqs:SendMessage (and DLQ ReceiveMessage/DeleteMessage) for DLQ E2E, "
                "or rely on the RedrivePolicy-only scenario."
            )
        raise
    logger.info(
        "Sent DLQ test message (non-JSON body) correlation=%s",
        corr,
    )


@when("I wait for that test message to appear in the CDM DLQ")
def _when_wait_for_message_in_dlq(cdm_state: dict) -> None:
    corr = cdm_state["cdm_sqs_test_correlation"]
    region = cdm_state["cdm_sqs_dlq_region"]
    dlq_name = cdm_state["cdm_sqs_dlq_name"]
    profile = _cdm_aws_profile()
    wait_sec = float(os.getenv("CDM_DLQ_WAIT_SECONDS", "300"))
    deadline = time.monotonic() + wait_sec

    def _dlq_url() -> str:
        client = boto3.Session(profile_name=profile).client("sqs", region_name=region)
        return client.get_queue_url(QueueName=dlq_name)["QueueUrl"]

    try:
        dlq_url = call_with_credential_refresh(profile, _dlq_url)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "AccessDenied":
            pytest.skip(
                "sqs:GetQueueUrl denied on DLQ — grant sqs:ReceiveMessage on the DLQ for E2E."
            )
        raise
    cdm_state["cdm_dlq_url"] = dlq_url

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wait = min(20, max(1, int(remaining)))

        def _recv() -> dict[str, Any]:
            client = boto3.Session(profile_name=profile).client("sqs", region_name=region)
            return client.receive_message(
                QueueUrl=dlq_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=wait,
                AttributeNames=["All"],
            )

        try:
            resp = call_with_credential_refresh(profile, _recv)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "AccessDenied":
                pytest.skip(
                    "sqs:ReceiveMessage denied on DLQ — grant IAM for DLQ E2E."
                )
            raise
        for msg in resp.get("Messages", []):
            body = msg.get("Body", "")
            if corr in body:
                cdm_state["cdm_dlq_received_message"] = msg
                logger.info("Test message visible in DLQ (matched correlation)")
                return
        time.sleep(2)

    pytest.fail(
        f"Test message with correlation {corr!r} did not appear in DLQ '{dlq_name}' "
        f"within {wait_sec:g}s. If Lambda treats this payload as success, pick another "
        "failure shape or increase CDM_DLQ_WAIT_SECONDS."
    )


# ---------------------------------------------------------------------------
# WHEN — S3
# ---------------------------------------------------------------------------


def _run_cdm_s3_list_objects(cdm_state: dict) -> None:
    bucket = cdm_state["s3_bucket"]
    prefix = cdm_state["s3_prefix"]
    profile = _cdm_aws_profile()

    def _list() -> dict:
        client = S3Client(Settings(aws_profile=profile))._client
        return client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)

    try:
        resp = call_with_credential_refresh(profile, _list)
    except ClientError as exc:
        cdm_state["s3_list_error"] = str(exc)
        raise
    cdm_state["s3_list_response"] = resp
    cdm_state["s3_list_error"] = None
    keys = [o["Key"] for o in resp.get("Contents", [])]
    logger.info("list_objects_v2 OK — sample keys: %s", keys[:5])


@when("I list objects under the CDM S3 prefix")
def _when_list_objects_cdm(cdm_state: dict) -> None:
    _run_cdm_s3_list_objects(cdm_state)


@when(parsers.parse('I list objects under the CDM S3 prefix "{prefix}"'))
def _when_list_objects_cdm_with_prefix(cdm_state: dict, prefix: str) -> None:
    cdm_state["s3_prefix"] = prefix
    logger.info("CDM S3 prefix: %s", prefix)
    _run_cdm_s3_list_objects(cdm_state)


def _run_cdm_s3_list_json(cdm_state: dict) -> None:
    bucket = cdm_state["s3_bucket"]
    prefix = cdm_state["s3_prefix"]
    profile = _cdm_aws_profile()

    def _list() -> dict:
        client = S3Client(Settings(aws_profile=profile))._client
        return client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)

    resp = call_with_credential_refresh(profile, _list)
    files = [
        obj for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]
    cdm_state["json_files"] = files
    cdm_state["prefix_has_more"] = resp.get("IsTruncated", False)
    logger.info(
        "Found %d JSON file(s) under '%s'",
        len(files), prefix,
    )


@when("I list JSON files under the CDM S3 prefix")
def _when_list_json_cdm(cdm_state: dict) -> None:
    _run_cdm_s3_list_json(cdm_state)


@when(parsers.parse('I list JSON files under the CDM S3 prefix "{prefix}"'))
def _when_list_json_cdm_with_prefix(cdm_state: dict, prefix: str) -> None:
    cdm_state["s3_prefix"] = prefix
    logger.info("CDM S3 prefix: %s", prefix)
    _run_cdm_s3_list_json(cdm_state)


# ---------------------------------------------------------------------------
# THEN — Lambda
# ---------------------------------------------------------------------------


@then("the CDM Lambda should exist")
def _then_lambda_exists(cdm_state: dict) -> None:
    cfg = cdm_state.get("lambda_config")
    assert cfg is not None and cfg.get("FunctionName"), "Lambda configuration missing"
    logger.info("PASS — Lambda exists: %s", cfg.get("FunctionName"))


def _cdm_lambda_env_vars(cdm_state: dict) -> dict[str, str]:
    resp = cdm_state.get("lambda_get_function_response", {})
    raw = (
        resp.get("Configuration", {})
        .get("Environment", {})
        .get("Variables", {})
    )
    return dict(raw) if raw else {}


@then(parsers.parse('the CDM Lambda environment variable "{key}" should equal "{value}"'))
def _then_env_var_equals(cdm_state: dict, key: str, value: str) -> None:
    env = _cdm_lambda_env_vars(cdm_state)
    actual = env.get(key)
    assert actual == value, (
        f"Environment variable {key!r}: expected {value!r}, got {actual!r}. "
        f"Keys present: {sorted(env.keys())}"
    )
    logger.info("PASS — %s == %s", key, value)


@then("the CDM Lambda runtime should be Node.js 24.x")
def _then_node_runtime_24(cdm_state: dict) -> None:
    cfg = cdm_state["lambda_config"]
    runtime = (cfg.get("Runtime") or "").strip()
    assert runtime == "nodejs24.x", (
        f"Runtime expected 'nodejs24.x', got {runtime!r}"
    )
    logger.info("PASS — Runtime %s", runtime)


@then("the CDM Lambda runtime should be Node.js 20 or newer")
def _then_node_runtime(cdm_state: dict) -> None:
    cfg = cdm_state["lambda_config"]
    runtime = cfg.get("Runtime") or ""
    m = _NODE_RUNTIME_RE.match(runtime.strip())
    assert m, (
        f"Runtime '{runtime}' is not a recognized Node.js managed runtime (expected nodejsNN.x)"
    )
    major = int(m.group(1))
    assert major >= 20, (
        f"Runtime '{runtime}' — Node major version {major} < 20; "
        "use Node.js 20 or newer per story."
    )
    logger.info("PASS — Runtime %s (Node %d+)", runtime, major)


@then("the CDM Lambda configuration details should be printed")
def _then_print_lambda_config(cdm_state: dict) -> None:
    cfg = cdm_state.get("lambda_config", {})
    interesting = {
        "FunctionName": cfg.get("FunctionName"),
        "Runtime": cfg.get("Runtime"),
        "Handler": cfg.get("Handler"),
        "MemorySize": cfg.get("MemorySize"),
        "Timeout": cfg.get("Timeout"),
        "LastModified": cfg.get("LastModified"),
    }
    logger.info("Lambda configuration:\n%s", json.dumps(interesting, indent=2, default=str))
    logger.info("PASS — Configuration printed")


@then("the CDM Lambda environment variables should be printed")
def _then_print_lambda_env(cdm_state: dict) -> None:
    resp = cdm_state.get("lambda_get_function_response", {})
    env = (
        resp.get("Configuration", {})
        .get("Environment", {})
        .get("Variables", {})
    )
    logger.info("Lambda environment variables:\n%s", json.dumps(env, indent=2, sort_keys=True))
    logger.info(
        "Note — Non-prod log levels may include DEBUG/INFO; prod should restrict to ERROR/FATAL."
    )
    logger.info("PASS — Environment printed")


@then("the CDM Lambda should have Dynatrace-related tags")
def _then_dynatrace_tags(cdm_state: dict) -> None:
    tags = cdm_state.get("lambda_tags")
    assert tags is not None, "List Lambda tags first"
    if not tags:
        pytest.fail("Lambda has no resource tags — expected Dynatrace-related tags")

    extra_keys = os.getenv("CDM_DYNATRACE_TAG_KEYS", "")
    required: list[str] = [k.strip() for k in extra_keys.split(",") if k.strip()]

    matched: list[str] = []
    for key in tags:
        lk = key.lower()
        if "dynatrace" in lk or lk.startswith("dt.") or lk.startswith("dt-"):
            matched.append(key)
        # Platform observability (Dynatrace / shared monitoring tags)
        if lk == "observability":
            matched.append(key)
        if key in required:
            matched.append(key)

    assert matched, (
        f"No Dynatrace-style tags found. Tags present: {list(tags.keys())}. "
        "Set CDM_DYNATRACE_TAG_KEYS=key1,key2 if your keys differ."
    )
    logger.info("PASS — Dynatrace-related tag keys: %s", matched)


@then("the CDM Lambda tags should be printed")
def _then_print_tags(cdm_state: dict) -> None:
    tags = cdm_state.get("lambda_tags", {})
    logger.info("Lambda tags:\n%s", json.dumps(tags, indent=2, sort_keys=True))
    logger.info("PASS — Tags printed")


@then(
    parsers.parse(
        'the CDM Lambda should have an enabled SQS event source for queue "{queue_name}"'
    )
)
def _then_enabled_sqs_event_source(cdm_state: dict, queue_name: str) -> None:
    mappings = cdm_state.get("lambda_event_source_mappings")
    assert mappings is not None, "Run 'I retrieve event source mappings for the CDM Lambda' first"

    for m in mappings:
        arn = m.get("EventSourceArn") or ""
        if not arn.startswith("arn:aws:sqs:"):
            continue
        if arn.rsplit(":", 1)[-1] != queue_name:
            continue
        state = (m.get("State") or "").strip()
        assert state == "Enabled", (
            f"SQS event source for '{queue_name}' exists but State is '{state}', "
            "expected 'Enabled'"
        )
        logger.info(
            "PASS — Enabled SQS mapping UUID=%s State=%s",
            m.get("UUID"),
            state,
        )
        return

    arns = [x.get("EventSourceArn") for x in mappings]
    pytest.fail(
        f"No SQS event source for queue '{queue_name}'. EventSourceArns: {arns}"
    )


@then(parsers.parse('the CDM queue dead letter target should be "{dlq_name}"'))
def _then_sqs_dlq_target(cdm_state: dict, dlq_name: str) -> None:
    attrs = cdm_state.get("cdm_sqs_queue_attributes")
    assert attrs, "Run 'I retrieve RedrivePolicy for the CDM SQS queue' first"
    rp_raw = attrs.get("RedrivePolicy")
    assert rp_raw, (
        "Queue has no RedrivePolicy attribute — DLQ redrive may not be configured"
    )
    rp = json.loads(rp_raw)
    target_arn = rp.get("deadLetterTargetArn", "")
    assert dlq_name in target_arn, (
        f"deadLetterTargetArn does not reference '{dlq_name}': {target_arn!r}"
    )
    mrc = rp.get("maxReceiveCount")
    logger.info(
        "PASS — RedrivePolicy targets DLQ %s (maxReceiveCount=%s)",
        dlq_name,
        mrc,
    )


@then("the CDM DLQ message body should contain the test correlation id")
def _then_dlq_body_contains_correlation(cdm_state: dict) -> None:
    corr = cdm_state.get("cdm_sqs_test_correlation")
    msg = cdm_state.get("cdm_dlq_received_message")
    assert corr, "No test correlation id — send step did not run"
    assert msg, "No DLQ message captured — wait step did not find the test message"
    body = msg.get("Body", "")
    assert corr in body, (
        f"DLQ message body does not contain correlation {corr!r}: {body[:500]!r}"
    )
    logger.info("PASS — DLQ body contains correlation id")


@then("I delete the CDM test message from the DLQ")
def _then_delete_cdm_dlq_test_message(cdm_state: dict) -> None:
    msg = cdm_state.get("cdm_dlq_received_message")
    dlq_url = cdm_state.get("cdm_dlq_url")
    region = cdm_state["cdm_sqs_dlq_region"]
    profile = _cdm_aws_profile()
    assert msg and dlq_url and msg.get("ReceiptHandle"), (
        "Nothing to delete — run wait and correlation steps first"
    )

    def _delete() -> None:
        client = boto3.Session(profile_name=profile).client("sqs", region_name=region)
        client.delete_message(QueueUrl=dlq_url, ReceiptHandle=msg["ReceiptHandle"])

    try:
        call_with_credential_refresh(profile, _delete)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "AccessDenied":
            logger.warning(
                "sqs:DeleteMessage denied — test message may remain in DLQ; grant delete permission."
            )
            return
        raise
    logger.info("PASS — Deleted test message from DLQ")


# ---------------------------------------------------------------------------
# THEN — CloudWatch
# ---------------------------------------------------------------------------


@then("the CDM log group should exist")
def _then_log_group_exists(cdm_state: dict) -> None:
    log_group = cdm_state["log_group"]
    region = cdm_state.get("cw_region") or cdm_state["lambda_region"]
    profile = _cdm_aws_profile()

    def _describe() -> dict:
        client = _lambda_boto_client("logs", region)
        return client.describe_log_groups(logGroupNamePrefix=log_group, limit=50)

    resp = call_with_credential_refresh(profile, _describe)
    names = [g["logGroupName"] for g in resp.get("logGroups", [])]
    assert log_group in names, (
        f"Log group '{log_group}' not found. Close matches: {names[:10]}"
    )
    logger.info("PASS — Log group exists: %s", log_group)


@then(parsers.parse("the CDM log group retention should be {days:d} days"))
def _then_retention_days(cdm_state: dict, days: int) -> None:
    detail = cdm_state.get("cw_log_group_detail")
    assert detail, "Describe the CDM CloudWatch log group first"
    actual = detail.get("retentionInDays")
    assert actual == days, (
        f"retentionInDays is {actual}, expected {days} for non-prod UAT policy"
    )
    logger.info("PASS — Log retention = %d days", days)


@then(
    parsers.parse(
        "the CDM log group retention should be between {low:d} and {high:d} days inclusive"
    )
)
def _then_retention_range(cdm_state: dict, low: int, high: int) -> None:
    detail = cdm_state.get("cw_log_group_detail")
    assert detail, "Describe the CDM CloudWatch log group first"
    actual = detail.get("retentionInDays")
    assert actual is not None, "Log group has no retentionInDays (never expires)"
    assert low <= actual <= high, (
        f"retentionInDays is {actual}, expected between {low} and {high} inclusive"
    )
    logger.info("PASS — Log retention = %d days (within %d–%d)", actual, low, high)


# ---------------------------------------------------------------------------
# THEN — S3
# ---------------------------------------------------------------------------


@then("the CDM S3 list operation should succeed")
def _then_s3_list_ok(cdm_state: dict) -> None:
    assert cdm_state.get("s3_list_error") is None
    assert "s3_list_response" in cdm_state
    logger.info("PASS — S3 prefix is listable")


@then(parsers.parse("at least {n:d} JSON file should exist under the CDM prefix"))
def _then_at_least_n_json(cdm_state: dict, n: int) -> None:
    files = cdm_state.get("json_files", [])
    assert len(files) >= n, (
        f"Expected at least {n} JSON file(s), found {len(files)} "
        f"under '{cdm_state.get('s3_prefix')}'"
    )
    logger.info("PASS — %d JSON file(s) under prefix", len(files))
