"""Step definitions for Aircraft Event validation — Lambda, S3 output, and CloudWatch Logs.

Merged from:
  - test_s3_validation_steps.py   (S3 aircraft JSON event validation)
  - test_cloudwatch_logs_steps.py (Trax Aircraft Extractor Lambda + CloudWatch)

Credentials are resolved from environment variables or ~/.aws/credentials.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

import boto3
import pytest
from pytest_bdd import given, when, then, scenarios, parsers

from aws_testkit.aws_clients import S3Client, CloudWatchLogsClient
from aws_testkit.config import Settings

scenarios("aircraft_event_validation.feature")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FILENAME_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.json$"

REQUIRED_RECORD_FIELDS = [
    "FleetIdentificationNumber",
    "SerialNumber",
    "Manufacturer",
    "LastAcRegistration",
    "EventType",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def validation_state() -> dict:
    return {}


@pytest.fixture()
def cw_state() -> dict:
    return {}


@pytest.fixture()
def real_s3_client() -> S3Client:
    settings = Settings()
    return S3Client(settings)


@pytest.fixture()
def cw_logs_client() -> CloudWatchLogsClient:
    settings = Settings()
    return CloudWatchLogsClient(settings)


@pytest.fixture()
def lambda_client():
    settings = Settings()
    kwargs: dict = {
        "service_name": "lambda",
        "region_name": settings.aws_region,
    }
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key:
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_session_token:
        kwargs["aws_session_token"] = settings.aws_session_token
    if settings.aws_profile:
        session = boto3.Session(profile_name=settings.aws_profile)
        return session.client(**kwargs)
    return boto3.client(**kwargs)


# ---------------------------------------------------------------------------
# Helper — parse raw content into a list of records
# ---------------------------------------------------------------------------

def _parse_records(validation_state: dict) -> list[dict]:
    if "parsed_records" in validation_state:
        return validation_state["parsed_records"]

    raw = validation_state["raw_content"]
    data = json.loads(raw.decode("utf-8"))
    records = data if isinstance(data, list) else [data]
    validation_state["parsed_records"] = records
    return records


# =========================================================================
# GIVEN steps — Lambda / CloudWatch
# =========================================================================

@given(
    parsers.parse('the Lambda function "{function_name}" in region "{region}"'),
)
def _given_lambda_function(cw_state: dict, function_name: str, region: str) -> None:
    cw_state["lambda_function"] = function_name
    cw_state["lambda_region"] = region
    logger.info("Lambda function: %s  Region: %s", function_name, region)


@given(
    parsers.parse('the CloudWatch log group "{log_group}" in region "{region}"'),
    target_fixture="cw_state",
)
def _given_cw_log_group(cw_state: dict, log_group: str, region: str) -> dict:
    cw_state["log_group"] = log_group
    cw_state["region"] = region
    logger.info("Log group: %s  Region: %s", log_group, region)
    return cw_state


# =========================================================================
# GIVEN steps — S3
# =========================================================================

@given(
    parsers.parse('the S3 bucket "{bucket}" in region "{region}"'),
    target_fixture="validation_state",
)
def _given_s3_bucket(validation_state: dict, bucket: str, region: str) -> dict:
    validation_state["bucket"] = bucket
    validation_state["region"] = region
    logger.info("Bucket: %s  Region: %s", bucket, region)
    return validation_state


@given(parsers.parse('the key prefix "{prefix}"'))
def _given_key_prefix(validation_state: dict, prefix: str) -> None:
    validation_state["prefix"] = prefix
    logger.info("Prefix: %s", prefix)


@given("the latest aircraft JSON file in the bucket")
def _given_latest_file(validation_state: dict, real_s3_client: S3Client) -> None:
    bucket = validation_state["bucket"]
    prefix = validation_state["prefix"]

    paginator = real_s3_client._client.get_paginator("list_objects_v2")
    all_objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                all_objects.append(obj)

    assert len(all_objects) > 0, f"No JSON files found under '{prefix}'"

    latest = max(all_objects, key=lambda o: o["LastModified"])
    filename = latest["Key"].split("/")[-1]

    validation_state["filename"] = filename
    validation_state["full_key"] = latest["Key"]
    logger.info(
        "Latest file: %s (modified: %s, %d bytes)",
        latest["Key"], latest["LastModified"], latest["Size"],
    )


@given(parsers.parse('the specific file "{filename}" in the bucket'))
def _given_specific_file(validation_state: dict, real_s3_client: S3Client, filename: str) -> None:
    bucket = validation_state["bucket"]
    prefix = validation_state["prefix"]
    full_key = f"{prefix}{filename}"

    validation_state["filename"] = filename
    validation_state["full_key"] = full_key
    logger.info("Target file: %s", full_key)


# =========================================================================
# WHEN steps — Lambda
# =========================================================================

@when("I invoke the Lambda with a test event")
def _when_invoke_lambda(cw_state: dict, lambda_client) -> None:
    function_name = cw_state["lambda_function"]
    test_payload = json.dumps({
        "ruleName": "TraxAircraftExtractorSchedule-uatca1",
        "schedule": "rate(6 hours)",
        "enabled": True,
        "target": {
            "arn": f"arn:aws:lambda:ca-central-1:123456789012:function:{function_name}",
            "input": "{}",
            "description": "Triggers Trax aircraft extraction every 6 hours",
        },
    })

    logger.info("Invoking Lambda '%s' with test event...", function_name)

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=test_payload.encode("utf-8"),
        )

        status_code = response.get("StatusCode", 0)
        function_error = response.get("FunctionError")
        payload_bytes = response["Payload"].read()
        payload_str = payload_bytes.decode("utf-8")

        cw_state["lambda_status_code"] = status_code
        cw_state["lambda_function_error"] = function_error
        cw_state["lambda_raw_response"] = payload_str

        try:
            parsed = json.loads(payload_str)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            if isinstance(parsed, dict) and "body" in parsed:
                body = parsed["body"]
                if isinstance(body, str):
                    try:
                        parsed_body = json.loads(body)
                        if isinstance(parsed_body, dict):
                            parsed.update(parsed_body)
                    except (json.JSONDecodeError, TypeError):
                        pass
            cw_state["lambda_response"] = parsed
        except (json.JSONDecodeError, TypeError):
            cw_state["lambda_response"] = {"raw": payload_str}

        if function_error:
            cw_state["lambda_execution_status"] = "Failed"
            logger.error(
                "Lambda returned FunctionError: %s — StatusCode: %d",
                function_error, status_code,
            )
        else:
            cw_state["lambda_execution_status"] = "Succeeded"
            logger.info(
                "Lambda invocation succeeded — StatusCode: %d, Payload: %d bytes",
                status_code, len(payload_bytes),
            )

    except Exception as exc:
        cw_state["lambda_execution_status"] = "Failed"
        cw_state["lambda_error"] = str(exc)
        cw_state["lambda_response"] = {}
        logger.error("Lambda invocation failed: %s", exc)


# =========================================================================
# WHEN steps — CloudWatch Logs
# =========================================================================

@when(parsers.parse('I fetch recent logs containing "{search_text}"'))
def _when_fetch_filtered_logs(
    cw_state: dict, cw_logs_client: CloudWatchLogsClient, search_text: str,
) -> None:
    log_group = cw_state["log_group"]
    logger.info("Searching log group '%s' for: %s", log_group, search_text)

    filter_pattern = f'"{search_text}"'
    events = cw_logs_client.filter_log_events(
        log_group, filter_pattern=filter_pattern, limit=50,
    )

    cw_state["matching_events"] = events
    cw_state["search_text"] = search_text
    logger.info("Found %d event(s) matching '%s'", len(events), search_text)


@when("I fetch the latest log events from the most recent stream")
def _when_fetch_latest_stream_events(
    cw_state: dict, cw_logs_client: CloudWatchLogsClient,
) -> None:
    log_group = cw_state["log_group"]

    streams = cw_logs_client.get_recent_log_streams(log_group, limit=1)
    assert len(streams) > 0, f"No log streams found in log group '{log_group}'"

    stream_name = streams[0]["logStreamName"]
    cw_state["latest_stream"] = stream_name
    logger.info("Most recent stream: %s", stream_name)

    events = cw_logs_client.get_log_events(log_group, stream_name, limit=50)
    cw_state["stream_events"] = events
    logger.info("Fetched %d event(s) from stream '%s'", len(events), stream_name)


# =========================================================================
# WHEN steps — S3
# =========================================================================

@when("I fetch the file from S3")
def _when_fetch_file(validation_state: dict, real_s3_client: S3Client) -> None:
    bucket = validation_state["bucket"]
    key = validation_state["full_key"]

    try:
        raw = real_s3_client.get_object(bucket, key)
        validation_state["raw_content"] = raw
        validation_state["file_exists"] = True
        logger.info("Fetched: %s (%d bytes)", key, len(raw))
    except Exception as exc:
        validation_state["raw_content"] = None
        validation_state["file_exists"] = False
        validation_state["fetch_error"] = str(exc)
        logger.error("Fetch failed: %s — %s", key, exc)


@when("I fetch the lifecycle configuration for the bucket")
def _when_fetch_lifecycle(validation_state: dict, real_s3_client: S3Client) -> None:
    bucket = validation_state["bucket"]
    try:
        resp = real_s3_client._client.get_bucket_lifecycle_configuration(Bucket=bucket)
        validation_state["lifecycle_rules"] = resp.get("Rules", [])
        logger.info(
            "Fetched %d lifecycle rule(s) for bucket '%s'",
            len(validation_state["lifecycle_rules"]), bucket,
        )
    except real_s3_client._client.exceptions.ClientError as exc:
        if "NoSuchLifecycleConfiguration" in str(exc):
            validation_state["lifecycle_rules"] = []
            logger.warning("No lifecycle configuration found for bucket '%s'", bucket)
        else:
            raise


# =========================================================================
# THEN steps — Lambda execution
# =========================================================================

@then(parsers.parse('the Lambda execution status should be "{expected_status}"'))
def _then_lambda_status(cw_state: dict, expected_status: str) -> None:
    actual = cw_state.get("lambda_execution_status", "Unknown")
    assert actual == expected_status, (
        f"Lambda execution status is '{actual}', expected '{expected_status}'. "
        f"Error: {cw_state.get('lambda_error', cw_state.get('lambda_function_error', 'N/A'))}"
    )
    logger.info("PASS — Lambda execution status: %s", actual)


@then("the Lambda response details should be printed")
def _then_print_lambda_response(cw_state: dict) -> None:
    response = cw_state.get("lambda_response", {})
    status_code = cw_state.get("lambda_status_code", "N/A")
    function_error = cw_state.get("lambda_function_error")

    logger.info("=" * 80)
    logger.info("LAMBDA INVOCATION RESPONSE")
    logger.info("  Function     : %s", cw_state.get("lambda_function", "N/A"))
    logger.info("  StatusCode   : %s", status_code)
    logger.info("  Status       : %s", cw_state.get("lambda_execution_status", "N/A"))
    if function_error:
        logger.info("  FunctionError: %s", function_error)
    logger.info("-" * 80)

    pretty = json.dumps(response, indent=4, ensure_ascii=False)
    logger.info("  Response body:\n%s", pretty)

    logger.info("=" * 80)
    logger.info("PASS — Lambda response details printed")


@then(parsers.parse('the Lambda response should contain a "{field_name}" field'))
def _then_lambda_response_has_field(cw_state: dict, field_name: str) -> None:
    response = cw_state.get("lambda_response", {})
    assert field_name in response, (
        f"Lambda response missing field '{field_name}'. "
        f"Available: {list(response.keys())}"
    )
    logger.info("PASS — Lambda response contains '%s': %s", field_name, response[field_name])


@then("the Lambda response message should indicate success")
def _then_lambda_message_success(cw_state: dict) -> None:
    response = cw_state.get("lambda_response", {})
    message = response.get("message", "")
    assert message, "Lambda response 'message' is empty"

    success_indicators = ["success", "completed", "finished", "done"]
    message_lower = str(message).lower()
    found = any(indicator in message_lower for indicator in success_indicators)
    assert found, (
        f"Lambda response message does not indicate success: '{message}'. "
        f"Expected one of: {success_indicators}"
    )
    logger.info("PASS — Lambda response message indicates success: '%s'", message)


@then("the total records count should be greater than 0")
def _then_total_records_gt_zero(cw_state: dict) -> None:
    response = cw_state.get("lambda_response", {})
    total = response.get("totalRecords", 0)
    assert isinstance(total, (int, float)) and total > 0, (
        f"totalRecords should be > 0, got: {total}"
    )
    logger.info("PASS — totalRecords: %d", total)


@then("the success count should equal the total records count")
def _then_success_equals_total(cw_state: dict) -> None:
    response = cw_state.get("lambda_response", {})
    total = response.get("totalRecords", 0)
    success = response.get("successCount", 0)

    logger.info("totalRecords: %s, successCount: %s", total, success)

    assert isinstance(success, (int, float)), (
        f"successCount should be a number, got: {type(success).__name__} = {success}"
    )
    assert success == total, (
        f"successCount ({success}) does not match totalRecords ({total}). "
        f"{total - success} record(s) failed."
    )
    logger.info("PASS — successCount (%d) == totalRecords (%d)", success, total)


# =========================================================================
# THEN steps — CloudWatch log group
# =========================================================================

@then("the log group should exist")
def _then_log_group_exists(cw_state: dict, cw_logs_client: CloudWatchLogsClient) -> None:
    log_group = cw_state["log_group"]
    exists = cw_logs_client.log_group_exists(log_group)
    assert exists, f"CloudWatch log group '{log_group}' does not exist or is not accessible"
    logger.info("PASS — Log group '%s' exists and is accessible", log_group)


@then(parsers.parse("at least {count:d} matching log event should be found"))
def _then_at_least_n_matching_events(cw_state: dict, count: int) -> None:
    events = cw_state["matching_events"]
    assert len(events) >= count, (
        f"Expected at least {count} matching log event(s), found {len(events)}. "
        f"Search text: '{cw_state.get('search_text', '')}'"
    )
    logger.info("PASS — Found %d matching event(s) (minimum: %d)", len(events), count)


@then("each matching log event should be printed")
def _then_print_matching_events(cw_state: dict) -> None:
    events = cw_state["matching_events"]
    search_text = cw_state.get("search_text", "")

    logger.info("=" * 80)
    logger.info("MATCHING LOG EVENTS for: '%s'", search_text)
    logger.info("Log group: %s", cw_state["log_group"])
    logger.info("Total matches: %d", len(events))
    logger.info("=" * 80)

    for i, event in enumerate(events, start=1):
        timestamp_ms = event.get("timestamp", 0)
        timestamp_str = datetime.fromtimestamp(
            timestamp_ms / 1000, tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        message = event.get("message", "").strip()

        logger.info("-" * 80)
        logger.info("Event #%d", i)
        logger.info("  Timestamp : %s", timestamp_str)
        logger.info("  Stream    : %s", event.get("logStreamName", "N/A"))

        try:
            parsed = json.loads(message)
            pretty = json.dumps(parsed, indent=4, ensure_ascii=False)
            logger.info("  Message   :\n%s", pretty)
        except (json.JSONDecodeError, TypeError):
            logger.info("  Message   : %s", message)

    logger.info("=" * 80)
    logger.info("PASS — All %d matching event(s) printed successfully", len(events))


@then(parsers.parse("at least {count:d} log event should be returned"))
def _then_at_least_n_stream_events(cw_state: dict, count: int) -> None:
    events = cw_state["stream_events"]
    assert len(events) >= count, (
        f"Expected at least {count} log event(s), found {len(events)}"
    )
    logger.info("PASS — Found %d event(s) (minimum: %d)", len(events), count)


@then("each log event should be printed")
def _then_print_stream_events(cw_state: dict) -> None:
    events = cw_state["stream_events"]

    logger.info("=" * 80)
    logger.info("LATEST LOG EVENTS from stream: %s", cw_state.get("latest_stream", "N/A"))
    logger.info("Log group: %s", cw_state["log_group"])
    logger.info("Total events: %d", len(events))
    logger.info("=" * 80)

    for i, event in enumerate(events, start=1):
        timestamp_ms = event.get("timestamp", 0)
        timestamp_str = datetime.fromtimestamp(
            timestamp_ms / 1000, tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        message = event.get("message", "").strip()

        logger.info("-" * 80)
        logger.info("Event #%d", i)
        logger.info("  Timestamp : %s", timestamp_str)

        try:
            parsed = json.loads(message)
            pretty = json.dumps(parsed, indent=4, ensure_ascii=False)
            logger.info("  Message   :\n%s", pretty)
        except (json.JSONDecodeError, TypeError):
            logger.info("  Message   : %s", message)

    logger.info("=" * 80)
    logger.info("PASS — All %d event(s) printed", len(events))


# =========================================================================
# THEN steps — S3 file validation
# =========================================================================

@then("the file should exist and be readable")
def _then_file_exists(validation_state: dict) -> None:
    assert validation_state["file_exists"] is True, (
        f"File not found: {validation_state['full_key']}. "
        f"Error: {validation_state.get('fetch_error', 'unknown')}"
    )
    logger.info("PASS — File exists and is readable")


@then("the file content should be valid JSON")
def _then_valid_json(validation_state: dict) -> None:
    raw = validation_state["raw_content"]
    assert raw is not None, "No file content to validate"
    try:
        json.loads(raw.decode("utf-8"))
        logger.info("PASS — File content is valid JSON")
    except json.JSONDecodeError as exc:
        pytest.fail(f"File is not valid JSON: {exc}")


@then('the filename should match UUID pattern "{uuid}.json"')
def _then_filename_matches_pattern(validation_state: dict) -> None:
    filename = validation_state["filename"]
    assert re.match(FILENAME_PATTERN, filename), (
        f"Filename '{filename}' does not match expected UUID pattern "
        f"'{{uuid}}.json'"
    )
    logger.info("PASS — Filename '%s' matches expected UUID pattern", filename)


@then("the JSON content should be a non-empty array")
def _then_json_is_non_empty_array(validation_state: dict) -> None:
    records = _parse_records(validation_state)
    assert len(records) > 0, "JSON array is empty"
    logger.info("PASS — JSON is an array with %d record(s)", len(records))


@then(parsers.parse('each record should contain the field "{field_name}"'))
def _then_record_contains_field(validation_state: dict, field_name: str) -> None:
    records = _parse_records(validation_state)
    for i, record in enumerate(records):
        assert field_name in record, (
            f"Record[{i}] missing field '{field_name}'. "
            f"Available: {list(record.keys())}"
        )
    logger.info("PASS — All %d record(s) contain field '%s'", len(records), field_name)


@then(parsers.parse('the field "{field_name}" in each record should be a non-empty string'))
def _then_field_is_non_empty_string(validation_state: dict, field_name: str) -> None:
    records = _parse_records(validation_state)
    for i, record in enumerate(records):
        value = record.get(field_name)
        assert value is not None, f"Record[{i}].{field_name} is null"
        assert isinstance(value, str) and len(value.strip()) > 0, (
            f"Record[{i}].{field_name} should be a non-empty string, got: '{value}'"
        )
    logger.info("PASS — '%s' is non-empty in all %d record(s)", field_name, len(records))


@then(parsers.parse('the field "{field_name}" in each record should be a string or null'))
def _then_field_is_string_or_null(validation_state: dict, field_name: str) -> None:
    records = _parse_records(validation_state)
    for i, record in enumerate(records):
        assert field_name in record, (
            f"Record[{i}] missing field '{field_name}'. "
            f"Available: {list(record.keys())}"
        )
        value = record[field_name]
        assert value is None or isinstance(value, str), (
            f"Record[{i}].{field_name} should be a string or null, "
            f"got: {type(value).__name__} = '{value}'"
        )
    logger.info("PASS — '%s' is string or null in all %d record(s)", field_name, len(records))


@then(parsers.parse('the field "{field_name}" in each record should equal "{expected_value}"'))
def _then_field_equals_in_all(validation_state: dict, field_name: str, expected_value: str) -> None:
    records = _parse_records(validation_state)
    for i, record in enumerate(records):
        actual = record.get(field_name)
        assert str(actual) == expected_value, (
            f"Record[{i}].{field_name} expected '{expected_value}', got '{actual}'"
        )
    logger.info("PASS — '%s' == '%s' in all %d record(s)", field_name, expected_value, len(records))


@then("each record should log all field values")
def _then_log_all_field_values(validation_state: dict) -> None:
    records = _parse_records(validation_state)
    raw = validation_state.get("raw_content", b"")
    full_key = validation_state.get("full_key", "unknown")

    logger.info("File: %s", full_key)
    logger.info("Size: %d bytes", len(raw))
    logger.info("Records: %d", len(records))

    NULLABLE_FIELDS = {"Manufacturer", "LastAcRegistration"}

    for i, record in enumerate(records):
        for field in REQUIRED_RECORD_FIELDS:
            assert field in record, f"Record[{i}] missing required field '{field}'"
            value = record[field]
            if field in NULLABLE_FIELDS:
                assert value is None or isinstance(value, str), (
                    f"Record[{i}].{field} should be a string or null, got: {type(value).__name__}"
                )
            else:
                assert isinstance(value, str) and len(value.strip()) > 0, (
                    f"Record[{i}].{field} should be a non-empty string, got: '{value}'"
                )

    data = json.loads(raw.decode("utf-8"))
    pretty_json = json.dumps(data, indent=4, ensure_ascii=False)
    logger.info("JSON content:\n%s", pretty_json)

    logger.info("PASS — All %d record(s) validated and logged", len(records))


@then(parsers.parse('the field "{field_name}" in record {index:d} should be "{expected_value}"'))
def _then_field_equals_value(
    validation_state: dict, field_name: str, index: int, expected_value: str,
) -> None:
    records = _parse_records(validation_state)
    assert index < len(records), (
        f"Record index {index} out of range — only {len(records)} record(s) available"
    )
    record = records[index]
    assert field_name in record, (
        f"Record[{index}] missing field '{field_name}'. "
        f"Available: {list(record.keys())}"
    )
    actual = record[field_name]
    assert str(actual) == expected_value, (
        f"Record[{index}].{field_name} expected '{expected_value}', got '{actual}'"
    )
    logger.info("PASS — Record[%d].%s == '%s'", index, field_name, expected_value)


# =========================================================================
# THEN steps — S3 lifecycle / retention
# =========================================================================

@then(parsers.parse("a lifecycle rule with expiration of {days:d} days should exist"))
def _then_lifecycle_expiration_days(validation_state: dict, days: int) -> None:
    rules = validation_state["lifecycle_rules"]
    assert len(rules) > 0, (
        f"No lifecycle rules found on bucket '{validation_state['bucket']}'. "
        "Expected at least one rule with an expiration policy."
    )

    matching = [r for r in rules if r.get("Expiration", {}).get("Days") == days]
    assert len(matching) > 0, (
        f"No lifecycle rule with Expiration.Days={days} found. "
        f"Existing rules: {json.dumps(rules, indent=2, default=str)}"
    )
    validation_state["matching_lifecycle_rules"] = matching
    logger.info("PASS — Found %d lifecycle rule(s) with %d-day expiration", len(matching), days)


@then("the lifecycle rule should be enabled")
def _then_lifecycle_rule_enabled(validation_state: dict) -> None:
    rules = validation_state.get("matching_lifecycle_rules", [])
    for rule in rules:
        status = rule.get("Status", "Unknown")
        assert status == "Enabled", (
            f"Lifecycle rule '{rule.get('ID', 'N/A')}' status is '{status}', expected 'Enabled'"
        )
    logger.info("PASS — All matching lifecycle rule(s) are Enabled")


@then("the lifecycle rule details should be printed")
def _then_print_lifecycle_rules(validation_state: dict) -> None:
    rules = validation_state.get("matching_lifecycle_rules", [])
    bucket = validation_state["bucket"]

    logger.info("=" * 80)
    logger.info("LIFECYCLE RULES for bucket: %s", bucket)
    logger.info("Matching rules: %d", len(rules))
    logger.info("=" * 80)

    for i, rule in enumerate(rules, start=1):
        logger.info("-" * 80)
        logger.info("Rule #%d", i)
        logger.info("  ID         : %s", rule.get("ID", "N/A"))
        logger.info("  Status     : %s", rule.get("Status", "N/A"))
        logger.info("  Expiration : %s days", rule.get("Expiration", {}).get("Days", "N/A"))

        filt = rule.get("Filter", {})
        prefix = filt.get("Prefix", filt.get("And", {}).get("Prefix", "(all objects)"))
        logger.info("  Prefix     : %s", prefix if prefix else "(all objects)")

        transitions = rule.get("Transitions", [])
        if transitions:
            for t in transitions:
                logger.info(
                    "  Transition : %s days -> %s",
                    t.get("Days", "N/A"), t.get("StorageClass", "N/A"),
                )

    logger.info("=" * 80)
    logger.info("PASS — Lifecycle rule details printed")
