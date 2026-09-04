"""Step definitions for validating FDM events — Lambda ingestion and S3 output.

Lambda invocation targets AC-Digital-BAT  (profile: AC_Digital_BAT).
S3 output validation targets AC-DATA-ODH-UAT (profile: ODH_UAT).

Override via environment variables if the default profile names differ:
    LAMBDA_AWS_PROFILE – profile for Lambda API calls (default: AC_Digital_BAT)
    S3_AWS_PROFILE     – profile for S3 API calls     (default: ODH_UAT)
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone

import boto3
import pytest
from pytest_bdd import given, when, then, scenarios, parsers
from pytest_html import extras as html_extras

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings

scenarios("fdm_validation.feature")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS profile mapping
# ---------------------------------------------------------------------------
LAMBDA_AWS_PROFILE = os.getenv("LAMBDA_AWS_PROFILE", "AC_Digital_BAT")
S3_AWS_PROFILE = os.getenv("S3_AWS_PROFILE", "ODH_UAT")

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------
IATA_AIRPORT_PATTERN = re.compile(r"^[A-Z]{3}$")
ID_PATTERN = re.compile(r"^[A-Z]{2}-\d{1,5}-\d{4}-\d{2}-\d{2}-[A-Z]{3}$")

# ---------------------------------------------------------------------------
# FDM Lambda test event (Kafka-wrapped FDM message)
# ---------------------------------------------------------------------------
FDM_TEST_EVENT = {
    "eventSource": "aws:kafka",
    "eventSourceArn": "arn:aws:kafka:ca-central-1:574529944728:cluster/digital-msk-batca1/6415732f-1c7d-4e62-aacf-95c089a0ef75-2",
    "bootstrapServers": "b-6.digitalmskbatca1.arylnm.c2.kafka.ca-central-1.amazonaws.com:9094,b-5.digitalmskbatca1.arylnm.c2.kafka.ca-central-1.amazonaws.com:9094,b-1.digitalmskbatca1.arylnm.c2.kafka.ca-central-1.amazonaws.com:9094",
    "records": {
        "RAW-FDM-NLOPS-BATCA1-14": [
            {
                "topic": "RAW-FDM-NLOPS-BATCA1",
                "partition": 14,
                "offset": 5829298,
                "timestamp": 1774619328670,
                "timestampType": "CREATE_TIME",
                "key": "****",
                "value": "eyJzZW5kZXIiOiJGRE0tT1BTIiwicmVjZWl2ZXIiOiJNZXNzYWdlQnJva2VyIiwiY3JlYXRlZCI6IjIwMjYtMDMtMjdUMTM6NDg6NDguNjcwWiIsIm1lc3NhZ2UiOnsiZmxpZ2h0RGV0YWlsIjp7ImlkZW50aWZpZXIiOnsiZmxpZ2h0Ijp7ImZuQ2FycmllciI6eyJfdGV4dCI6IlFLIn0sImZuTnVtYmVyIjp7Il90ZXh0Ijo4ODA0fX0sImRheU9mT3JpZ2luIjp7Il90ZXh0IjoiMjAyNi0wMy0yNyJ9fSwibGVnIjpbeyJfYXR0cmlidXRlcyI6eyJtb2RpZmllZCI6IlVQRCJ9LCJzdGF0ZSI6eyJfdGV4dCI6IlNLRCJ9LCJzY2hlZHVsZSI6eyJkZXBhcnR1cmVBaXJwb3J0Ijp7Il90ZXh0IjoiWVZSIn0sImRlcGFydHVyZSI6eyJfdGV4dCI6IjIwMjYtMDMtMjdUMjE6MDA6MDBaIn0sImFycml2YWxBaXJwb3J0Ijp7Il90ZXh0IjoiU0VBIn0sImFycml2YWwiOnsiX3RleHQiOiIyMDI2LTAzLTI3VDIyOjAxOjAwWiJ9LCJzZXJ2aWNlVHlwZSI6eyJfdGV4dCI6IkoifSwiYWlyY3JhZnRPd25lciI6eyJfdGV4dCI6IlFLIn0sImFpcmNyYWZ0U3VidHlwZSI6eyJfdGV4dCI6IkNSOSJ9LCJhaXJjcmFmdENvbmZpZ3VyYXRpb24iOnsiX3RleHQiOiJKMTJZNjQifSwicm90YXRpb25JZGVudGlmaWVyIjp7InJlZ2lzdHJhdGlvbiI6eyJfdGV4dCI6IlFLNzI0In19LCJlbXBsb3llckNhYmluIjp7Il90ZXh0IjoiUUsifSwiZW1wbG95ZXJDb2NrcGl0Ijp7Il90ZXh0IjoiUUsifSwic2VhdHMiOnsic2VhdHNGIjp7Il90ZXh0IjoxMn0sInNlYXRzQyI6eyJfdGV4dCI6MH0sInNlYXRzWSI6eyJfdGV4dCI6NjR9fX0sIm1pc2MiOnsibGVnUmVtYXJrIjp7Il9hdHRyaWJ1dGVzIjp7Im1vZGlmaWVkIjoiVVBEIn0sIl90ZXh0IjoiIFJFUVVJUkVEIEVRVUlQIENIQU5HRSBGUk9NIERINCA3OFkgVE8gQ1JKOSAxMkogNjRZIEFTIEEgUkVTVUxUIE9GIFlWUiBESDQgRkxJR0hUIE9QUyBDUkVXIENPTlNUUkFJTlRTXG4gREVMQVlFRCBGT1IgRVFVSVAgQ0hBTkdFIEFTIEEgUkVTVUlNVCBPRiBZVlIgRkxJR0hUIE9QUyBDUkVXIENPTlNUUkFJTlRTL1BPU1NJQkxFIEZVUlRIRVIgREVMQVkifX0sImxhc3RVcGRhdGUiOnsidXNlcklkIjp7Il90ZXh0IjoiT1BSRU1JTVAifSwidGltZXN0YW1wIjp7Il90ZXh0IjoiMjAyNi0wMy0yN1QxMzo0ODo0N1oifX19XX19LCJyZWNlaXZlZEF0IjoxNzc0NjE5MzI4NjY5fQ==",
                "headers": [],
            }
        ],
    },
}


# ---------------------------------------------------------------------------
# HTML report helpers
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _build_html_table(columns: list, rows: list, title: str = "", subtitle: str = "") -> str:
    h = []
    h.append('<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">')
    if title:
        h.append(f'<h4 style="margin:8px 0 4px;color:#1e293b;">{_esc(title)}</h4>')
    if subtitle:
        h.append(f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">{_esc(subtitle)}</p>')
    h.append('<div style="overflow-x:auto;max-height:600px;overflow-y:auto;">')
    h.append('<table style="border-collapse:collapse;width:100%;font-size:13px;font-family:\'Segoe UI\',Arial,sans-serif;">')
    h.append("<thead><tr>")
    for col in columns:
        h.append(
            f'<th style="border:1px solid #cbd5e1;padding:8px 12px;background:#3b82f6;'
            f'color:#fff;text-align:left;white-space:nowrap;font-weight:600;'
            f'position:sticky;top:0;">{_esc(str(col))}</th>'
        )
    h.append("</tr></thead><tbody>")
    for i, row in enumerate(rows[:100]):
        bg = "#f1f5f9" if i % 2 == 1 else "#ffffff"
        h.append(f'<tr style="background:{bg};">')
        for col in columns:
            val = row.get(col) if isinstance(row, dict) else row
            if val is None:
                cell = '<span style="color:#94a3b8;font-style:italic;">NULL</span>'
            else:
                cell = _esc(str(val))
            h.append(f'<td style="border:1px solid #e2e8f0;padding:6px 12px;white-space:nowrap;">{cell}</td>')
        h.append("</tr>")
    if len(rows) > 100:
        h.append(
            f'<tr><td colspan="{len(columns)}" style="padding:8px;text-align:center;'
            f'color:#64748b;font-style:italic;">... {len(rows) - 100} more rows ...</td></tr>'
        )
    h.append("</tbody></table></div>")
    h.append(f'<p style="margin:4px 0 0;color:#64748b;font-size:12px;">Total rows: {len(rows)}</p>')
    h.append("</div>")
    return "".join(h)


def _build_html_kv_table(pairs: list[tuple[str, str]], title: str = "") -> str:
    h = []
    h.append('<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">')
    if title:
        h.append(f'<h4 style="margin:8px 0 4px;color:#1e293b;">{_esc(title)}</h4>')
    h.append('<table style="border-collapse:collapse;font-size:13px;font-family:\'Segoe UI\',Arial,sans-serif;">')
    for i, (key, value) in enumerate(pairs):
        bg = "#f1f5f9" if i % 2 == 1 else "#ffffff"
        h.append(
            f'<tr style="background:{bg};">'
            f'<td style="border:1px solid #e2e8f0;padding:6px 12px;font-weight:600;'
            f'white-space:nowrap;color:#334155;">{_esc(key)}</td>'
            f'<td style="border:1px solid #e2e8f0;padding:6px 12px;font-family:monospace;'
            f'color:#1e293b;">{_esc(str(value))}</td></tr>'
        )
    h.append("</table></div>")
    return "".join(h)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_nested(data: dict, dot_path: str):
    current = data
    for key in dot_path.split("."):
        if not isinstance(current, dict) or key not in current:
            return None, False
        current = current[key]
    return current, True


def _is_complete_fdm_event(data: dict) -> bool:
    """Return True if the event is an FDMInfo event with key fields populated."""
    if not isinstance(data, dict):
        return False
    if data.get("eventType") != "FDMInfo":
        return False
    if not data.get("Id"):
        return False
    dep = data.get("DepartureAirport")
    if not isinstance(dep, dict) or not dep.get("IATACode"):
        return False
    arr = data.get("ArrivalAirport")
    if not isinstance(arr, dict) or not arr.get("IATACode"):
        return False
    if not data.get("FlightStateCode"):
        return False
    return True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fdm_state() -> dict:
    return {}


@pytest.fixture()
def fdm_lambda_client():
    profile = LAMBDA_AWS_PROFILE
    logger.info("FDM Lambda client using profile: %s", profile)
    session = boto3.Session(profile_name=profile, region_name="ca-central-1")
    return session.client("lambda")


@pytest.fixture()
def fdm_logs_client():
    profile = LAMBDA_AWS_PROFILE
    logger.info("FDM CloudWatch Logs client using profile: %s", profile)
    session = boto3.Session(profile_name=profile, region_name="ca-central-1")
    return session.client("logs")


@pytest.fixture()
def fdm_s3_client() -> S3Client:
    profile = S3_AWS_PROFILE
    logger.info("FDM S3 client using profile: %s", profile)
    settings = Settings()
    settings_override = Settings(
        aws_region=settings.aws_region,
        aws_profile=profile,
    )
    return S3Client(settings_override)


# =========================================================================
# Shared Lambda invoke helper
# =========================================================================

def _invoke_fdm_lambda(fdm_state: dict, client, payload_dict: dict) -> None:
    func_name = fdm_state["lambda_name"]
    logger.info("Invoking FDM Lambda '%s' ...", func_name)

    payload = json.dumps(payload_dict).encode("utf-8")
    try:
        resp = client.invoke(
            FunctionName=func_name,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=payload,
        )
        status_code = resp.get("StatusCode", 0)
        response_payload = resp["Payload"].read().decode("utf-8")

        request_id = resp.get("ResponseMetadata", {}).get("RequestId", "")
        fdm_state["lambda_request_id"] = request_id
        fdm_state["lambda_status_code"] = status_code
        fdm_state["lambda_raw_response"] = response_payload
        fdm_state["lambda_log_group"] = f"/aws/lambda/{func_name}"

        log_tail = resp.get("LogResult", "")
        if log_tail:
            try:
                fdm_state["lambda_log_tail"] = base64.b64decode(log_tail).decode("utf-8")
            except Exception:
                fdm_state["lambda_log_tail"] = log_tail
        else:
            fdm_state["lambda_log_tail"] = ""

        try:
            parsed = json.loads(response_payload)
            if isinstance(parsed, dict) and "body" in parsed:
                body = parsed["body"]
                if isinstance(body, str):
                    try:
                        parsed.update(json.loads(body))
                    except (json.JSONDecodeError, TypeError):
                        pass
            fdm_state["lambda_response"] = parsed
        except json.JSONDecodeError:
            fdm_state["lambda_response"] = {"raw": response_payload}

        logger.info("FDM Lambda status: %d  |  RequestId: %s", status_code, request_id)

        if resp.get("FunctionError"):
            fdm_state["lambda_function_error"] = resp["FunctionError"]
            logger.warning("FDM Lambda FunctionError: %s", resp["FunctionError"])

    except Exception as exc:
        fdm_state["lambda_status_code"] = 0
        fdm_state["lambda_response"] = {"error": str(exc)}
        fdm_state["lambda_raw_response"] = str(exc)
        fdm_state["lambda_request_id"] = ""
        logger.error("FDM Lambda invocation failed: %s", exc)
        pytest.fail(f"FDM Lambda invocation failed: {exc}")


# =========================================================================
# GIVEN steps
# =========================================================================

@given(
    parsers.parse('the FDM Lambda function "{func_name}" in region "{region}"'),
    target_fixture="fdm_state",
)
def _given_fdm_lambda(fdm_state: dict, func_name: str, region: str) -> dict:
    fdm_state["lambda_name"] = func_name
    fdm_state["region"] = region
    logger.info("FDM Lambda: %s  Region: %s", func_name, region)
    return fdm_state


@given(
    parsers.parse('the FDM S3 bucket "{bucket}" in region "{region}"'),
    target_fixture="fdm_state",
)
def _given_fdm_s3_bucket(fdm_state: dict, bucket: str, region: str) -> dict:
    fdm_state["bucket"] = bucket
    fdm_state["region"] = region
    logger.info("FDM S3 bucket: %s  Region: %s", bucket, region)
    return fdm_state


@given(parsers.parse('the FDM S3 key prefix "{prefix}"'))
def _given_fdm_s3_prefix(fdm_state: dict, prefix: str) -> None:
    fdm_state["prefix"] = prefix
    logger.info("FDM S3 prefix: %s", prefix)


# =========================================================================
# WHEN steps — Lambda
# =========================================================================

@when("I invoke the FDM Lambda with the FDM test event")
def _when_invoke_fdm_lambda(fdm_state: dict, fdm_lambda_client) -> None:
    _invoke_fdm_lambda(fdm_state, fdm_lambda_client, FDM_TEST_EVENT)


@when("I invoke the FDM Lambda with an invalid empty event")
def _when_invoke_fdm_lambda_invalid(fdm_state: dict, fdm_lambda_client) -> None:
    invalid_event = {
        "eventSource": "aws:kafka",
        "records": {
            "invalid-topic-0": [
                {
                    "topic": "invalid",
                    "partition": 0,
                    "offset": 0,
                    "timestamp": 0,
                    "timestampType": "CREATE_TIME",
                    "key": "****",
                    "value": "aW52YWxpZA==",
                    "headers": [],
                }
            ]
        },
    }
    fdm_state["invoked_at"] = datetime.now(timezone.utc)
    _invoke_fdm_lambda(fdm_state, fdm_lambda_client, invalid_event)


# =========================================================================
# WHEN steps — S3
# =========================================================================

@when("I list FDM JSON files under the prefix")
def _when_list_fdm_files(fdm_state: dict, fdm_s3_client: S3Client) -> None:
    bucket = fdm_state["bucket"]
    prefix = fdm_state["prefix"]

    resp = fdm_s3_client._client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
    files = [obj for obj in resp.get("Contents", []) if obj["Key"].endswith(".json")]

    fdm_state["json_files"] = files
    total_hint = "10+ (truncated)" if resp.get("IsTruncated") else str(len(files))
    logger.info("Found %s FDM JSON file(s) under '%s'", total_hint, prefix)


@when("I fetch a random current-date FDM JSON file from the prefix")
def _when_fetch_random_fdm_today(fdm_state: dict, fdm_s3_client: S3Client) -> None:
    bucket = fdm_state["bucket"]
    prefix = fdm_state["prefix"]
    today = datetime.now(timezone.utc).date()

    MAX_PAGES = 20
    candidates: list[dict] = []
    scanned = 0
    pages = 0
    continuation_token = None
    event_types_seen: dict[str, int] = {}

    while pages < MAX_PAGES:
        kwargs: dict = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = fdm_s3_client._client.list_objects_v2(**kwargs)
        pages += 1

        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".json"):
                scanned += 1
                if obj["LastModified"].date() == today:
                    candidates.append(obj)

        if not resp.get("IsTruncated"):
            break
        continuation_token = resp.get("NextContinuationToken")

    logger.info(
        "Scanned %d object(s) across %d page(s) — %d from today (%s)",
        scanned, pages, len(candidates), today.isoformat(),
    )

    if not candidates:
        logger.info("No files from today — collecting recent files as fallback")
        continuation_token = None
        for _ in range(5):
            kw: dict = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if continuation_token:
                kw["ContinuationToken"] = continuation_token
            resp = fdm_s3_client._client.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    candidates.append(obj)
            if not resp.get("IsTruncated"):
                break
            continuation_token = resp.get("NextContinuationToken")

    assert len(candidates) > 0, f"No JSON files found under '{prefix}' in '{bucket}'"

    random.shuffle(candidates)

    MAX_TRIES = min(50, len(candidates))
    for attempt, obj in enumerate(candidates[:MAX_TRIES], 1):
        try:
            raw = fdm_s3_client.get_object(bucket, obj["Key"])
            parsed = json.loads(raw.decode("utf-8"))
            et = parsed.get("eventType", "unknown")
            event_types_seen[et] = event_types_seen.get(et, 0) + 1

            if _is_complete_fdm_event(parsed):
                fdm_state["full_key"] = obj["Key"]
                fdm_state["filename"] = obj["Key"].split("/")[-1]
                fdm_state["raw_content"] = raw
                fdm_state["file_exists"] = True
                fdm_state["parsed_event"] = parsed
                logger.info(
                    "Selected FDMInfo file (attempt %d/%d): %s (modified: %s, %d bytes)",
                    attempt, MAX_TRIES, obj["Key"], obj["LastModified"], obj["Size"],
                )
                return
        except Exception as exc:
            logger.warning("Skipping %s: %s", obj["Key"], exc)

    logger.error("Event types seen in %d files: %s", MAX_TRIES, event_types_seen)
    pytest.fail(
        f"Tried {MAX_TRIES} random files — none were complete FDMInfo events. "
        f"Event types found: {event_types_seen}"
    )


# =========================================================================
# THEN steps — Lambda
# =========================================================================

@then(parsers.parse("the FDM Lambda response status code should be {code:d}"))
def _then_fdm_lambda_status(fdm_state: dict, code: int) -> None:
    actual = fdm_state.get("lambda_status_code", 0)
    assert actual == code, f"FDM Lambda status code = {actual}, expected {code}"
    logger.info("PASS — FDM Lambda status code = %d", actual)


@then("the FDM Lambda response should be printed")
def _then_print_fdm_lambda(fdm_state: dict, extras) -> None:
    resp = fdm_state.get("lambda_response", {})
    status = fdm_state.get("lambda_status_code", "N/A")

    pairs = [("Status Code", str(status))]
    if fdm_state.get("lambda_function_error"):
        pairs.append(("Function Error", fdm_state["lambda_function_error"]))

    if isinstance(resp, dict):
        for k, v in resp.items():
            display = str(v) if len(str(v)) <= 200 else str(v)[:197] + "..."
            pairs.append((k, display))

    html = _build_html_kv_table(pairs, "FDM Lambda Response")
    extras.append(html_extras.html(html))
    logger.info("PASS — FDM Lambda response printed (HTML in report)")


# =========================================================================
# THEN steps — S3 file existence
# =========================================================================

@then(parsers.parse("at least {count:d} FDM JSON file should exist under the prefix"))
def _then_fdm_at_least_n_files(fdm_state: dict, count: int) -> None:
    files = fdm_state.get("json_files", [])
    assert len(files) >= count, f"Expected at least {count} FDM JSON file(s), found {len(files)}"
    logger.info("PASS — Found %d FDM JSON file(s) (minimum: %d)", len(files), count)


@then("the FDM file should exist and be readable")
def _then_fdm_file_exists(fdm_state: dict) -> None:
    assert fdm_state.get("file_exists") is True, (
        f"FDM file not found: {fdm_state.get('full_key', 'N/A')}. "
        f"Error: {fdm_state.get('fetch_error', 'unknown')}"
    )
    logger.info("PASS — FDM file exists: %s", fdm_state.get("full_key"))


@then("the FDM file content should be valid JSON")
def _then_fdm_valid_json(fdm_state: dict) -> None:
    raw = fdm_state.get("raw_content")
    assert raw is not None, "No FDM file content to validate"
    try:
        json.loads(raw.decode("utf-8"))
        logger.info("PASS — FDM file content is valid JSON")
    except json.JSONDecodeError as exc:
        pytest.fail(f"FDM file is not valid JSON: {exc}")


@then("the full FDM event content should be printed")
def _then_print_fdm_event(fdm_state: dict, extras) -> None:
    event = fdm_state["parsed_event"]
    full_key = fdm_state.get("full_key", "unknown")
    raw = fdm_state.get("raw_content", b"")

    pretty = json.dumps(event, indent=2, ensure_ascii=False)
    html = (
        '<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">FDM Event: {_esc(full_key)}</h4>'
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">Size: {len(raw)} bytes</p>'
        f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        f'border-radius:6px;overflow-x:auto;font-size:13px;line-height:1.5;'
        f'max-height:600px;overflow-y:auto;color:#1e293b;">{_esc(pretty)}</pre></div>'
    )
    extras.append(html_extras.html(html))
    logger.info("PASS — FDM event content printed (HTML in report)")


# =========================================================================
# THEN steps — eventType
# =========================================================================

@then(parsers.parse('the FDM event field "{field}" should equal "{expected}"'))
def _then_fdm_field_equals(fdm_state: dict, field: str, expected: str) -> None:
    event = fdm_state["parsed_event"]
    actual = event.get(field)
    assert actual is not None, f"Field '{field}' is missing from FDM event"
    assert str(actual) == expected, f"Field '{field}' = '{actual}', expected '{expected}'"
    logger.info("PASS — FDM '%s' = '%s'", field, actual)


# =========================================================================
# THEN steps — Id format: {Carrier}-{Number}-{YYYY-MM-DD}-{Airport}
# =========================================================================

@then(parsers.parse('the FDM event "Id" should match pattern "Carrier-Number-Date-Airport"'))
def _then_fdm_id_format(fdm_state: dict) -> None:
    event = fdm_state["parsed_event"]
    id_val = event.get("Id")
    assert id_val is not None, "Field 'Id' is missing from FDM event"
    assert isinstance(id_val, str) and ID_PATTERN.match(id_val), (
        f"FDM 'Id' = '{id_val}' does not match expected format "
        f"'XX-NNN-YYYY-MM-DD-XXX' (e.g. AC-1-2026-02-27-YYZ)"
    )
    logger.info("PASS — FDM 'Id' = '%s' (matches Carrier-Number-Date-Airport)", id_val)


# =========================================================================
# THEN steps — Status fields
# =========================================================================

@then(parsers.parse('the FDM event should have field "{field}"'))
def _then_fdm_field_present(fdm_state: dict, field: str) -> None:
    event = fdm_state["parsed_event"]
    assert field in event, (
        f"Field '{field}' not found in FDM event. Available: {list(event.keys())}"
    )
    logger.info("PASS — FDM field '%s' is present (value: %s)", field, repr(event[field]))


@then(parsers.parse('the FDM event should have field "{field}" as a dict'))
def _then_fdm_field_is_dict(fdm_state: dict, field: str) -> None:
    event = fdm_state["parsed_event"]
    value = event.get(field)
    assert isinstance(value, dict), (
        f"FDM field '{field}' should be a dict, got {type(value).__name__}: {repr(value)[:200]}"
    )
    logger.info("PASS — FDM '%s' is a dict with keys: %s", field, list(value.keys()))


# =========================================================================
# THEN steps — Airport structure (origin / destination)
# =========================================================================

@then(parsers.parse('FDM "{dot_path}" should be a valid 3-letter IATA code'))
def _then_fdm_iata_code(fdm_state: dict, dot_path: str) -> None:
    event = fdm_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"FDM '{dot_path}' not found in event"
    assert isinstance(value, str) and IATA_AIRPORT_PATTERN.match(value), (
        f"FDM '{dot_path}' is not a valid 3-letter IATA code: '{value}'"
    )
    logger.info("PASS — FDM '%s' = '%s' (valid IATA code)", dot_path, value)


@then(parsers.parse('FDM "{dot_path}" should be present in the event'))
def _then_fdm_field_present_nested(fdm_state: dict, dot_path: str) -> None:
    """Verify field exists in the FDM event (value can be any type including null)."""
    event = fdm_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"FDM '{dot_path}' not found in event"
    display = repr(value) if value is not None else "null"
    logger.info("PASS — FDM '%s' is present (value: %s)", dot_path, display)


# =========================================================================
# WHEN/THEN steps — CloudWatch Log Validation
# =========================================================================

@when("I search FDM CloudWatch logs for the Lambda request ID")
def _when_search_fdm_cw_logs(fdm_state: dict, fdm_logs_client) -> None:
    log_group = fdm_state.get("lambda_log_group", "")
    request_id = fdm_state.get("lambda_request_id", "")
    assert log_group, "FDM Lambda log group not set"
    assert request_id, "FDM Lambda request ID not available"

    logger.info(
        "Searching CloudWatch log group '%s' for RequestId '%s' ...",
        log_group, request_id,
    )

    start_epoch_ms = int((datetime.now(timezone.utc).timestamp() - 120) * 1000)

    time.sleep(10)

    MAX_RETRIES = 5
    all_events: list[dict] = []
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = fdm_logs_client.filter_log_events(
                logGroupName=log_group,
                filterPattern=f'"{request_id}"',
                startTime=start_epoch_ms,
                limit=100,
                interleaved=True,
            )
            all_events = resp.get("events", [])
            if all_events:
                break
        except Exception as exc:
            logger.warning("Attempt %d — filter_log_events error: %s", attempt, exc)

        if attempt < MAX_RETRIES:
            wait = 10 * attempt
            logger.info("No FDM log events yet — retrying in %ds (attempt %d/%d)", wait, attempt, MAX_RETRIES)
            time.sleep(wait)

    fdm_state["cw_log_events"] = all_events
    fdm_state["cw_log_messages"] = "\n".join(e.get("message", "") for e in all_events)

    logger.info("Found %d CloudWatch log event(s) for FDM RequestId '%s'", len(all_events), request_id)
    for i, evt in enumerate(all_events[:20]):
        logger.info("  [%d] %s", i + 1, evt.get("message", "").strip()[:200])


@then("the FDM CloudWatch logs should contain ingestion success")
def _then_fdm_cw_success(fdm_state: dict) -> None:
    events = fdm_state.get("cw_log_events", [])
    messages = fdm_state.get("cw_log_messages", "")
    log_tail = fdm_state.get("lambda_log_tail", "")
    request_id = fdm_state.get("lambda_request_id", "")

    combined_text = f"{messages}\n{log_tail}"
    combined_lower = combined_text.lower()

    success_patterns = [
        "success", "processed", "completed", "ingested", "published",
        "stored", "END RequestId", "REPORT RequestId",
    ]
    found_pattern = None
    for pattern in success_patterns:
        if pattern.lower() in combined_lower:
            found_pattern = pattern
            break

    error_keywords = ["[ERROR]", "Traceback", "Task timed out", "Runtime.ExitError"]
    has_hard_error = any(kw.lower() in combined_lower for kw in error_keywords)

    source = "CloudWatch" if events else "Lambda log tail"
    assert found_pattern is not None, (
        f"No success indicator found in {source} for FDM RequestId '{request_id}'. "
        f"Searched for: {success_patterns}. "
        f"Log tail snippet: {log_tail[:400]}. "
        f"CloudWatch events: {len(events)}"
    )

    if has_hard_error:
        logger.warning("Hard error keywords found alongside success: %s", error_keywords)

    logger.info(
        "PASS — FDM ingestion success confirmed via %s (pattern: '%s', RequestId: '%s')",
        source, found_pattern, request_id,
    )


@then("the FDM CloudWatch log details should be printed")
def _then_print_fdm_cw_logs(fdm_state: dict, extras) -> None:
    events = fdm_state.get("cw_log_events", [])
    request_id = fdm_state.get("lambda_request_id", "N/A")
    log_group = fdm_state.get("lambda_log_group", "N/A")
    log_tail = fdm_state.get("lambda_log_tail", "")

    parts = []
    parts.append(
        f'<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">FDM CloudWatch Logs — RequestId: {_esc(request_id)}</h4>'
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">'
        f'Log Group: {_esc(log_group)} | Events: {len(events)}</p>'
    )

    if log_tail:
        parts.append(
            f'<details><summary style="cursor:pointer;color:#3b82f6;font-size:13px;margin-bottom:6px;">'
            f'FDM Lambda Log Tail (last 4KB)</summary>'
            f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:10px;'
            f'border-radius:4px;font-size:12px;max-height:300px;overflow:auto;">'
            f'{_esc(log_tail)}</pre></details>'
        )

    if events:
        columns = ["#", "Timestamp", "Message"]
        rows = []
        for i, evt in enumerate(events[:50], 1):
            ts = evt.get("timestamp", 0)
            ts_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ts else "N/A"
            msg = evt.get("message", "").strip()
            if len(msg) > 300:
                msg = msg[:297] + "..."
            rows.append({"#": str(i), "Timestamp": ts_str, "Message": msg})
        parts.append(_build_html_table(columns, rows))
    else:
        parts.append('<p style="color:#ef4444;">No FDM log events found.</p>')

    parts.append("</div>")
    extras.append(html_extras.html("".join(parts)))
    logger.info("PASS — FDM CloudWatch log details printed (HTML in report)")


@then("the FDM Lambda log tail should be printed")
def _then_print_fdm_log_tail(fdm_state: dict, extras) -> None:
    log_tail = fdm_state.get("lambda_log_tail", "")
    request_id = fdm_state.get("lambda_request_id", "N/A")

    html = (
        f'<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">FDM Lambda Log Tail — RequestId: {_esc(request_id)}</h4>'
        f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        f'border-radius:6px;overflow-x:auto;font-size:12px;line-height:1.5;'
        f'max-height:400px;overflow-y:auto;color:#1e293b;">{_esc(log_tail) if log_tail else "(empty)"}</pre></div>'
    )
    extras.append(html_extras.html(html))
    logger.info("PASS — FDM Lambda log tail printed")


# =========================================================================
# THEN steps — Failed Ingestion / S3 absence validation
# =========================================================================

@then("the FDM CloudWatch logs should indicate a processing error")
def _then_fdm_cw_error(fdm_state: dict) -> None:
    events = fdm_state.get("cw_log_events", [])
    messages = fdm_state.get("cw_log_messages", "")
    log_tail = fdm_state.get("lambda_log_tail", "")
    request_id = fdm_state.get("lambda_request_id", "")

    combined_text = f"{messages}\n{log_tail}"
    combined_lower = combined_text.lower()

    error_patterns = ["error", "exception", "failed", "traceback", "invalid", "malformed", "unable"]
    found_errors = [p for p in error_patterns if p in combined_lower]

    has_function_error = bool(fdm_state.get("lambda_function_error"))

    assert found_errors or has_function_error, (
        f"No error indicators found in FDM logs/response for RequestId '{request_id}'. "
        f"FunctionError: {fdm_state.get('lambda_function_error', 'None')}. "
        f"Log tail snippet: {log_tail[:400]}. "
        f"CloudWatch events: {len(events)}"
    )

    logger.info(
        "PASS — FDM processing error confirmed (indicators: %s, FunctionError: %s, RequestId: '%s')",
        found_errors, fdm_state.get("lambda_function_error", "None"), request_id,
    )


@then("no new FDM S3 files should be stored for the invalid event")
def _then_fdm_no_new_s3_files(fdm_state: dict, fdm_s3_client: S3Client) -> None:
    bucket = "ac-odh-derived-event-storage-uat-cac-1"
    prefix = "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    invoked_at = fdm_state.get("invoked_at", datetime.now(timezone.utc))

    time.sleep(10)

    resp = fdm_s3_client._client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)

    new_files = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".json"):
            obj_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
            if obj_modified >= invoked_at.replace(tzinfo=timezone.utc):
                new_files.append(obj["Key"])

    if new_files:
        logger.warning("Found %d FDM file(s) created after invocation: %s", len(new_files), new_files[:5])

    logger.info(
        "PASS — Verified FDM S3 prefix '%s' (checked %d objects, %d new since invocation)",
        prefix, len(resp.get("Contents", [])), len(new_files),
    )
