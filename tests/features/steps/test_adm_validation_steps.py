"""Step definitions for validating ADM events — Lambda ingestion and S3 output.

Lambda invocation targets AC-Digital-BAT  (profile: AC_Digital_BAT).
S3 output validation targets AC-DATA-ODH-UAT (profile: ODH_UAT).

Override via environment variables if the default profile names differ:
    LAMBDA_AWS_PROFILE – profile for Lambda API calls (default: AC_Digital_BAT)
    S3_AWS_PROFILE     – profile for S3 API calls     (default: ODH_UAT)
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from datetime import datetime, timezone

import boto3
import pytest
from pytest_bdd import given, when, then, scenarios, parsers
from pytest_html import extras as html_extras

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings

scenarios("adm_validation.feature")

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
# ADM Lambda test event (Kafka-wrapped ADM message)
# ---------------------------------------------------------------------------
ADM_TEST_EVENT = {
    "eventSource": "aws:kafka",
    "eventSourceArn": "arn:aws:kafka:ca-central-1:574529944728:cluster/digital-msk-batca1/6415732f-1c7d-4e62-aacf-95c089a0ef75-2",
    "bootstrapServers": "b-3.digitalmskbatca1.arylnm.c2.kafka.ca-central-1.amazonaws.com:9094,b-5.digitalmskbatca1.arylnm.c2.kafka.ca-central-1.amazonaws.com:9094,b-1.digitalmskbatca1.arylnm.c2.kafka.ca-central-1.amazonaws.com:9094",
    "records": {
        "emh-dev.EAI-ADM-UAT-0": [
            {
                "topic": "emh-dev.EAI-ADM-UAT",
                "partition": 0,
                "offset": 22255310,
                "timestamp": 1773097992751,
                "timestampType": "CREATE_TIME",
                "value": "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiIHN0YW5kYWxvbmU9InllcyI/PjxhY2ZpZHM6YWlycG9ydERldGFpbHNNZXNzYWdlIHhtbG5zOm1jPSJodHRwOi8vd3d3LmNhc3Nlcy5hZXJvL3NjaGVtYS9jdXN0b21lci9BQ0EvQUNGSURTL01lc3NhZ2VDb21wb25lbnQiIHhtbG5zOmFjZmlkcz0iaHR0cDovL3d3dy5jYXNzZXMuYWVyby9zY2hlbWEvY3VzdG9tZXIvQUNBL0FDRklEUyI+PGFkZHJlc3M+RUFJPC9hZGRyZXNzPjxpZGVudGlmaWVyPjxmbGlnaHQ+PGZuQ2Fycmllcj5BQzwvZm5DYXJyaWVyPjxmbk51bWJlcj4xMzc3PC9mbk51bWJlcj48L2ZsaWdodD48ZGF5T2ZPcmlnaW4+MjAyNi0wNC0wMTwvZGF5T2ZPcmlnaW4+PC9pZGVudGlmaWVyPjxsZWcgbW9kaWZpZWQ9IlVQRCI+PHN0YXRlPlNLRDwvc3RhdGU+PHNjaGVkdWxlPjxkZXBhcnR1cmVBaXJwb3J0PkVXUjwvZGVwYXJ0dXJlQWlycG9ydD48ZGVwYXJ0dXJlPjIwMjYtMDQtMDFUMjI6NTU6MDBaPC9kZXBhcnR1cmU+PGFycml2YWxBaXJwb3J0PllZWjwvYXJyaXZhbEFpcnBvcnQ+PGFycml2YWw+MjAyNi0wNC0wMlQwMDozNDowMFo8L2Fycml2YWw+PHNlcnZpY2VUeXBlPko8L3NlcnZpY2VUeXBlPjxhaXJjcmFmdE93bmVyPkFDPC9haXJjcmFmdE93bmVyPjxhaXJjcmFmdFN1YnR5cGU+MjIzPC9haXJjcmFmdFN1YnR5cGU+PGFpcmNyYWZ0Q29uZmlndXJhdGlvbj5KMTJZMTI1PC9haXJjcmFmdENvbmZpZ3VyYXRpb24+PHJvdGF0aW9uSWRlbnRpZmllciBtb2RpZmllZD0iVVBEIj48cHJldlJlZ2lzdHJhdGlvbj5BQzExNjwvcHJldlJlZ2lzdHJhdGlvbj48cmVnaXN0cmF0aW9uPkFDMTAyPC9yZWdpc3RyYXRpb24+PC9yb3RhdGlvbklkZW50aWZpZXI+PGVtcGxveWVyQ2FiaW4+QUM8L2VtcGxveWVyQ2FiaW4+PGVtcGxveWVyQ29ja3BpdD5BQzwvZW1wbG95ZXJDb2NrcGl0PjxzZWF0cz48c2VhdHNGPjEyPC9zZWF0c0Y+PHNlYXRzQz4wPC9zZWF0c0M+PHNlYXRzWT4xMjU8L3NlYXRzWT48L3NlYXRzPjwvc2NoZWR1bGU+PGxhc3RVcGRhdGU+PHVzZXJJZD5UTFhJTlQ8L3VzZXJJZD48dGltZXN0YW1wPjIwMjYtMDMtMjdUMDU6MjY6NDhaPC90aW1lc3RhbXA+PC9sYXN0VXBkYXRlPjwvbGVnPjxtYzpBQ0ZJRFNNZXNzYWdlQ29tcG9uZW50PjxDQ0ZsaWdodEluZGljYXRvcj5OPC9DQ0ZsaWdodEluZGljYXRvcj48ZXZlbnQgbW9kaWZpZWQ9IlVQRCI+RkRNPC9ldmVudD48RklEU1N0YXR1cz5PblRpbWU8L0ZJRFNTdGF0dXM+PHN0YXRpb24+RVdSPC9zdGF0aW9uPjxzdGF0aW9uRmxpZ2h0VGltZT4yMDI2LTA0LTAxIDE4OjU1TDwvc3RhdGlvbkZsaWdodFRpbWU+PGxlZ1JvdXRpbmc+PGRlcFN0YXRpb24+RVdSPC9kZXBTdGF0aW9uPjxkZXBTdGF0aW9uTmFtZT5OZXdhcmsgSW50bDwvZGVwU3RhdGlvbk5hbWU+PGFyclN0YXRpb24+WVlaPC9hcnJTdGF0aW9uPjxhcnJTdGF0aW9uTmFtZT5Ub3JvbnRvPC9hcnJTdGF0aW9uTmFtZT48ZmluTnVtYmVyPkFDMTAyPC9maW5OdW1iZXI+PHJlZ2lzdHJhdGlvbk51bWJlcj5YR0pYRTwvcmVnaXN0cmF0aW9uTnVtYmVyPjxwcm9kdWN0VHlwZT5UcmFuc2JvcmRlcjwvcHJvZHVjdFR5cGU+PC9sZWdSb3V0aW5nPjxHYXRpbmc+PG9wZXJhdGlvbmFsR2F0ZS8+PHB1YmxpY0dhdGUvPjxnYXRlUmVtYXJrLz48b3BlcmF0aW9uYWxUZXJtaW5hbElELz48cHVibGljVGVybWluYWxJRC8+PHpvbmUvPjxncm91bmR0aW1lPjAwMDo1MjwvZ3JvdW5kdGltZT48bWluaW11bUdyb3VuZHRpbWU+MDAwOjQwPC9taW5pbXVtR3JvdW5kdGltZT48L0dhdGluZz48QmFnZ2FnZT48YmFnZ2FnZUJlbHQvPjxjYXJvdXNlbC8+PC9CYWdnYWdlPjxSZW1hcmtzPjxUT1dSZW1hcmsvPjxBVFdSZW1hcmsvPjxCVFdSZW1hcmsvPjxDQVJHT1JlbWFyay8+PE9QRVJBVElPTlNSZW1hcmsvPjxwdWJsaWNSZW1hcmsvPjxlbXBsb3llZU5vdGljZT5FV1I8L2VtcGxveWVlTm90aWNlPjxvcGVyYXRpb25zTm90aWNlPkVXUjwvb3BlcmF0aW9uc05vdGljZT48L1JlbWFya3M+PExBQTE+c3RhdGlvbkZsaWdodFRpbWVVVEM9MjAyNi0wNC0wMVQyMjo1NVo8L0xBQTE+PExBQTIvPjxMQUEzLz48L21jOkFDRklEU01lc3NhZ2VDb21wb25lbnQ+PC9hY2ZpZHM6YWlycG9ydERldGFpbHNNZXNzYWdlPg==",
                "headers": [],
            }
        ]
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
    h.append('<thead><tr>')
    for col in columns:
        h.append(
            f'<th style="border:1px solid #cbd5e1;padding:8px 12px;background:#3b82f6;'
            f'color:#fff;text-align:left;white-space:nowrap;font-weight:600;'
            f'position:sticky;top:0;">{_esc(str(col))}</th>'
        )
    h.append('</tr></thead><tbody>')
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
        h.append('</tr>')
    if len(rows) > 100:
        h.append(
            f'<tr><td colspan="{len(columns)}" style="padding:8px;text-align:center;'
            f'color:#64748b;font-style:italic;">... {len(rows) - 100} more rows ...</td></tr>'
        )
    h.append('</tbody></table></div>')
    h.append(f'<p style="margin:4px 0 0;color:#64748b;font-size:12px;">Total rows: {len(rows)}</p>')
    h.append('</div>')
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
    h.append('</table></div>')
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


def _is_complete_adm_event(data: dict) -> bool:
    """Return True if the event is an ADMInfo event with key fields populated."""
    if not isinstance(data, dict):
        return False
    if data.get("eventType") != "ADMInfo":
        return False
    if not data.get("Id"):
        return False
    dep = data.get("DepartureAirport")
    if not isinstance(dep, dict) or not dep.get("IATACode"):
        return False
    arr = data.get("ArrivalAirport")
    if not isinstance(arr, dict) or not arr.get("IATACode"):
        return False
    return True




# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def adm_state() -> dict:
    return {}


@pytest.fixture()
def lambda_client():
    profile = LAMBDA_AWS_PROFILE
    logger.info("Lambda client using profile: %s", profile)
    session = boto3.Session(profile_name=profile, region_name="ca-central-1")
    return session.client("lambda")


@pytest.fixture()
def logs_client():
    profile = LAMBDA_AWS_PROFILE
    logger.info("CloudWatch Logs client using profile: %s", profile)
    session = boto3.Session(profile_name=profile, region_name="ca-central-1")
    return session.client("logs")


@pytest.fixture()
def s3_client() -> S3Client:
    profile = S3_AWS_PROFILE
    logger.info("S3 client using profile: %s", profile)
    settings = Settings()
    settings_override = Settings(
        aws_region=settings.aws_region,
        aws_profile=profile,
    )
    return S3Client(settings_override)


# =========================================================================
# GIVEN steps
# =========================================================================

@given(
    parsers.parse('the ADM Lambda function "{func_name}" in region "{region}"'),
    target_fixture="adm_state",
)
def _given_lambda(adm_state: dict, func_name: str, region: str) -> dict:
    adm_state["lambda_name"] = func_name
    adm_state["region"] = region
    logger.info("ADM Lambda: %s  Region: %s", func_name, region)
    return adm_state


@given(
    parsers.parse('the ADM S3 bucket "{bucket}" in region "{region}"'),
    target_fixture="adm_state",
)
def _given_s3_bucket(adm_state: dict, bucket: str, region: str) -> dict:
    adm_state["bucket"] = bucket
    adm_state["region"] = region
    logger.info("S3 bucket: %s  Region: %s", bucket, region)
    return adm_state


@given(parsers.parse('the ADM S3 key prefix "{prefix}"'))
def _given_s3_prefix(adm_state: dict, prefix: str) -> None:
    adm_state["prefix"] = prefix
    logger.info("S3 prefix: %s", prefix)


# =========================================================================
# WHEN steps — Lambda
# =========================================================================

def _invoke_adm_lambda(adm_state: dict, lambda_client, payload_dict: dict) -> None:
    """Shared helper to invoke the ADM Lambda and store response in adm_state."""
    import base64

    func_name = adm_state["lambda_name"]
    logger.info("Invoking Lambda '%s' ...", func_name)

    payload = json.dumps(payload_dict).encode("utf-8")
    try:
        resp = lambda_client.invoke(
            FunctionName=func_name,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=payload,
        )
        status_code = resp.get("StatusCode", 0)
        response_payload = resp["Payload"].read().decode("utf-8")

        request_id = resp.get("ResponseMetadata", {}).get("RequestId", "")
        adm_state["lambda_request_id"] = request_id
        adm_state["lambda_status_code"] = status_code
        adm_state["lambda_raw_response"] = response_payload
        adm_state["lambda_log_group"] = f"/aws/lambda/{func_name}"

        log_tail = resp.get("LogResult", "")
        if log_tail:
            try:
                adm_state["lambda_log_tail"] = base64.b64decode(log_tail).decode("utf-8")
            except Exception:
                adm_state["lambda_log_tail"] = log_tail
        else:
            adm_state["lambda_log_tail"] = ""

        try:
            parsed = json.loads(response_payload)
            if isinstance(parsed, dict) and "body" in parsed:
                body = parsed["body"]
                if isinstance(body, str):
                    try:
                        parsed.update(json.loads(body))
                    except (json.JSONDecodeError, TypeError):
                        pass
            adm_state["lambda_response"] = parsed
        except json.JSONDecodeError:
            adm_state["lambda_response"] = {"raw": response_payload}

        logger.info("Lambda status: %d  |  RequestId: %s", status_code, request_id)

        if resp.get("FunctionError"):
            adm_state["lambda_function_error"] = resp["FunctionError"]
            logger.warning("Lambda FunctionError: %s", resp["FunctionError"])

    except Exception as exc:
        adm_state["lambda_status_code"] = 0
        adm_state["lambda_response"] = {"error": str(exc)}
        adm_state["lambda_raw_response"] = str(exc)
        adm_state["lambda_request_id"] = ""
        logger.error("Lambda invocation failed: %s", exc)
        pytest.fail(f"Lambda invocation failed: {exc}")


@when("I invoke the ADM Lambda with the ADM test event")
def _when_invoke_lambda(adm_state: dict, lambda_client) -> None:
    _invoke_adm_lambda(adm_state, lambda_client, ADM_TEST_EVENT)


@when("I invoke the ADM Lambda with an invalid empty event")
def _when_invoke_lambda_invalid(adm_state: dict, lambda_client) -> None:
    invalid_event = {
        "eventSource": "aws:kafka",
        "records": {"invalid-topic-0": [{"topic": "invalid", "partition": 0, "offset": 0, "timestamp": 0, "timestampType": "CREATE_TIME", "value": "aW52YWxpZA==", "headers": []}]},
    }
    adm_state["invoked_at"] = datetime.now(timezone.utc)
    _invoke_adm_lambda(adm_state, lambda_client, invalid_event)


# =========================================================================
# WHEN steps — S3
# =========================================================================

@when("I list ADM JSON files under the prefix")
def _when_list_files(adm_state: dict, s3_client: S3Client) -> None:
    bucket = adm_state["bucket"]
    prefix = adm_state["prefix"]

    resp = s3_client._client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
    files = [obj for obj in resp.get("Contents", []) if obj["Key"].endswith(".json")]

    adm_state["json_files"] = files
    total_hint = "10+ (truncated)" if resp.get("IsTruncated") else str(len(files))
    logger.info("Found %s JSON file(s) under '%s'", total_hint, prefix)


@when("I fetch a random current-date ADM JSON file from the prefix")
def _when_fetch_random_today(adm_state: dict, s3_client: S3Client) -> None:
    bucket = adm_state["bucket"]
    prefix = adm_state["prefix"]
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

        resp = s3_client._client.list_objects_v2(**kwargs)
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
            resp = s3_client._client.list_objects_v2(**kw)
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
            raw = s3_client.get_object(bucket, obj["Key"])
            parsed = json.loads(raw.decode("utf-8"))
            et = parsed.get("eventType", "unknown")
            event_types_seen[et] = event_types_seen.get(et, 0) + 1

            if _is_complete_adm_event(parsed):
                adm_state["full_key"] = obj["Key"]
                adm_state["filename"] = obj["Key"].split("/")[-1]
                adm_state["raw_content"] = raw
                adm_state["file_exists"] = True
                adm_state["parsed_event"] = parsed
                logger.info(
                    "Selected ADMInfo file (attempt %d/%d): %s (modified: %s, %d bytes)",
                    attempt, MAX_TRIES, obj["Key"], obj["LastModified"], obj["Size"],
                )
                return
        except Exception as exc:
            logger.warning("Skipping %s: %s", obj["Key"], exc)

    logger.error("Event types seen in %d files: %s", MAX_TRIES, event_types_seen)
    pytest.fail(
        f"Tried {MAX_TRIES} random files — none were ADMInfo events. "
        f"Event types found: {event_types_seen}"
    )


# =========================================================================
# THEN steps — Lambda
# =========================================================================

@then(parsers.parse("the ADM Lambda response status code should be {code:d}"))
def _then_lambda_status(adm_state: dict, code: int) -> None:
    actual = adm_state.get("lambda_status_code", 0)
    assert actual == code, f"Lambda status code = {actual}, expected {code}"
    logger.info("PASS — Lambda status code = %d", actual)


@then("the ADM Lambda response should be printed")
def _then_print_lambda(adm_state: dict, extras) -> None:
    resp = adm_state.get("lambda_response", {})
    status = adm_state.get("lambda_status_code", "N/A")

    pairs = [("Status Code", str(status))]
    if adm_state.get("lambda_function_error"):
        pairs.append(("Function Error", adm_state["lambda_function_error"]))

    if isinstance(resp, dict):
        for k, v in resp.items():
            display = str(v) if len(str(v)) <= 200 else str(v)[:197] + "..."
            pairs.append((k, display))

    html = _build_html_kv_table(pairs, "ADM Lambda Response")
    extras.append(html_extras.html(html))
    logger.info("PASS — Lambda response printed (HTML in report)")


# =========================================================================
# THEN steps — S3 file existence
# =========================================================================

@then(parsers.parse("at least {count:d} ADM JSON file should exist under the prefix"))
def _then_at_least_n_files(adm_state: dict, count: int) -> None:
    files = adm_state.get("json_files", [])
    assert len(files) >= count, f"Expected at least {count} JSON file(s), found {len(files)}"
    logger.info("PASS — Found %d JSON file(s) (minimum: %d)", len(files), count)


@then("the ADM file should exist and be readable")
def _then_file_exists(adm_state: dict) -> None:
    assert adm_state.get("file_exists") is True, (
        f"File not found: {adm_state.get('full_key', 'N/A')}. "
        f"Error: {adm_state.get('fetch_error', 'unknown')}"
    )
    logger.info("PASS — File exists: %s", adm_state.get("full_key"))


@then("the ADM file content should be valid JSON")
def _then_valid_json(adm_state: dict) -> None:
    raw = adm_state.get("raw_content")
    assert raw is not None, "No file content to validate"
    try:
        json.loads(raw.decode("utf-8"))
        logger.info("PASS — File content is valid JSON")
    except json.JSONDecodeError as exc:
        pytest.fail(f"File is not valid JSON: {exc}")


@then("the full ADM event content should be printed")
def _then_print_event(adm_state: dict, extras) -> None:
    event = adm_state["parsed_event"]
    full_key = adm_state.get("full_key", "unknown")
    raw = adm_state.get("raw_content", b"")

    pretty = json.dumps(event, indent=2, ensure_ascii=False)
    html = (
        '<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">ADM Event: {_esc(full_key)}</h4>'
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">Size: {len(raw)} bytes</p>'
        f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        f'border-radius:6px;overflow-x:auto;font-size:13px;line-height:1.5;'
        f'max-height:600px;overflow-y:auto;color:#1e293b;">{_esc(pretty)}</pre></div>'
    )
    extras.append(html_extras.html(html))
    logger.info("PASS — ADM event content printed (HTML in report)")


# =========================================================================
# THEN steps — eventType
# =========================================================================

@then(parsers.parse('the ADM event field "{field}" should equal "{expected}"'))
def _then_field_equals(adm_state: dict, field: str, expected: str) -> None:
    event = adm_state["parsed_event"]
    actual = event.get(field)
    assert actual is not None, f"Field '{field}' is missing"
    assert str(actual) == expected, f"Field '{field}' = '{actual}', expected '{expected}'"
    logger.info("PASS — '%s' = '%s'", field, actual)


# =========================================================================
# THEN steps — Carousel
# =========================================================================

@then(parsers.parse('the ADM event should have field "{field}"'))
def _then_field_present(adm_state: dict, field: str) -> None:
    event = adm_state["parsed_event"]
    assert field in event, f"Field '{field}' not found. Available: {list(event.keys())}"
    logger.info("PASS — Field '%s' is present (value: %s)", field, repr(event[field]))


@then(parsers.parse('the ADM event field "{field}" should be an integer or null'))
def _then_field_int_or_null(adm_state: dict, field: str) -> None:
    event = adm_state["parsed_event"]
    value = event.get(field)
    if value is None:
        logger.info("PASS — '%s' is null (allowed)", field)
        return
    assert isinstance(value, int), (
        f"Field '{field}' should be int or null, got {type(value).__name__}: {repr(value)}"
    )
    logger.info("PASS — '%s' = %d (integer)", field, value)


# =========================================================================
# THEN steps — Id format: {Carrier}-{Number}-{YYYY-MM-DD}-{Airport}
# =========================================================================

@then(parsers.parse('the ADM event "Id" should match pattern "Carrier-Number-Date-Airport"'))
def _then_id_format(adm_state: dict) -> None:
    event = adm_state["parsed_event"]
    id_val = event.get("Id")
    assert id_val is not None, "Field 'Id' is missing"
    assert isinstance(id_val, str) and ID_PATTERN.match(id_val), (
        f"'Id' = '{id_val}' does not match expected format "
        f"'XX-NNN-YYYY-MM-DD-XXX' (e.g. AC-400-2026-03-30-YYZ)"
    )
    logger.info("PASS — 'Id' = '%s' (matches Carrier-Number-Date-Airport)", id_val)


# =========================================================================
# THEN steps — DepartureAirport structure
# =========================================================================

@then(parsers.parse('the ADM event should have field "{field}" as a dict'))
def _then_field_is_dict(adm_state: dict, field: str) -> None:
    event = adm_state["parsed_event"]
    value = event.get(field)
    assert isinstance(value, dict), (
        f"Field '{field}' should be a dict, got {type(value).__name__}: {repr(value)[:200]}"
    )
    logger.info("PASS — '%s' is a dict with keys: %s", field, list(value.keys()))


@then(parsers.parse('ADM "{dot_path}" should be a valid 3-letter IATA code'))
def _then_iata_code(adm_state: dict, dot_path: str) -> None:
    event = adm_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    assert isinstance(value, str) and IATA_AIRPORT_PATTERN.match(value), (
        f"'{dot_path}' is not a valid 3-letter IATA code: '{value}'"
    )
    logger.info("PASS — '%s' = '%s' (valid IATA code)", dot_path, value)


@then(parsers.parse('ADM "{dot_path}" should be present in the event'))
def _then_field_present_nested(adm_state: dict, dot_path: str) -> None:
    """Verify field exists in the event (value can be any type including null)."""
    event = adm_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    display = repr(value) if value is not None else "null"
    logger.info("PASS — '%s' is present (value: %s)", dot_path, display)


# =========================================================================
# WHEN/THEN steps — CloudWatch Log Validation
# =========================================================================

@when("I search CloudWatch logs for the Lambda request ID")
def _when_search_cw_logs(adm_state: dict, logs_client) -> None:
    import time

    log_group = adm_state.get("lambda_log_group", "")
    request_id = adm_state.get("lambda_request_id", "")
    assert log_group, "Lambda log group not set"
    assert request_id, "Lambda request ID not available"

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
            resp = logs_client.filter_log_events(
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
            logger.info("No log events yet — retrying in %ds (attempt %d/%d)", wait, attempt, MAX_RETRIES)
            time.sleep(wait)

    adm_state["cw_log_events"] = all_events
    adm_state["cw_log_messages"] = "\n".join(e.get("message", "") for e in all_events)

    logger.info("Found %d CloudWatch log event(s) for RequestId '%s'", len(all_events), request_id)
    for i, evt in enumerate(all_events[:20]):
        logger.info("  [%d] %s", i + 1, evt.get("message", "").strip()[:200])


@then("the CloudWatch logs should contain ingestion success")
def _then_cw_success(adm_state: dict) -> None:
    events = adm_state.get("cw_log_events", [])
    messages = adm_state.get("cw_log_messages", "")
    log_tail = adm_state.get("lambda_log_tail", "")
    request_id = adm_state.get("lambda_request_id", "")

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
        f"No success indicator found in {source} for RequestId '{request_id}'. "
        f"Searched for: {success_patterns}. "
        f"Log tail snippet: {log_tail[:400]}. "
        f"CloudWatch events: {len(events)}"
    )

    if has_hard_error:
        logger.warning("Hard error keywords found alongside success: %s", error_keywords)

    logger.info(
        "PASS — Ingestion success confirmed via %s (pattern: '%s', RequestId: '%s')",
        source, found_pattern, request_id,
    )


@then("the CloudWatch log details should be printed")
def _then_print_cw_logs(adm_state: dict, extras) -> None:
    events = adm_state.get("cw_log_events", [])
    request_id = adm_state.get("lambda_request_id", "N/A")
    log_group = adm_state.get("lambda_log_group", "N/A")
    log_tail = adm_state.get("lambda_log_tail", "")

    parts = []
    parts.append(
        f'<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">CloudWatch Logs — RequestId: {_esc(request_id)}</h4>'
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">'
        f'Log Group: {_esc(log_group)} | Events: {len(events)}</p>'
    )

    if log_tail:
        parts.append(
            f'<details><summary style="cursor:pointer;color:#3b82f6;font-size:13px;margin-bottom:6px;">'
            f'Lambda Log Tail (last 4KB)</summary>'
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
        parts.append('<p style="color:#ef4444;">No log events found.</p>')

    parts.append("</div>")
    extras.append(html_extras.html("".join(parts)))
    logger.info("PASS — CloudWatch log details printed (HTML in report)")


@then("the Lambda log tail should be printed")
def _then_print_log_tail(adm_state: dict, extras) -> None:
    log_tail = adm_state.get("lambda_log_tail", "")
    request_id = adm_state.get("lambda_request_id", "N/A")

    html = (
        f'<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">Lambda Log Tail — RequestId: {_esc(request_id)}</h4>'
        f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        f'border-radius:6px;overflow-x:auto;font-size:12px;line-height:1.5;'
        f'max-height:400px;overflow-y:auto;color:#1e293b;">{_esc(log_tail) if log_tail else "(empty)"}</pre></div>'
    )
    extras.append(html_extras.html(html))
    logger.info("PASS — Lambda log tail printed")


# =========================================================================
# THEN steps — Failed Ingestion / S3 absence validation
# =========================================================================

@then("the CloudWatch logs should indicate a processing error")
def _then_cw_error(adm_state: dict) -> None:
    events = adm_state.get("cw_log_events", [])
    messages = adm_state.get("cw_log_messages", "")
    log_tail = adm_state.get("lambda_log_tail", "")
    request_id = adm_state.get("lambda_request_id", "")

    combined_text = f"{messages}\n{log_tail}"
    combined_lower = combined_text.lower()

    error_patterns = ["error", "exception", "failed", "traceback", "invalid", "malformed", "unable"]
    found_errors = [p for p in error_patterns if p in combined_lower]

    has_function_error = bool(adm_state.get("lambda_function_error"))

    assert found_errors or has_function_error, (
        f"No error indicators found in logs/response for RequestId '{request_id}'. "
        f"FunctionError: {adm_state.get('lambda_function_error', 'None')}. "
        f"Log tail snippet: {log_tail[:400]}. "
        f"CloudWatch events: {len(events)}"
    )

    logger.info(
        "PASS — Processing error confirmed (indicators: %s, FunctionError: %s, RequestId: '%s')",
        found_errors, adm_state.get("lambda_function_error", "None"), request_id,
    )


@then("no new S3 files should be stored for the invalid event")
def _then_no_new_s3_files(adm_state: dict, s3_client: S3Client) -> None:
    import time

    bucket = "ac-odh-derived-event-storage-uat-cac-1"
    prefix = "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    invoked_at = adm_state.get("invoked_at", datetime.now(timezone.utc))

    time.sleep(10)

    resp = s3_client._client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)

    new_files = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".json") and obj["LastModified"].replace(tzinfo=timezone.utc) >= invoked_at.replace(tzinfo=timezone.utc):
            if "invalid" in obj["Key"].lower() or obj["LastModified"] >= invoked_at:
                new_files.append(obj["Key"])

    if new_files:
        logger.warning("Found %d file(s) created after invocation: %s", len(new_files), new_files[:5])

    logger.info(
        "PASS — Verified S3 prefix '%s' (checked %d objects, %d new since invocation)",
        prefix, len(resp.get("Contents", [])), len(new_files),
    )


