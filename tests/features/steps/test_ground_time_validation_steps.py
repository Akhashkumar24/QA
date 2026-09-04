"""Step definitions for validating the Ground Time Glue job and S3 output.

Glue job validation targets AC-Digital-BAT  (profile: AC_Digital_BAT).
S3 output validation targets AC-DATA-ODH-UAT (profile: ODH_UAT).

Profile mapping
---------------
Override via environment variables if the default profile names differ:
    GLUE_AWS_PROFILE  – profile for Glue API calls  (default: AC_Digital_BAT)
    S3_AWS_PROFILE    – profile for S3 API calls     (default: ODH_UAT)
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import socket
import subprocess
import time
from datetime import datetime, timezone

import boto3
import mysql.connector
import pytest
from pytest_bdd import given, when, then, scenarios, parsers
from sshtunnel import SSHTunnelForwarder

from pytest_html import extras as html_extras

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings

scenarios("ground_time_validation.feature")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTML report helpers
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _build_html_table(columns: list, rows: list, title: str = "", subtitle: str = "") -> str:
    """Build a styled HTML table for the pytest-html report."""
    h = []
    h.append('<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">')

    if title:
        h.append(f'<h4 style="margin:8px 0 4px;color:#1e293b;">{_esc(title)}</h4>')
    if subtitle:
        h.append(
            f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">'
            f'{_esc(subtitle)}</p>'
        )

    h.append('<div style="overflow-x:auto;max-height:600px;overflow-y:auto;">')
    h.append(
        '<table style="border-collapse:collapse;width:100%;font-size:13px;'
        'font-family:\'Segoe UI\',Arial,sans-serif;">'
    )

    h.append('<thead><tr>')
    for col in columns:
        h.append(
            f'<th style="border:1px solid #cbd5e1;padding:8px 12px;'
            f'background:#3b82f6;color:#fff;text-align:left;'
            f'white-space:nowrap;font-weight:600;position:sticky;top:0;">'
            f'{_esc(str(col))}</th>'
        )
    h.append('</tr></thead><tbody>')

    display_rows = rows[:100]
    for i, row in enumerate(display_rows):
        bg = "#f1f5f9" if i % 2 == 1 else "#ffffff"
        h.append(f'<tr style="background:{bg};">')
        for col in columns:
            val = row.get(col) if isinstance(row, dict) else row
            if val is None:
                cell = '<span style="color:#94a3b8;font-style:italic;">NULL</span>'
            else:
                cell = _esc(str(val))
            h.append(
                f'<td style="border:1px solid #e2e8f0;padding:6px 12px;'
                f'white-space:nowrap;">{cell}</td>'
            )
        h.append('</tr>')

    if len(rows) > 100:
        h.append(
            f'<tr><td colspan="{len(columns)}" style="padding:8px;'
            f'text-align:center;color:#64748b;font-style:italic;">'
            f'... {len(rows) - 100} more rows not shown ...</td></tr>'
        )

    h.append('</tbody></table></div>')
    h.append(
        f'<p style="margin:4px 0 0;color:#64748b;font-size:12px;">'
        f'Total rows: {len(rows)}</p>'
    )
    h.append('</div>')
    return "".join(h)


def _build_html_kv_table(pairs: list[tuple[str, str]], title: str = "") -> str:
    """Build a key-value HTML table for the report."""
    h = []
    h.append('<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">')
    if title:
        h.append(f'<h4 style="margin:8px 0 4px;color:#1e293b;">{_esc(title)}</h4>')

    h.append(
        '<table style="border-collapse:collapse;font-size:13px;'
        'font-family:\'Segoe UI\',Arial,sans-serif;">'
    )
    for i, (key, value) in enumerate(pairs):
        bg = "#f1f5f9" if i % 2 == 1 else "#ffffff"
        h.append(
            f'<tr style="background:{bg};">'
            f'<td style="border:1px solid #e2e8f0;padding:6px 12px;'
            f'font-weight:600;white-space:nowrap;color:#334155;">{_esc(key)}</td>'
            f'<td style="border:1px solid #e2e8f0;padding:6px 12px;'
            f'font-family:monospace;color:#1e293b;">{_esc(str(value))}</td>'
            f'</tr>'
        )
    h.append('</table></div>')
    return "".join(h)

# ---------------------------------------------------------------------------
# AWS profile mapping — one per target account
# ---------------------------------------------------------------------------
GLUE_AWS_PROFILE = os.getenv("GLUE_AWS_PROFILE", "AC_Digital_BAT")
S3_AWS_PROFILE = os.getenv("S3_AWS_PROFILE", "ODH_UAT")

# ---------------------------------------------------------------------------
# Digital-ODS BATCA1 database connection details (SSH tunnel + MySQL)
# ---------------------------------------------------------------------------
DB_SSH_HOST = os.getenv("DB_SSH_HOST", "127.0.0.1")
DB_SSH_PORT = int(os.getenv("DB_SSH_PORT", "13408"))
DB_SSH_USERNAME = os.getenv("DB_SSH_USERNAME", "ec2devuser")
DB_SSH_PASSWORD = os.getenv("DB_SSH_PASSWORD", "gehe5KV9noScWoUOmuYl")
DB_MYSQL_HOST = os.getenv(
    "DB_MYSQL_HOST",
    "digital-ods-infra-rds-batca1-cluster.cluster-cvzclik0t3ie.ca-central-1.rds.amazonaws.com",
)
DB_MYSQL_PORT = int(os.getenv("DB_MYSQL_PORT", "3306"))
DB_MYSQL_USERNAME = os.getenv("DB_MYSQL_USERNAME", "dbdevuser")
DB_MYSQL_PASSWORD = os.getenv("DB_MYSQL_PASSWORD", r"aLWU~4]+ATj6+^!K")
DB_MYSQL_DATABASE = os.getenv("DB_MYSQL_DATABASE", "FlightScheduleDataStore")

# SSM port forwarding — auto-start when local SSH port is unreachable
SSM_INSTANCE_ID = os.getenv("SSM_INSTANCE_ID", "i-016a4569690b162b8")
SSM_REGION = os.getenv("SSM_REGION", "ca-central-1")
SSM_PROFILE = os.getenv("SSM_PROFILE", GLUE_AWS_PROFILE)

_ssm_process: subprocess.Popen | None = None


def _is_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return True if a TCP connection to *host:port* succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def _ensure_ssm_port_forwarding() -> None:
    """Start an AWS SSM port-forwarding session if the local SSH port is not
    reachable.  The session runs as a background subprocess and is reused for
    subsequent test scenarios."""
    global _ssm_process

    if _is_port_open(DB_SSH_HOST, DB_SSH_PORT):
        logger.info(
            "Port %s:%d is already open — SSM forwarding not needed",
            DB_SSH_HOST, DB_SSH_PORT,
        )
        return

    if _ssm_process is not None and _ssm_process.poll() is None:
        logger.info("SSM process (pid %d) is still running — waiting for port …", _ssm_process.pid)
    else:
        logger.info(
            "Port %s:%d is not reachable — starting SSM port forwarding "
            "(instance %s, profile %s) …",
            DB_SSH_HOST, DB_SSH_PORT, SSM_INSTANCE_ID, SSM_PROFILE,
        )
        cmd = [
            "aws", "ssm", "start-session",
            "--target", SSM_INSTANCE_ID,
            "--document-name", "AWS-StartPortForwardingSession",
            "--parameters", f"portNumber=22,localPortNumber={DB_SSH_PORT}",
            "--profile", SSM_PROFILE,
            "--region", SSM_REGION,
        ]
        _ssm_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        logger.info("SSM session started (pid %d)", _ssm_process.pid)

    max_wait, interval = 30, 2
    elapsed = 0
    while elapsed < max_wait:
        if _is_port_open(DB_SSH_HOST, DB_SSH_PORT):
            logger.info(
                "Port %s:%d is now open (waited %ds)",
                DB_SSH_HOST, DB_SSH_PORT, elapsed,
            )
            return
        time.sleep(interval)
        elapsed += interval

    pytest.fail(
        f"SSM port forwarding started but port {DB_SSH_HOST}:{DB_SSH_PORT} "
        f"did not become reachable within {max_wait}s"
    )


# ---------------------------------------------------------------------------
# Regex patterns for datatype validation
# ---------------------------------------------------------------------------
ISO8601_UTC_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$"
)
TIME_FORMAT_PATTERN = re.compile(r"^\d{2}:\d{2}:\d{2}$")
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z]{2}$")
IATA_AIRPORT_PATTERN = re.compile(r"^[A-Z]{3}$")

THIRTY_MIN_PATTERNS = [
    re.compile(r"rate\s*\(\s*30\s+minutes?\s*\)", re.IGNORECASE),
    re.compile(r"cron\s*\(\s*[0-9,/]+\s+[0-9,/]+\s+\*", re.IGNORECASE),
]

TYPE_MAP = {"string": str, "dict": dict, "list": list, "int": int, "float": float}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_nested(data: dict, dot_path: str):
    """Navigate a nested dict using dot-notation, e.g. 'Flight.OperatingCarrier.Code'."""
    current = data
    for key in dot_path.split("."):
        if not isinstance(current, dict) or key not in current:
            return None, False
        current = current[key]
    return current, True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def gt_state() -> dict:
    """Mutable dict that carries state between Given/When/Then steps."""
    return {}


@pytest.fixture()
def glue_client():
    """Boto3 Glue client using the AC-Digital-BAT profile."""
    profile = GLUE_AWS_PROFILE
    logger.info("Glue client using profile: %s", profile)
    session = boto3.Session(profile_name=profile, region_name="ca-central-1")
    return session.client("glue")


@pytest.fixture()
def s3_client() -> S3Client:
    """S3Client using the AC-DATA-ODH-UAT profile."""
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
    parsers.parse('the Glue job "{job_name}" in region "{region}"'),
    target_fixture="gt_state",
)
def _given_glue_job(gt_state: dict, job_name: str, region: str) -> dict:
    gt_state["glue_job_name"] = job_name
    gt_state["region"] = region
    logger.info("Glue job: %s  Region: %s", job_name, region)
    return gt_state


@given(
    parsers.parse('the S3 bucket "{bucket}" in region "{region}"'),
    target_fixture="gt_state",
)
def _given_s3_bucket(gt_state: dict, bucket: str, region: str) -> dict:
    gt_state["bucket"] = bucket
    gt_state["region"] = region
    logger.info("S3 bucket: %s  Region: %s", bucket, region)
    return gt_state


@given(parsers.parse('the S3 key prefix "{prefix}"'))
def _given_s3_prefix(gt_state: dict, prefix: str) -> None:
    gt_state["prefix"] = prefix
    logger.info("S3 prefix: %s", prefix)


# =========================================================================
# WHEN steps — Glue
# =========================================================================

@when("I retrieve the Glue job configuration")
def _when_get_glue_config(gt_state: dict, glue_client) -> None:
    job_name = gt_state["glue_job_name"]
    try:
        resp = glue_client.get_job(JobName=job_name)
        gt_state["glue_job_config"] = resp.get("Job", {})
        gt_state["glue_job_exists"] = True
        logger.info("Retrieved Glue job config for '%s'", job_name)
    except glue_client.exceptions.EntityNotFoundException:
        gt_state["glue_job_config"] = {}
        gt_state["glue_job_exists"] = False
        logger.error("Glue job '%s' not found", job_name)
    except Exception as exc:
        gt_state["glue_job_config"] = {}
        gt_state["glue_job_exists"] = False
        gt_state["glue_error"] = str(exc)
        logger.error("Error retrieving Glue job '%s': %s", job_name, exc)


@when("I retrieve the triggers for the Glue job")
def _when_get_triggers(gt_state: dict, glue_client) -> None:
    job_name = gt_state["glue_job_name"]
    logger.info("Searching for triggers associated with Glue job '%s'...", job_name)

    matching_triggers: list[dict] = []
    try:
        paginator = glue_client.get_paginator("get_triggers")
        for page in paginator.paginate():
            for trigger in page.get("Triggers", []):
                actions = trigger.get("Actions", [])
                for action in actions:
                    if action.get("JobName") == job_name:
                        matching_triggers.append(trigger)
                        break
    except Exception:
        try:
            resp = glue_client.get_triggers(MaxResults=200)
            for trigger in resp.get("Triggers", []):
                actions = trigger.get("Actions", [])
                for action in actions:
                    if action.get("JobName") == job_name:
                        matching_triggers.append(trigger)
                        break
        except Exception as exc:
            logger.warning(
                "get_triggers failed (%s), falling back to list_triggers", exc
            )
            next_token = None
            while True:
                list_kwargs: dict = {"MaxResults": 200}
                if next_token:
                    list_kwargs["NextToken"] = next_token
                resp = glue_client.list_triggers(**list_kwargs)
                for trigger_name in resp.get("TriggerNames", []):
                    try:
                        t_resp = glue_client.get_trigger(Name=trigger_name)
                        trigger = t_resp.get("Trigger", {})
                        actions = trigger.get("Actions", [])
                        for action in actions:
                            if action.get("JobName") == job_name:
                                matching_triggers.append(trigger)
                                break
                    except Exception as t_exc:
                        logger.warning(
                            "Could not get trigger '%s': %s", trigger_name, t_exc
                        )
                next_token = resp.get("NextToken")
                if not next_token:
                    break

    gt_state["triggers"] = matching_triggers
    logger.info(
        "Found %d trigger(s) for Glue job '%s'", len(matching_triggers), job_name
    )
    for t in matching_triggers:
        logger.info(
            "  Trigger: %s | Type: %s | Schedule: %s | State: %s",
            t.get("Name", "N/A"),
            t.get("Type", "N/A"),
            t.get("Schedule", "N/A"),
            t.get("State", "N/A"),
        )


# =========================================================================
# WHEN steps — S3
# =========================================================================

@when("I list JSON files under the prefix")
def _when_list_json_files(gt_state: dict, s3_client: S3Client) -> None:
    bucket = gt_state["bucket"]
    prefix = gt_state["prefix"]

    resp = s3_client._client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, MaxKeys=10,
    )
    files = [
        obj for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]

    gt_state["json_files"] = files
    gt_state["prefix_has_more"] = resp.get("IsTruncated", False)
    total_hint = f"10+ (truncated)" if resp.get("IsTruncated") else str(len(files))
    logger.info(
        "Found %s JSON file(s) under '%s' in bucket '%s'",
        total_hint, prefix, bucket,
    )


def _is_complete_event(data: dict) -> bool:
    """Return True if the event has all required fields populated."""
    if not isinstance(data.get("InboundFlight"), dict):
        return False
    if not isinstance(data.get("Flight"), dict):
        return False
    for field in ("ScheduledGroundTime", "MinimumGroundTime", "GoTime"):
        if not data.get(field):
            return False
    flight = data["Flight"]
    arr_code, found = _resolve_nested(flight, "ArrivalAirport.IATACode")
    if not found or not arr_code:
        return False
    return True


@when("I fetch a random current-date JSON file with complete data from the prefix")
def _when_fetch_random_today(gt_state: dict, s3_client: S3Client) -> None:
    bucket = gt_state["bucket"]
    prefix = gt_state["prefix"]
    today = datetime.now(timezone.utc).date()

    MAX_SCAN = 2000
    MAX_CANDIDATES = 200
    candidates: list[dict] = []
    scanned = 0
    continuation_token = None

    while scanned < MAX_SCAN and len(candidates) < MAX_CANDIDATES:
        kwargs: dict = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client._client.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".json"):
                scanned += 1
                if obj["LastModified"].date() == today:
                    candidates.append(obj)
                    if len(candidates) >= MAX_CANDIDATES:
                        break

        if not resp.get("IsTruncated"):
            break
        continuation_token = resp.get("NextContinuationToken")

    logger.info(
        "Scanned %d object(s), found %d from today (%s)",
        scanned, len(candidates), today.isoformat(),
    )

    assert len(candidates) > 0, (
        f"No JSON files from today ({today}) found under '{prefix}' in '{bucket}'"
    )

    random.shuffle(candidates)

    MAX_TRIES = min(20, len(candidates))
    for attempt, obj in enumerate(candidates[:MAX_TRIES], 1):
        try:
            raw = s3_client.get_object(bucket, obj["Key"])
            parsed = json.loads(raw.decode("utf-8"))
            if _is_complete_event(parsed):
                gt_state["full_key"] = obj["Key"]
                gt_state["filename"] = obj["Key"].split("/")[-1]
                gt_state["raw_content"] = raw
                gt_state["file_exists"] = True
                gt_state["parsed_event"] = parsed
                logger.info(
                    "Selected file (attempt %d/%d): %s (modified: %s, %d bytes)",
                    attempt, MAX_TRIES, obj["Key"],
                    obj["LastModified"], obj["Size"],
                )
                return
        except Exception as exc:
            logger.warning("Skipping %s: %s", obj["Key"], exc)

    pytest.fail(
        f"Tried {MAX_TRIES} random files from today — none had complete data "
        f"(InboundFlight, ground times, etc.)"
    )


# =========================================================================
# THEN steps — Glue job existence
# =========================================================================

@then("the Glue job should exist")
def _then_glue_job_exists(gt_state: dict) -> None:
    assert gt_state.get("glue_job_exists") is True, (
        f"Glue job '{gt_state['glue_job_name']}' does not exist. "
        f"Error: {gt_state.get('glue_error', 'EntityNotFoundException')}"
    )
    logger.info("PASS — Glue job '%s' exists", gt_state["glue_job_name"])


@then("the Glue job details should be printed")
def _then_print_glue_job(gt_state: dict, extras) -> None:
    config = gt_state.get("glue_job_config", {})
    job_name = gt_state["glue_job_name"]
    command = config.get("Command", {})

    pairs = [
        ("Name", config.get("Name", "N/A")),
        ("Role", config.get("Role", "N/A")),
        ("Glue Version", config.get("GlueVersion", "N/A")),
        ("Max Retries", str(config.get("MaxRetries", "N/A"))),
        ("Timeout (min)", str(config.get("Timeout", "N/A"))),
        ("Max Capacity", str(config.get("MaxCapacity", "N/A"))),
        ("Number Of Workers", str(config.get("NumberOfWorkers", "N/A"))),
        ("Worker Type", config.get("WorkerType", "N/A")),
        ("Script Location", command.get("ScriptLocation", "N/A")),
        ("Python Version", command.get("PythonVersion", "N/A")),
        ("Command Name", command.get("Name", "N/A")),
    ]

    default_args = config.get("DefaultArguments", {})
    for k, v in default_args.items():
        pairs.append((f"Arg: {k}", str(v)))

    html = _build_html_kv_table(pairs, f"Glue Job: {job_name}")
    extras.append(html_extras.html(html))

    logger.info("PASS — Glue job details printed (HTML table in report)")


# =========================================================================
# THEN steps — Trigger / Schedule validation
# =========================================================================

@then(parsers.parse("at least {count:d} trigger should be associated with the Glue job"))
def _then_at_least_n_triggers(gt_state: dict, count: int) -> None:
    triggers = gt_state.get("triggers", [])
    assert len(triggers) >= count, (
        f"Expected at least {count} trigger(s) for Glue job "
        f"'{gt_state['glue_job_name']}', found {len(triggers)}"
    )
    logger.info(
        "PASS — Found %d trigger(s) (minimum: %d)", len(triggers), count
    )


@then("the trigger schedule should be every 30 minutes")
def _then_trigger_every_30_min(gt_state: dict) -> None:
    triggers = gt_state.get("triggers", [])
    assert len(triggers) > 0, "No triggers found to validate schedule"

    schedule_found = False
    for trigger in triggers:
        schedule = trigger.get("Schedule", "")
        trigger_type = trigger.get("Type", "")
        name = trigger.get("Name", "N/A")

        logger.info(
            "Checking trigger '%s' — Type: %s, Schedule: '%s'",
            name, trigger_type, schedule,
        )

        if trigger_type == "SCHEDULED" and schedule:
            for pattern in THIRTY_MIN_PATTERNS:
                if pattern.search(schedule):
                    schedule_found = True
                    logger.info(
                        "MATCH — Trigger '%s' schedule '%s' matches 30-minute pattern",
                        name, schedule,
                    )
                    break

            if not schedule_found and "cron" in schedule.lower():
                cron_match = re.search(r"cron\((.+)\)", schedule)
                if cron_match:
                    cron_parts = cron_match.group(1).split()
                    if len(cron_parts) >= 2:
                        minute_field = cron_parts[0]
                        if minute_field in ("*/30", "0/30"):
                            schedule_found = True
                            logger.info(
                                "MATCH — Trigger '%s' cron minute field '%s' "
                                "indicates every 30 minutes",
                                name, minute_field,
                            )

    assert schedule_found, (
        f"No trigger with a 30-minute schedule found for Glue job "
        f"'{gt_state['glue_job_name']}'. "
        f"Triggers: {json.dumps([{'Name': t.get('Name'), 'Type': t.get('Type'), 'Schedule': t.get('Schedule')} for t in triggers], indent=2)}"
    )
    logger.info("PASS — Glue job has a trigger running every 30 minutes")


@then(parsers.parse('the trigger should be in "{expected_state}" state'))
def _then_trigger_state(gt_state: dict, expected_state: str) -> None:
    triggers = gt_state.get("triggers", [])
    assert len(triggers) > 0, "No triggers found to validate state"

    active_found = False
    for trigger in triggers:
        state = trigger.get("State", "")
        name = trigger.get("Name", "N/A")
        logger.info("Trigger '%s' state: %s", name, state)
        if state == expected_state:
            active_found = True

    assert active_found, (
        f"No trigger in '{expected_state}' state found for Glue job "
        f"'{gt_state['glue_job_name']}'. "
        f"Trigger states: {[{'Name': t.get('Name'), 'State': t.get('State')} for t in triggers]}"
    )
    logger.info("PASS — Trigger is in '%s' state", expected_state)


# =========================================================================
# THEN steps — S3 file existence
# =========================================================================

@then(parsers.parse("at least {count:d} JSON file should exist under the prefix"))
def _then_at_least_n_json_files(gt_state: dict, count: int) -> None:
    files = gt_state.get("json_files", [])
    assert len(files) >= count, (
        f"Expected at least {count} JSON file(s) under "
        f"'{gt_state.get('prefix', '')}', found {len(files)}"
    )
    logger.info("PASS — Found %d JSON file(s) (minimum: %d)", len(files), count)


@then("the file should exist and be readable")
def _then_file_exists(gt_state: dict) -> None:
    assert gt_state.get("file_exists") is True, (
        f"File not found: {gt_state.get('full_key', 'N/A')}. "
        f"Error: {gt_state.get('fetch_error', 'unknown')}"
    )
    logger.info("PASS — File exists and is readable: %s", gt_state.get("full_key"))


@then("the file content should be valid JSON")
def _then_valid_json(gt_state: dict) -> None:
    raw = gt_state["raw_content"]
    assert raw is not None, "No file content to validate"
    try:
        json.loads(raw.decode("utf-8"))
        logger.info("PASS — File content is valid JSON")
    except json.JSONDecodeError as exc:
        pytest.fail(f"File is not valid JSON: {exc}")


@then("the full event content should be printed")
def _then_print_event(gt_state: dict, extras) -> None:
    event = gt_state["parsed_event"]
    full_key = gt_state.get("full_key", "unknown")
    raw = gt_state.get("raw_content", b"")

    pretty = json.dumps(event, indent=2, ensure_ascii=False)

    html = (
        '<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        f'<h4 style="margin:8px 0 4px;color:#1e293b;">Event: {_esc(full_key)}</h4>'
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">'
        f'Size: {len(raw)} bytes</p>'
        f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        f'border-radius:6px;overflow-x:auto;font-size:13px;line-height:1.5;'
        f'color:#1e293b;">{_esc(pretty)}</pre></div>'
    )
    extras.append(html_extras.html(html))

    logger.info("PASS — Event content printed (HTML in report)")


# =========================================================================
# THEN steps — Top-level field presence and type
# =========================================================================

@then(parsers.parse('the event should have field "{field_name}" of type "{expected_type}"'))
def _then_event_field_type(gt_state: dict, field_name: str, expected_type: str) -> None:
    event = gt_state["parsed_event"]
    assert field_name in event, (
        f"Event missing field '{field_name}'. Available: {list(event.keys())}"
    )
    value = event[field_name]
    py_type = TYPE_MAP.get(expected_type)
    assert py_type is not None, f"Unknown expected type: '{expected_type}'"
    assert isinstance(value, py_type), (
        f"Field '{field_name}' expected type '{expected_type}', "
        f"got '{type(value).__name__}': {repr(value)[:200]}"
    )
    logger.info(
        "PASS — '%s' is present and of type '%s'", field_name, expected_type
    )


# =========================================================================
# THEN steps — Nested field datatype validation (dot-path)
# =========================================================================

@then(parsers.parse('"{dot_path}" should be a string'))
def _then_is_string(gt_state: dict, dot_path: str) -> None:
    event = gt_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    assert isinstance(value, str), (
        f"'{dot_path}' should be a string, got {type(value).__name__}: {repr(value)}"
    )
    display = value if value else "(empty)"
    logger.info("PASS — '%s' = '%s' (string)", dot_path, display)


@then(parsers.parse('"{dot_path}" should be a non-empty string'))
def _then_non_empty_string(gt_state: dict, dot_path: str) -> None:
    event = gt_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    assert isinstance(value, str) and len(value.strip()) > 0, (
        f"'{dot_path}' should be a non-empty string, got: {repr(value)}"
    )
    logger.info("PASS — '%s' = '%s' (non-empty string)", dot_path, value)


@then(parsers.parse('"{dot_path}" should be a valid 3-letter IATA code'))
def _then_iata_code(gt_state: dict, dot_path: str) -> None:
    event = gt_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    assert isinstance(value, str) and IATA_AIRPORT_PATTERN.match(value), (
        f"'{dot_path}' is not a valid 3-letter IATA code: '{value}'"
    )
    logger.info("PASS — '%s' = '%s' (valid IATA code)", dot_path, value)


@then(parsers.parse('"{dot_path}" should be a valid 2-letter country code'))
def _then_country_code(gt_state: dict, dot_path: str) -> None:
    event = gt_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    assert isinstance(value, str) and COUNTRY_CODE_PATTERN.match(value), (
        f"'{dot_path}' is not a valid 2-letter country code: '{value}'"
    )
    logger.info("PASS — '%s' = '%s' (valid country code)", dot_path, value)


@then(parsers.parse('"{dot_path}" should be a valid date in YYYY-MM-DD format'))
def _then_date_format(gt_state: dict, dot_path: str) -> None:
    event = gt_state["parsed_event"]
    value, found = _resolve_nested(event, dot_path)
    assert found, f"'{dot_path}' not found in event"
    assert isinstance(value, str) and DATE_PATTERN.match(value), (
        f"'{dot_path}' does not match YYYY-MM-DD format: '{value}'"
    )
    logger.info("PASS — '%s' = '%s' (valid YYYY-MM-DD date)", dot_path, value)


# =========================================================================
# THEN steps — Ground time field format validation
# =========================================================================

@then(parsers.parse('"{field_name}" should match HH:MM:SS time format'))
def _then_time_format(gt_state: dict, field_name: str) -> None:
    event = gt_state["parsed_event"]
    value = event.get(field_name)
    assert value is not None, f"'{field_name}' is missing or null"
    assert isinstance(value, str) and TIME_FORMAT_PATTERN.match(value), (
        f"'{field_name}' does not match HH:MM:SS format: '{value}'"
    )
    logger.info("PASS — '%s' = '%s' (valid HH:MM:SS)", field_name, value)


@then(parsers.parse('"{field_name}" should be a valid ISO 8601 UTC datetime'))
def _then_iso8601(gt_state: dict, field_name: str) -> None:
    event = gt_state["parsed_event"]
    value = event.get(field_name)
    assert value is not None, f"'{field_name}' is missing or null"
    assert isinstance(value, str) and ISO8601_UTC_PATTERN.match(value), (
        f"'{field_name}' is not a valid ISO 8601 UTC datetime: '{value}'"
    )
    logger.info("PASS — '%s' = '%s' (valid ISO 8601 UTC)", field_name, value)


# =========================================================================
# THEN steps — Data consistency
# =========================================================================

@then("InboundFlight ArrivalAirport should match Flight DepartureAirport")
def _then_airports_match(gt_state: dict) -> None:
    event = gt_state["parsed_event"]

    inbound_arr, found_ib = _resolve_nested(event, "InboundFlight.ArrivalAirport.IATACode")
    assert found_ib, "InboundFlight.ArrivalAirport.IATACode not found"

    flight_dep, found_fd = _resolve_nested(event, "Flight.DepartureAirport.IATACode")
    assert found_fd, "Flight.DepartureAirport.IATACode not found"

    assert inbound_arr == flight_dep, (
        f"InboundFlight ArrivalAirport '{inbound_arr}' does not match "
        f"Flight DepartureAirport '{flight_dep}'. "
        "The inbound leg must arrive where the outbound leg departs."
    )
    logger.info(
        "PASS — InboundFlight arrival '%s' == Flight departure '%s'",
        inbound_arr, flight_dep,
    )


@then("Flight and InboundFlight should have the same FleetIdentificationNumber")
def _then_same_fleet_id(gt_state: dict) -> None:
    event = gt_state["parsed_event"]

    flight_fin, found_f = _resolve_nested(
        event, "Flight.Aircraft.FleetIdentificationNumber"
    )
    assert found_f, "Flight.Aircraft.FleetIdentificationNumber not found"

    inbound_fin, found_i = _resolve_nested(
        event, "InboundFlight.Aircraft.FleetIdentificationNumber"
    )
    assert found_i, "InboundFlight.Aircraft.FleetIdentificationNumber not found"

    assert flight_fin == inbound_fin, (
        f"Flight FIN '{flight_fin}' does not match "
        f"InboundFlight FIN '{inbound_fin}'. "
        "Both legs should reference the same aircraft."
    )
    logger.info(
        "PASS — Flight and InboundFlight FleetIdentificationNumber match: '%s'",
        flight_fin,
    )


# =========================================================================
# GIVEN/WHEN/THEN steps — MySQL Database via SSH Tunnel
# =========================================================================

@given("the Digital-ODS BATCA1 database connection details")
def _given_db_connection_details(gt_state: dict) -> None:
    gt_state["ssh_host"] = DB_SSH_HOST
    gt_state["ssh_port"] = DB_SSH_PORT
    gt_state["ssh_username"] = DB_SSH_USERNAME
    gt_state["mysql_host"] = DB_MYSQL_HOST
    gt_state["mysql_port"] = DB_MYSQL_PORT
    gt_state["mysql_username"] = DB_MYSQL_USERNAME
    gt_state["mysql_database"] = DB_MYSQL_DATABASE
    logger.info(
        "DB connection: SSH %s:%d → MySQL %s:%d / %s",
        DB_SSH_HOST, DB_SSH_PORT, DB_MYSQL_HOST, DB_MYSQL_PORT, DB_MYSQL_DATABASE,
    )


@when("I connect to the database via SSH tunnel")
def _when_connect_db(gt_state: dict) -> None:
    _ensure_ssm_port_forwarding()

    logger.info(
        "Opening SSH tunnel %s:%d → %s:%d ...",
        DB_SSH_HOST, DB_SSH_PORT, DB_MYSQL_HOST, DB_MYSQL_PORT,
    )

    try:
        tunnel = SSHTunnelForwarder(
            (DB_SSH_HOST, DB_SSH_PORT),
            ssh_username=DB_SSH_USERNAME,
            ssh_password=DB_SSH_PASSWORD,
            remote_bind_address=(DB_MYSQL_HOST, DB_MYSQL_PORT),
        )
        tunnel.start()
        gt_state["ssh_tunnel"] = tunnel
        local_port = tunnel.local_bind_port
        logger.info("SSH tunnel established — local port: %d", local_port)

        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=local_port,
            user=DB_MYSQL_USERNAME,
            password=DB_MYSQL_PASSWORD,
            database=DB_MYSQL_DATABASE,
            connection_timeout=30,
        )
        gt_state["db_connection"] = conn
        gt_state["db_connected"] = True
        server_ver = conn.server_info if hasattr(conn, "server_info") else conn.get_server_info()
        logger.info(
            "MySQL connected — server version: %s, database: %s",
            server_ver, DB_MYSQL_DATABASE,
        )

    except Exception as exc:
        gt_state["db_connected"] = False
        gt_state["db_error"] = str(exc)
        logger.error("Database connection failed: %s", exc)
        pytest.fail(f"Database connection failed: {exc}")


@when(parsers.parse('I execute the query "{query}"'))
def _when_execute_query(gt_state: dict, query: str) -> None:
    conn = gt_state.get("db_connection")
    assert conn is not None, "No database connection available"

    logger.info("Executing: %s", query)
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = cursor.column_names if hasattr(cursor, "column_names") else []
        gt_state["query_result"] = rows
        gt_state["query_columns"] = list(columns)
        gt_state["query_text"] = query
        logger.info("Query returned %d row(s), %d column(s)", len(rows), len(columns))
    finally:
        cursor.close()


@then("the database connection should be established")
def _then_db_connected(gt_state: dict) -> None:
    assert gt_state.get("db_connected") is True, (
        f"Database connection not established. Error: {gt_state.get('db_error', 'unknown')}"
    )
    logger.info("PASS — Database connection established")


@then("the connection details should be printed")
def _then_print_connection(gt_state: dict, extras) -> None:
    conn = gt_state.get("db_connection")
    tunnel = gt_state.get("ssh_tunnel")

    pairs = [
        ("SSH Host", f"{DB_SSH_HOST}:{DB_SSH_PORT}"),
        ("SSH User", DB_SSH_USERNAME),
    ]
    if tunnel:
        pairs.append(("Local Tunnel Port", str(tunnel.local_bind_port)))
    pairs.extend([
        ("MySQL Host", DB_MYSQL_HOST),
        ("MySQL Port", str(DB_MYSQL_PORT)),
        ("MySQL User", DB_MYSQL_USERNAME),
        ("Database", DB_MYSQL_DATABASE),
    ])
    if conn:
        server_ver = conn.server_info if hasattr(conn, "server_info") else conn.get_server_info()
        pairs.append(("Server Version", str(server_ver)))
        pairs.append(("Connection ID", str(conn.connection_id)))

    html = _build_html_kv_table(pairs, "Database Connection Details")
    extras.append(html_extras.html(html))

    logger.info("PASS — Connection details printed (HTML table in report)")


@then(parsers.parse("the query result should have at least {count:d} row"))
def _then_at_least_n_rows(gt_state: dict, count: int) -> None:
    rows = gt_state.get("query_result", [])
    assert len(rows) >= count, (
        f"Expected at least {count} row(s), got {len(rows)}. "
        f"Query: {gt_state.get('query_text', 'N/A')}"
    )
    logger.info("PASS — Query returned %d row(s) (minimum: %d)", len(rows), count)


@then("the query result should be printed")
def _then_print_query_result(gt_state: dict, extras) -> None:
    rows = gt_state.get("query_result", [])
    columns = gt_state.get("query_columns", [])
    query = gt_state.get("query_text", "N/A")

    logger.info("Query: %s — %d row(s), %d column(s)", query, len(rows), len(columns))

    html = _build_html_table(columns, rows, "Query Result", f"Query: {query}")
    extras.append(html_extras.html(html))

    logger.info("PASS — Query result printed (HTML table in report)")


@then(parsers.parse('the result should contain column "{column_name}"'))
def _then_result_contains_column(gt_state: dict, column_name: str) -> None:
    rows = gt_state.get("query_result", [])
    assert len(rows) > 0, "No query results to check"

    found = any(
        str(row.get("COLUMN_NAME", "")) == column_name
        for row in rows
    )
    assert found, (
        f"Column '{column_name}' not found in result. "
        f"Available columns: {[str(r.get('COLUMN_NAME', '')) for r in rows]}"
    )
    logger.info("PASS — Column '%s' exists in the table", column_name)


# =========================================================================
# THEN steps — DB-to-Event value comparison
# =========================================================================

@then(parsers.parse('DB column "{column}" should equal "{expected}"'))
def _then_db_col_equals(gt_state: dict, column: str, expected: str) -> None:
    rows = gt_state.get("query_result", [])
    assert len(rows) > 0, "No query results to validate"
    actual = str(rows[0].get(column, ""))
    assert actual == expected, (
        f"DB column '{column}' = '{actual}', expected '{expected}'"
    )
    logger.info("PASS — DB '%s' = '%s'", column, actual)


@then(parsers.parse('DB column "{column}" in minutes should equal time "{time_val}"'))
def _then_db_col_minutes_time(gt_state: dict, column: str, time_val: str) -> None:
    """Compare a DB smallint (minutes) with an event HH:MM:SS value."""
    rows = gt_state.get("query_result", [])
    assert len(rows) > 0, "No query results to validate"
    db_val = rows[0].get(column)
    assert db_val is not None, f"DB column '{column}' is NULL"

    parts = time_val.split(":")
    expected_minutes = int(parts[0]) * 60 + int(parts[1])
    assert int(db_val) == expected_minutes, (
        f"DB '{column}' = {db_val} min, expected {expected_minutes} min "
        f"(event value: '{time_val}')"
    )
    logger.info(
        "PASS — DB '%s' = %s min  ←  event '%s'", column, db_val, time_val,
    )


@then(parsers.parse('DB column "{column}" in minutes should equal UTC time "{dt_val}"'))
def _then_db_col_minutes_utc(gt_state: dict, column: str, dt_val: str) -> None:
    """Compare a DB smallint (minutes-from-midnight UTC) with an ISO 8601 datetime."""
    rows = gt_state.get("query_result", [])
    assert len(rows) > 0, "No query results to validate"
    db_val = rows[0].get(column)
    assert db_val is not None, f"DB column '{column}' is NULL"

    dt = datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
    expected_minutes = dt.hour * 60 + dt.minute
    assert int(db_val) == expected_minutes, (
        f"DB '{column}' = {db_val} min, expected {expected_minutes} min "
        f"(event value: '{dt_val}', UTC time: {dt.hour:02d}:{dt.minute:02d})"
    )
    logger.info(
        "PASS — DB '%s' = %s min  ←  event '%s' (UTC %02d:%02d)",
        column, db_val, dt_val, dt.hour, dt.minute,
    )


@pytest.fixture(autouse=True)
def _cleanup_db(gt_state: dict):
    """Close DB connection and SSH tunnel after each scenario."""
    yield
    conn = gt_state.get("db_connection")
    if conn:
        try:
            conn.close()
            logger.info("MySQL connection closed")
        except Exception:
            pass
    tunnel = gt_state.get("ssh_tunnel")
    if tunnel:
        try:
            tunnel.stop()
            logger.info("SSH tunnel closed")
        except Exception:
            pass
