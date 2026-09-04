"""CEP Flight Status — S3 flight event JSON vs documented getFlightFlattened keys (ACNP BAT).

Expected flat keys match ``data.customData.getFlightFlattened`` from the
``com.aircanada.airport_mobility.flight_status_change`` CloudEvent contract (see product docs).
S3 objects may store the inner payload only (``customData`` at top level) or the full envelope.

Environment
-----------
CEP_AWS_PROFILE    Default: CEP_ACNP_BAT
FLIGHT_EVENTS_DATE Optional YYYY-MM-DD for the ``flight-events/<date>/`` folder (latest-json scenario)
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import pytest
from botocore.exceptions import ClientError
from pytest_bdd import given, scenarios, then, when, parsers
from pytest_html import extras as html_extras

from aws_testkit.aws_clients import S3Client
from aws_testkit.config import Settings

scenarios("cep_flight_status_s3_mapping.feature")

logger = logging.getLogger(__name__)

_DEFAULT_CEP_PROFILE = "CEP_ACNP_BAT"

# Keys under getFlightFlattened from documentation (flat string keys, dotted segments).
_DOCUMENTED_GET_FLIGHT_FLATTENED_KEYS: tuple[str, ...] = (
    "airlineCode",
    "originCode",
    "destinationCode",
    "flightNumber",
    "date",
    "overallStatusCode",
    "estimatedDepartureTimeGMT",
    "flightInfo.operatingAirline.code",
    "flightInfo.operatingAirline.flightNumber",
    "flightInfo.marketingAirline.code",
    "flightInfo.marketingAirline.flightNumber",
    "flightInfo.overallStatus.status.code",
    "flightInfo.overallStatus.delayReasonsHASH",
    "flightInfo.origin.terminal",
    "flightInfo.origin.gate",
    "flightInfo.origin.statusCode",
    "flightInfo.origin.location.code",
    "flightInfo.origin.scheduledDateTime.local",
    "flightInfo.origin.scheduledDateTime.GMT",
    "flightInfo.origin.estimatedDateTime.local",
    "flightInfo.origin.estimatedDateTime.GMT",
    "flightInfo.destination.terminal",
    "flightInfo.destination.gate",
    "flightInfo.destination.statusCode",
    "flightInfo.destination.location.code",
    "flightInfo.destination.scheduledDateTime.local",
    "flightInfo.destination.scheduledDateTime.GMT",
    "flightInfo.destination.estimatedDateTime.local",
    "flightInfo.destination.estimatedDateTime.GMT",
    "flightInfo.destination.baggageCarousel",
    "flightInfo.market.code",
    "flightInfo.aircraft.aircraftCode",
    "flightInfo.aircraft.airlineCode",
    "flightInfo.aircraft.fin",
    "flightInfo.aircraft.registrationNumber",
    "flightInfo.aircraft.friendlyModelName",
)


def _cep_profile() -> str:
    return (
        os.getenv("CEP_AWS_PROFILE")
        or os.getenv("AWS_PROFILE")
        or _DEFAULT_CEP_PROFILE
    ).strip()


def _flight_events_date_override() -> str | None:
    raw = os.getenv("FLIGHT_EVENTS_DATE", "").strip()
    return raw if raw else None


@pytest.fixture()
def cep_state() -> dict:
    return {}


def _unwrap_get_flight_flattened(doc: Any) -> tuple[Mapping[str, Any] | None, str]:
    """Resolve getFlightFlattened using Mapping (not only dict) for decoded JSON objects."""
    paths: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("data.customData.getFlightFlattened", ("data", "customData", "getFlightFlattened")),
        ("customData.getFlightFlattened", ("customData", "getFlightFlattened")),
        ("getFlightFlattened", ("getFlightFlattened",)),
    )
    for label, path in paths:
        cur: Any = doc
        ok = True
        for key in path:
            if not isinstance(cur, Mapping) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok and isinstance(cur, Mapping):
            return cur, label
    return None, ""


def _esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _append_s3_event_to_report(
    extras: list,
    *,
    bucket: str,
    key: str,
    body_bytes: bytes,
    parsed: Any,
) -> None:
    """Attach full S3 event JSON to pytest-html for audit evidence."""
    pretty = json.dumps(parsed, indent=2, ensure_ascii=False, default=str)
    uri = f"s3://{bucket}/{key}"
    html = (
        '<div style="margin:12px 0;font-family:\'Segoe UI\',Arial,sans-serif;">'
        '<h4 style="margin:8px 0 4px;color:#1e293b;">CEP flight event (S3)</h4>'
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">'
        f'<code>{_esc(uri)}</code> &mdash; {len(body_bytes)} bytes</p>'
        '<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        'border-radius:6px;overflow-x:auto;font-size:12px;line-height:1.45;'
        'max-height:720px;overflow-y:auto;color:#0f172a;white-space:pre-wrap;word-break:break-word;">'
        f"{_esc(pretty)}</pre></div>"
    )
    extras.append(html_extras.html(html))


def _flattened_key_present(flat: Mapping[str, Any], key: str) -> bool:
    """Doc uses flat keys with dots; fall back to nested resolve if the publisher nests."""
    if key in flat:
        return True
    cur: Any = flat
    for part in key.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            return False
        cur = cur[part]
    return True


@pytest.fixture()
def cep_s3_client() -> S3Client:
    return S3Client(Settings(aws_profile=_cep_profile()))


@given(parsers.parse('the CEP flight events S3 bucket "{bucket}" in region "{region}"'))
def _given_cep_bucket(cep_state: dict, bucket: str, region: str) -> None:
    cep_state["bucket"] = bucket
    cep_state["region"] = region
    logger.info("CEP flight events bucket: %s  Region: %s", bucket, region)


@given("the flight events date folder is today UTC")
def _given_flight_events_today(cep_state: dict) -> None:
    day = _flight_events_date_override() or datetime.now(timezone.utc).date().isoformat()
    cep_state["flight_events_prefix"] = f"flight-events/{day}/"
    logger.info("flight-events prefix: %s", cep_state["flight_events_prefix"])


@given(parsers.parse('the CEP flight event S3 object key "{key}"'))
def _given_object_key(cep_state: dict, key: str) -> None:
    cep_state["object_key"] = key
    logger.info("CEP object key: %s", key)


@when("I fetch the latest CEP flight event JSON under the flight-events date folder")
def _when_fetch_latest_json(cep_state: dict, cep_s3_client: S3Client) -> None:
    bucket = cep_state["bucket"]
    prefix = cep_state["flight_events_prefix"]
    client = cep_s3_client._client
    candidates: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json"):
                candidates.append(obj)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    assert len(candidates) > 0, (
        f"No .json objects under s3://{bucket}/{prefix} — check date folder or FLIGHT_EVENTS_DATE"
    )
    best = max(candidates, key=lambda o: o["LastModified"])
    key = best["Key"]
    raw = cep_s3_client.get_object(bucket, key)
    cep_state["object_key"] = key
    cep_state["raw_bytes"] = raw
    cep_state["parsed_json"] = json.loads(raw.decode("utf-8"))
    logger.info(
        "Latest JSON: %s (%d bytes, modified %s)",
        key, best["Size"], best["LastModified"],
    )


@when("I fetch the CEP flight event JSON from S3")
def _when_fetch_by_key(cep_state: dict, cep_s3_client: S3Client) -> None:
    bucket = cep_state["bucket"]
    key = cep_state["object_key"]
    try:
        raw = cep_s3_client.get_object(bucket, key)
    except ClientError as exc:
        pytest.fail(f"Cannot read s3://{bucket}/{key}: {exc}")
    cep_state["raw_bytes"] = raw
    cep_state["parsed_json"] = json.loads(raw.decode("utf-8"))
    logger.info("Loaded %d bytes from s3://%s/%s", len(raw), bucket, key)


@then("all documented getFlightFlattened keys should be present in the payload")
def _then_documented_keys_present(cep_state: dict, extras: list) -> None:
    doc = cep_state.get("parsed_json")
    assert doc is not None, "Load a CEP flight event JSON first"
    bucket = str(cep_state.get("bucket", ""))
    key = str(cep_state.get("object_key", ""))
    raw_body = cep_state.get("raw_bytes") or b""
    _append_s3_event_to_report(
        extras, bucket=bucket, key=key, body_bytes=raw_body, parsed=doc
    )
    logger.info("S3 event body appended to HTML report (evidence)")

    flat, via = _unwrap_get_flight_flattened(doc)
    if flat is None:
        top = list(doc.keys()) if isinstance(doc, Mapping) else type(doc).__name__
        pytest.fail(
            "Could not resolve getFlightFlattened (tried data.customData.getFlightFlattened, "
            f"customData.getFlightFlattened, getFlightFlattened). Top-level keys: {top!r}"
        )
    logger.info("Resolved getFlightFlattened via %s", via)
    missing = [k for k in _DOCUMENTED_GET_FLIGHT_FLATTENED_KEYS if not _flattened_key_present(flat, k)]
    if missing:
        sample = json.dumps(flat, indent=2, default=str)[:4000]
        pytest.fail(
            f"Missing {len(missing)} documented key(s) in getFlightFlattened: "
            f"{missing[:30]}{'...' if len(missing) > 30 else ''}\ngetFlightFlattened sample:\n{sample}"
        )
    logger.info(
        "PASS — All %d documented getFlightFlattened keys present",
        len(_DOCUMENTED_GET_FLIGHT_FLATTENED_KEYS),
    )
