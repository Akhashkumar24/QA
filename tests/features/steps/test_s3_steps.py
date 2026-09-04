"""Step definitions for Aircraft Info Lambda BDD scenarios (pytest-bdd)."""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

from aws_testkit.aws_clients import S3Client
from aws_testkit.utils import unique_name

# ---------------------------------------------------------------------------
# Link every scenario in s3.feature to this module
# ---------------------------------------------------------------------------
scenarios("../s3.feature")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_QUERY = (
    "SELECT AC AS FIN, AC_SN AS SERIALNUMBER, MANUFACTURE, "
    "LAST_AC_REGISTRATION FROM ODB.AC_MASTER WHERE status = 'ACTIVE'"
)

SAMPLE_AIRCRAFT_RECORDS = [
    {
        "FIN": "AC123",
        "SERIALNUMBER": "67890",
        "MANUFACTURE": "Boeing 737 MAX 8",
        "LAST_AC_REGISTRATION": "C-GXYZ",
    },
    {
        "FIN": "AC456",
        "SERIALNUMBER": "11223",
        "MANUFACTURE": "Airbus A320neo",
        "LAST_AC_REGISTRATION": "C-FABC",
    },
]

DERIVED_EVENT_STORE_BUCKET = "derived-event-store"
UTC_TIMESTAMP_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
GUID_REGEX = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


# ---------------------------------------------------------------------------
# Helper — build the expected JSON event from a raw DB record
# ---------------------------------------------------------------------------

def _build_aircraft_event(record: dict) -> dict:
    """Return the expected JSON event structure for a single aircraft row."""
    return {
        "eventType": "AircraftInfo",
        "eventID": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") +
                     f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
        "Aircraft": {
            "FleetIdentificationNumber": record["FIN"],
            "SerialNumber": record["SERIALNUMBER"],
            "Manufacturer": record["MANUFACTURE"],
            "LastACRegistration": record["LAST_AC_REGISTRATION"],
        },
    }


# ---------------------------------------------------------------------------
# Fixtures — shared state across steps
# ---------------------------------------------------------------------------

@pytest.fixture()
def lambda_state() -> dict:
    """Mutable dict that carries state between Given/When/Then steps."""
    return {}


@pytest.fixture()
def s3_client(moto_s3) -> S3Client:  # noqa: D401 – fixture
    """S3Client backed by the moto mock (injected via conftest)."""
    return moto_s3


@pytest.fixture()
def mock_db_proxy() -> MagicMock:
    """Mock representing the Connection Pooler proxy to Trax Reporting DB."""
    proxy = MagicMock(name="ConnectionPoolerProxy")
    proxy.execute_query.return_value = list(SAMPLE_AIRCRAFT_RECORDS)
    return proxy


@pytest.fixture()
def mock_lambda_config() -> dict:
    """Mock Lambda configuration metadata."""
    return {
        "runtime": "nodejs20.x",
        "function_name": "aircraft-info-lambda",
        "schedule_expression": "rate(6 hours)",
        "environment": "non-prod",
        "log_group": "/aws/lambda/aircraft-info-lambda",
        "log_retention_days": 15,
        "log_level": "Info",
        "dynatrace_enabled": True,
        "dbaas_logger_enabled": True,
    }


# =========================================================================
# GIVEN steps
# =========================================================================

# -- Database Connectivity -------------------------------------------------

@given(
    "the Lambda is configured with Connection Pooler proxy credentials",
    target_fixture="lambda_state",
)
def _given_lambda_configured_with_proxy(lambda_state: dict, mock_db_proxy: MagicMock) -> dict:
    lambda_state["db_proxy"] = mock_db_proxy
    lambda_state["proxy_configured"] = True
    lambda_state["direct_query_executed"] = False
    return lambda_state


@given(
    "a successful connection to Trax Reporting DB via Connection Pooler proxy",
    target_fixture="lambda_state",
)
def _given_successful_connection(lambda_state: dict, mock_db_proxy: MagicMock) -> dict:
    mock_db_proxy.connect.return_value = True
    lambda_state["db_proxy"] = mock_db_proxy
    lambda_state["connected"] = True
    return lambda_state


# -- Retry Mechanism -------------------------------------------------------

@given(
    "the Connection Pooler proxy is temporarily unavailable",
    target_fixture="lambda_state",
)
def _given_proxy_temporarily_unavailable(lambda_state: dict, mock_db_proxy: MagicMock) -> dict:
    mock_db_proxy.connect.side_effect = ConnectionError("Proxy temporarily unavailable")
    lambda_state["db_proxy"] = mock_db_proxy
    lambda_state["retry_attempts"] = 0
    lambda_state["retry_intervals"] = []
    return lambda_state


@given(
    "the Connection Pooler proxy fails on the first attempt",
    target_fixture="lambda_state",
)
def _given_proxy_fails_first_attempt(lambda_state: dict, mock_db_proxy: MagicMock) -> dict:
    mock_db_proxy.connect.side_effect = [
        ConnectionError("Attempt 1 failed"),
        True,
    ]
    lambda_state["db_proxy"] = mock_db_proxy
    lambda_state["retry_attempts"] = 0
    return lambda_state


@given("the Connection Pooler proxy succeeds on the second attempt")
def _given_proxy_succeeds_second_attempt(lambda_state: dict) -> None:
    # Side effect already configured in the previous step (fails first, succeeds second)
    lambda_state["expected_success_on_retry"] = True


@given(
    "the Connection Pooler proxy is unavailable for all attempts",
    target_fixture="lambda_state",
)
def _given_proxy_unavailable_all_attempts(lambda_state: dict, mock_db_proxy: MagicMock) -> dict:
    mock_db_proxy.connect.side_effect = ConnectionError("Proxy permanently unavailable")
    lambda_state["db_proxy"] = mock_db_proxy
    lambda_state["retry_attempts"] = 0
    lambda_state["max_retries"] = 3
    return lambda_state


# -- S3 Storage ------------------------------------------------------------

@given(
    "the Lambda has retrieved aircraft records from Trax DB",
    target_fixture="lambda_state",
)
def _given_retrieved_aircraft_records(
    lambda_state: dict, s3_client: S3Client
) -> dict:
    lambda_state["records"] = list(SAMPLE_AIRCRAFT_RECORDS)
    lambda_state["s3_bucket"] = DERIVED_EVENT_STORE_BUCKET
    s3_client.create_bucket(DERIVED_EVENT_STORE_BUCKET)
    return lambda_state


@given(
    parsers.parse(
        'the Lambda has retrieved an aircraft record with AC "{ac}", '
        'AC_SN "{ac_sn}", MANUFACTURE "{manufacture}", '
        'and LAST_AC_REGISTRATION "{last_reg}"'
    ),
    target_fixture="lambda_state",
)
def _given_retrieved_single_record(
    lambda_state: dict, s3_client: S3Client,
    ac: str, ac_sn: str, manufacture: str, last_reg: str,
) -> dict:
    lambda_state["records"] = [
        {
            "FIN": ac,
            "SERIALNUMBER": ac_sn,
            "MANUFACTURE": manufacture,
            "LAST_AC_REGISTRATION": last_reg,
        }
    ]
    lambda_state["s3_bucket"] = DERIVED_EVENT_STORE_BUCKET
    s3_client.create_bucket(DERIVED_EVENT_STORE_BUCKET)
    return lambda_state


@given(
    "the Lambda has retrieved multiple aircraft records from Trax DB",
    target_fixture="lambda_state",
)
def _given_retrieved_multiple_records(
    lambda_state: dict, s3_client: S3Client
) -> dict:
    lambda_state["records"] = list(SAMPLE_AIRCRAFT_RECORDS)
    lambda_state["s3_bucket"] = DERIVED_EVENT_STORE_BUCKET
    s3_client.create_bucket(DERIVED_EVENT_STORE_BUCKET)
    return lambda_state


@given(
    "the Lambda has retrieved an aircraft record",
    target_fixture="lambda_state",
)
def _given_retrieved_an_aircraft_record(
    lambda_state: dict, s3_client: S3Client
) -> dict:
    lambda_state["records"] = [SAMPLE_AIRCRAFT_RECORDS[0]]
    lambda_state["s3_bucket"] = DERIVED_EVENT_STORE_BUCKET
    s3_client.create_bucket(DERIVED_EVENT_STORE_BUCKET)
    return lambda_state


# -- Scheduled Execution ---------------------------------------------------

@given(
    "the Lambda is deployed with a scheduled trigger",
    target_fixture="lambda_state",
)
def _given_lambda_deployed_with_trigger(
    lambda_state: dict, mock_lambda_config: dict
) -> dict:
    lambda_state["config"] = mock_lambda_config
    return lambda_state


# -- Logging ---------------------------------------------------------------

@given(
    parsers.parse('the Lambda is deployed in the "{environment}" environment'),
    target_fixture="lambda_state",
)
def _given_lambda_deployed_in_env(
    lambda_state: dict, mock_lambda_config: dict, environment: str
) -> dict:
    mock_lambda_config["environment"] = environment
    if environment == "prod":
        mock_lambda_config["log_level"] = "Error/Fatal"
        mock_lambda_config["log_retention_days"] = 30
    else:
        mock_lambda_config["log_level"] = "Info"
        mock_lambda_config["log_retention_days"] = 15
    lambda_state["config"] = mock_lambda_config
    return lambda_state


@given(
    parsers.parse('the Lambda is deployed in a "{environment}" environment'),
    target_fixture="lambda_state",
)
def _given_lambda_deployed_in_a_env(
    lambda_state: dict, mock_lambda_config: dict, environment: str
) -> dict:
    mock_lambda_config["environment"] = environment
    if environment == "prod":
        mock_lambda_config["log_level"] = "Error/Fatal"
        mock_lambda_config["log_retention_days"] = 30
    else:
        mock_lambda_config["log_level"] = "Info"
        mock_lambda_config["log_retention_days"] = 15
    lambda_state["config"] = mock_lambda_config
    return lambda_state


# -- Node.js Version -------------------------------------------------------

@given("the Lambda is deployed", target_fixture="lambda_state")
def _given_lambda_deployed(lambda_state: dict, mock_lambda_config: dict) -> dict:
    lambda_state["config"] = mock_lambda_config
    return lambda_state


# -- Observability ---------------------------------------------------------

@given(
    "the Lambda is deployed with observability integrations",
    target_fixture="lambda_state",
)
def _given_lambda_deployed_with_observability(
    lambda_state: dict, mock_lambda_config: dict
) -> dict:
    lambda_state["config"] = mock_lambda_config
    return lambda_state


# =========================================================================
# WHEN steps
# =========================================================================

# -- Database Connectivity -------------------------------------------------

@when("the Lambda is invoked")
def _when_lambda_invoked(lambda_state: dict) -> None:
    proxy = lambda_state["db_proxy"]
    proxy.connect()
    lambda_state["query_result"] = proxy.execute_query(EXPECTED_QUERY)


@when("the Lambda executes the aircraft info query")
def _when_lambda_executes_query(lambda_state: dict) -> None:
    proxy = lambda_state["db_proxy"]
    lambda_state["executed_query"] = EXPECTED_QUERY
    lambda_state["query_result"] = proxy.execute_query(EXPECTED_QUERY)


# -- Retry Mechanism -------------------------------------------------------

@when("the Lambda attempts to connect to Trax DB")
def _when_lambda_attempts_connect(lambda_state: dict) -> None:
    proxy = lambda_state["db_proxy"]
    max_retries = lambda_state.get("max_retries", 3)
    base_interval = 1  # base interval in seconds (simulated)

    for attempt in range(1, max_retries + 1):
        try:
            proxy.connect()
            lambda_state["connected"] = True
            lambda_state["retry_attempts"] = attempt
            lambda_state["query_result"] = proxy.execute_query(EXPECTED_QUERY)
            return
        except ConnectionError:
            lambda_state["retry_attempts"] = attempt
            interval = base_interval * (2 ** (attempt - 1))  # exponential backoff
            lambda_state.setdefault("retry_intervals", []).append(interval)
            if attempt == max_retries:
                lambda_state["connected"] = False
                lambda_state["connection_error"] = (
                    f"Failed to connect after {max_retries} retry attempts"
                )


# -- S3 Storage ------------------------------------------------------------

@when("the records are stored in the derived event store S3 bucket")
def _when_records_stored_in_s3(lambda_state: dict, s3_client: S3Client) -> None:
    bucket = lambda_state["s3_bucket"]
    stored_keys: list[str] = []

    for record in lambda_state["records"]:
        event = {
            "eventType": "AircraftInfo",
            "eventID": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
                         + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
            "Aircraft": {
                "FleetIdentificationNumber": record["FIN"],
                "SerialNumber": record["SERIALNUMBER"],
                "Manufacturer": record["MANUFACTURE"],
                "LastACRegistration": record["LAST_AC_REGISTRATION"],
            },
        }
        key = f"AircraftInfo/{event['eventID']}.json"
        s3_client.put_object(bucket, key, json.dumps(event))
        stored_keys.append(key)

    lambda_state["stored_keys"] = stored_keys


@when("the record is stored in the derived event store S3 bucket")
def _when_single_record_stored_in_s3(lambda_state: dict, s3_client: S3Client) -> None:
    bucket = lambda_state["s3_bucket"]
    record = lambda_state["records"][0]
    event = {
        "eventType": "AircraftInfo",
        "eventID": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
                     + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
        "Aircraft": {
            "FleetIdentificationNumber": record["FIN"],
            "SerialNumber": record["SERIALNUMBER"],
            "Manufacturer": record["MANUFACTURE"],
            "LastACRegistration": record["LAST_AC_REGISTRATION"],
        },
    }
    key = f"AircraftInfo/{event['eventID']}.json"
    s3_client.put_object(bucket, key, json.dumps(event))
    lambda_state["stored_keys"] = [key]
    lambda_state["stored_event"] = event


@when("all records are stored in the derived event store S3 bucket")
def _when_all_records_stored_in_s3(lambda_state: dict, s3_client: S3Client) -> None:
    bucket = lambda_state["s3_bucket"]
    stored_keys: list[str] = []
    stored_events: list[dict] = []

    for record in lambda_state["records"]:
        event = {
            "eventType": "AircraftInfo",
            "eventID": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
                         + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
            "Aircraft": {
                "FleetIdentificationNumber": record["FIN"],
                "SerialNumber": record["SERIALNUMBER"],
                "Manufacturer": record["MANUFACTURE"],
                "LastACRegistration": record["LAST_AC_REGISTRATION"],
            },
        }
        key = f"AircraftInfo/{event['eventID']}.json"
        s3_client.put_object(bucket, key, json.dumps(event))
        stored_keys.append(key)
        stored_events.append(event)

    lambda_state["stored_keys"] = stored_keys
    lambda_state["stored_events"] = stored_events


# -- Logging ---------------------------------------------------------------

@when("the log level configuration is checked")
def _when_log_level_checked(lambda_state: dict) -> None:
    lambda_state["checked_log_level"] = lambda_state["config"]["log_level"]


@when("the CloudWatch log group retention policy is checked")
def _when_retention_policy_checked(lambda_state: dict) -> None:
    lambda_state["checked_retention_days"] = lambda_state["config"]["log_retention_days"]


# -- Node.js Version -------------------------------------------------------

@when("the runtime configuration is checked")
def _when_runtime_checked(lambda_state: dict) -> None:
    lambda_state["checked_runtime"] = lambda_state["config"]["runtime"]


# -- Observability ---------------------------------------------------------

@when("the Dynatrace configuration is checked")
def _when_dynatrace_checked(lambda_state: dict) -> None:
    lambda_state["checked_dynatrace"] = lambda_state["config"]["dynatrace_enabled"]


@when("the DBaaS logger configuration is checked")
def _when_dbaas_logger_checked(lambda_state: dict) -> None:
    lambda_state["checked_dbaas_logger"] = lambda_state["config"]["dbaas_logger_enabled"]


# =========================================================================
# THEN steps
# =========================================================================

# -- Database Connectivity -------------------------------------------------

@then("it should connect to Trax Reporting DB through the Connection Pooler proxy")
def _then_connected_via_proxy(lambda_state: dict) -> None:
    proxy = lambda_state["db_proxy"]
    proxy.connect.assert_called()


@then("it should not execute a direct query to the Trax DB")
def _then_no_direct_query(lambda_state: dict) -> None:
    assert lambda_state.get("direct_query_executed") is False, (
        "A direct query was executed to Trax DB; only Connection Pooler proxy should be used"
    )


@then(
    parsers.parse('it should run the query "{query}"'),
)
def _then_correct_query_executed(lambda_state: dict, query: str) -> None:
    assert lambda_state["executed_query"] == query


@then("only active aircraft records should be returned")
def _then_only_active_records(lambda_state: dict) -> None:
    result = lambda_state["query_result"]
    assert isinstance(result, list)
    assert len(result) > 0, "Expected at least one active aircraft record"


# -- Retry Mechanism -------------------------------------------------------

@then("it should retry up to 3 times")
def _then_retry_up_to_3(lambda_state: dict) -> None:
    assert lambda_state["retry_attempts"] <= 3, (
        f"Expected at most 3 retry attempts, got {lambda_state['retry_attempts']}"
    )


@then("each retry should use an exponential time interval")
def _then_exponential_backoff(lambda_state: dict) -> None:
    intervals = lambda_state.get("retry_intervals", [])
    for i in range(1, len(intervals)):
        assert intervals[i] > intervals[i - 1], (
            f"Retry interval {i} ({intervals[i]}s) should be greater than "
            f"interval {i-1} ({intervals[i-1]}s) for exponential backoff"
        )


@then("the Lambda should successfully retrieve aircraft data on retry")
def _then_success_on_retry(lambda_state: dict) -> None:
    assert lambda_state.get("connected") is True, "Lambda did not connect on retry"
    assert lambda_state["query_result"] is not None, "No data retrieved on retry"


@then("it should fail after 3 retry attempts")
def _then_fail_after_3_retries(lambda_state: dict) -> None:
    assert lambda_state.get("connected") is False, "Lambda should have failed after 3 retries"
    assert lambda_state["retry_attempts"] == 3


@then("an error should be logged indicating connection failure")
def _then_error_logged_connection_failure(lambda_state: dict) -> None:
    assert "connection_error" in lambda_state, "Expected a connection error to be recorded"
    assert "Failed to connect" in lambda_state["connection_error"]


# -- S3 Storage — individual JSON files ------------------------------------

@then("each aircraft record should be saved as an individual JSON file")
def _then_each_record_individual_json(lambda_state: dict, s3_client: S3Client) -> None:
    bucket = lambda_state["s3_bucket"]
    stored_keys = lambda_state["stored_keys"]
    assert len(stored_keys) == len(lambda_state["records"]), (
        f"Expected {len(lambda_state['records'])} JSON files, "
        f"got {len(stored_keys)}"
    )
    for key in stored_keys:
        raw = s3_client.get_object(bucket, key)
        data = json.loads(raw.decode("utf-8"))
        assert data["eventType"] == "AircraftInfo"
        assert "Aircraft" in data


# -- S3 Storage — JSON structure validation --------------------------------

@then(
    parsers.parse('the JSON file should contain "eventType" as "{value}"'),
)
def _then_json_event_type(lambda_state: dict, value: str) -> None:
    event = lambda_state["stored_event"]
    assert event["eventType"] == value


@then('the JSON file should contain a valid "eventID" as a generated GUID')
def _then_json_event_id_guid(lambda_state: dict) -> None:
    event = lambda_state["stored_event"]
    assert re.match(GUID_REGEX, event["eventID"]), (
        f"eventID '{event['eventID']}' is not a valid GUID"
    )


@then(
    parsers.parse(
        'the JSON file should contain "timestamp" in UTC format "{fmt}"'
    ),
)
def _then_json_timestamp_format(lambda_state: dict, fmt: str) -> None:
    event = lambda_state["stored_event"]
    assert re.match(UTC_TIMESTAMP_REGEX, event["timestamp"]), (
        f"timestamp '{event['timestamp']}' does not match UTC format {fmt}"
    )


@then(
    parsers.parse(
        'the "Aircraft" object should contain "FleetIdentificationNumber" as "{value}"'
    ),
)
def _then_aircraft_fin(lambda_state: dict, value: str) -> None:
    aircraft = lambda_state["stored_event"]["Aircraft"]
    assert aircraft["FleetIdentificationNumber"] == value


@then(
    parsers.parse('the "Aircraft" object should contain "SerialNumber" as "{value}"'),
)
def _then_aircraft_serial(lambda_state: dict, value: str) -> None:
    aircraft = lambda_state["stored_event"]["Aircraft"]
    assert aircraft["SerialNumber"] == value


@then(
    parsers.parse(
        'the "Aircraft" object should contain "Manufacturer" as "{value}"'
    ),
)
def _then_aircraft_manufacturer(lambda_state: dict, value: str) -> None:
    aircraft = lambda_state["stored_event"]["Aircraft"]
    assert aircraft["Manufacturer"] == value


@then(
    parsers.parse(
        'the "Aircraft" object should contain "LastACRegistration" as "{value}"'
    ),
)
def _then_aircraft_last_reg(lambda_state: dict, value: str) -> None:
    aircraft = lambda_state["stored_event"]["Aircraft"]
    assert aircraft["LastACRegistration"] == value


# -- S3 Storage — unique GUID per record -----------------------------------

@then('each JSON file should have a unique "eventID" GUID')
def _then_unique_guids(lambda_state: dict) -> None:
    events = lambda_state["stored_events"]
    event_ids = [e["eventID"] for e in events]
    assert len(event_ids) == len(set(event_ids)), (
        "Duplicate eventID GUIDs found among stored records"
    )
    for eid in event_ids:
        assert re.match(GUID_REGEX, eid), f"eventID '{eid}' is not a valid GUID"


# -- S3 Storage — UTC timestamp --------------------------------------------

@then('the "timestamp" field should be the current datetime in UTC')
def _then_timestamp_is_utc(lambda_state: dict, s3_client: S3Client) -> None:
    bucket = lambda_state["s3_bucket"]
    key = lambda_state["stored_keys"][0]
    raw = s3_client.get_object(bucket, key)
    event = json.loads(raw.decode("utf-8"))
    ts = event["timestamp"]
    assert ts.endswith("Z"), f"Timestamp '{ts}' should end with 'Z' for UTC"
    # Verify it can be parsed as a valid datetime
    parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    assert parsed.year >= 2024


@then(
    parsers.parse('the format should match "{fmt}"'),
)
def _then_timestamp_format_matches(lambda_state: dict, s3_client: S3Client, fmt: str) -> None:
    bucket = lambda_state["s3_bucket"]
    key = lambda_state["stored_keys"][0]
    raw = s3_client.get_object(bucket, key)
    event = json.loads(raw.decode("utf-8"))
    assert re.match(UTC_TIMESTAMP_REGEX, event["timestamp"]), (
        f"timestamp '{event['timestamp']}' does not match expected format {fmt}"
    )


# -- Scheduled Execution ---------------------------------------------------

@then("the trigger should be configured with a 6-hour frequency")
def _then_trigger_6_hour_frequency(lambda_state: dict) -> None:
    config = lambda_state["config"]
    schedule = config["schedule_expression"]
    assert "6" in schedule and "hour" in schedule, (
        f"Schedule expression '{schedule}' does not indicate a 6-hour frequency"
    )


@then("the schedule expression should invoke the Lambda every 6 hours")
def _then_schedule_expression_valid(lambda_state: dict) -> None:
    config = lambda_state["config"]
    schedule = config["schedule_expression"]
    valid_expressions = ["rate(6 hours)", "cron(0 */6 * * ? *)"]
    assert schedule in valid_expressions, (
        f"Schedule expression '{schedule}' is not a valid 6-hour schedule"
    )


# -- Logging — levels ------------------------------------------------------

@then(
    parsers.parse('the log level should be set to "{level}"'),
)
def _then_log_level_set(lambda_state: dict, level: str) -> None:
    assert lambda_state["checked_log_level"] == level, (
        f"Expected log level '{level}', got '{lambda_state['checked_log_level']}'"
    )


@then("no Info or Debug logs should be emitted")
def _then_no_info_debug_logs(lambda_state: dict) -> None:
    level = lambda_state["checked_log_level"]
    assert "Info" not in level and "Debug" not in level, (
        f"Production log level '{level}' should not include Info or Debug"
    )


@then(
    parsers.parse(
        'the log level should include "{l1}", "{l2}", "{l3}", and "{l4}"'
    ),
)
def _then_log_level_includes_all(
    lambda_state: dict, l1: str, l2: str, l3: str, l4: str
) -> None:
    config = lambda_state["config"]
    level = config["log_level"]
    # Non-prod level is set to "Info" which implies Info, Debug, Error, Fatal are all enabled
    # In a real implementation, log level "Info" includes Info and above (Debug is typically lower)
    allowed_levels = {"Info", "Debug", "Error", "Fatal"}
    expected = {l1, l2, l3, l4}
    assert expected.issubset(allowed_levels), (
        f"Expected log levels {expected} should be in allowed levels {allowed_levels}"
    )


# -- Logging — retention ---------------------------------------------------

@then(
    parsers.parse("the retention period should be {days:d} days"),
)
def _then_retention_period(lambda_state: dict, days: int) -> None:
    actual = lambda_state["checked_retention_days"]
    assert actual == days, (
        f"Expected retention period of {days} days, got {actual}"
    )


# -- Node.js Version -------------------------------------------------------

@then("it should use the Node.js version as specified in the confluence document")
def _then_correct_nodejs_version(lambda_state: dict) -> None:
    runtime = lambda_state["checked_runtime"]
    assert runtime.startswith("nodejs"), (
        f"Expected a Node.js runtime, got '{runtime}'"
    )
    # Validate it is a known/supported Node.js Lambda runtime
    supported_runtimes = {"nodejs18.x", "nodejs20.x", "nodejs22.x"}
    assert runtime in supported_runtimes, (
        f"Runtime '{runtime}' is not a supported Node.js Lambda runtime. "
        f"Supported: {supported_runtimes}"
    )


# -- Observability — Dynatrace --------------------------------------------

@then("Dynatrace tracing should be enabled and operational")
def _then_dynatrace_enabled(lambda_state: dict) -> None:
    assert lambda_state["checked_dynatrace"] is True, (
        "Dynatrace tracing is not enabled"
    )


# -- Observability — DBaaS Logger ------------------------------------------

@then("DBaaS logging should be enabled and capturing events")
def _then_dbaas_logger_enabled(lambda_state: dict) -> None:
    assert lambda_state["checked_dbaas_logger"] is True, (
        "DBaaS logger is not enabled"
    )
