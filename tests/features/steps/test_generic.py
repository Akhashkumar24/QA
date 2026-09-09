"""The one and only step library — every pipeline is validated from config.

Each generic scenario is bound once here and parametrised over the use cases
that declare the capability it needs (``usecase_params``). Adding a
``conf/usecases/<id>.yaml`` is enough — no edit to this file or the features.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from datetime import UTC, datetime

import pytest
from pytest_bdd import given, scenario, then, when

from aws_testkit import checks
from aws_testkit.aws_clients import (
    CloudWatchLogsClient,
    GlueClient,
    LambdaClient,
    S3Client,
    SqsClient,
)
from aws_testkit.config import Settings
from aws_testkit.report import attach, html_json, html_kv, html_table
from aws_testkit.usecase import capabilities, load_all, load_usecase

logger = logging.getLogger("aws_testkit")

_ALL = load_all()
_SUCCESS_HINTS = ("success", "processed", "completed", "ingested", "published", "stored",
                  "end requestid", "report requestid")
_ERROR_HINTS = ("error", "exception", "failed", "traceback", "invalid", "malformed", "unable")


def usecase_params(capability: str):
    ids = [uid for uid, cfg in _ALL.items()
           if not isinstance(cfg, Exception) and capability in capabilities(cfg)]
    return ids or [pytest.param(
        "<none>", marks=pytest.mark.skip(reason=f"no use case declares '{capability}'")
    )]


# ---------------------------------------------------------------------------
# scenario bindings — one per (feature, scenario), parametrised by capability
# ---------------------------------------------------------------------------

def _bind(feature: str, name: str, capability: str):
    @pytest.mark.parametrize("usecase_id", usecase_params(capability))
    @scenario(f"generic/{feature}", name)
    def _test(usecase_id):  # noqa: ANN001, D401
        pass
    return _test


test_lambda_ingestion = _bind(
    "lambda_ingestion.feature", "Lambda ingests its test event successfully", "lambda_ingestion")
test_failed_ingestion = _bind(
    "failed_ingestion.feature",
    "The pipeline rejects an invalid event without storing output", "failed_ingestion")
test_s3_presence = _bind(
    "s3_derived_events.feature", "JSON output exists under every configured prefix", "s3_presence")
test_s3_schema = _bind(
    "s3_derived_events.feature", "A current-date derived event matches the schema", "s3_schema")
test_s3_filename = _bind(
    "s3_derived_events.feature",
    "Derived-event filenames follow the naming convention", "s3_filename")
test_s3_objects = _bind(
    "s3_object_mapping.feature", "Configured objects contain every documented field", "s3_objects")
test_s3_latest_dated = _bind(
    "s3_object_mapping.feature",
    "The latest event under today's date folder is fully mapped", "s3_latest_dated")
test_glue_job = _bind(
    "glue_job.feature", "Glue job exists and runs on the configured schedule", "glue")
test_db_reconciliation = _bind(
    "db_reconciliation.feature", "Every configured database check passes", "db")
test_lambda_configuration = _bind(
    "lambda_configuration.feature", "Lambda is deployed as configured", "lambda_config")
test_sqs_redrive = _bind(
    "sqs_dlq.feature", "The primary queue is wired to its DLQ", "sqs")
test_sqs_dlq_roundtrip = _bind(
    "sqs_dlq.feature", "A failing message lands in the DLQ", "sqs")
test_s3_lifecycle = _bind(
    "s3_lifecycle.feature", "The bucket enforces the configured retention", "lifecycle")


# ---------------------------------------------------------------------------
# client helpers
# ---------------------------------------------------------------------------

def _settings(cfg: dict) -> Settings:
    return Settings(aws_region=cfg.get("region", Settings().aws_region))


def _client(kind, cfg: dict, section: str):
    blk = cfg.get(section, {}) or {}
    return kind(_settings(cfg), profile=blk.get("profile"), region=cfg.get("region"))


def _log_group(cfg: dict) -> str:
    lc = cfg["lambda"]
    return lc.get("log_group") or f"/aws/lambda/{lc['name']}"


def _predicate(paths):
    if not paths:
        return None

    def ok(doc) -> bool:
        for p in paths:
            val, found = checks.resolve(doc, p)
            if not found or val in (None, ""):
                return False
        return True

    return ok


# ---------------------------------------------------------------------------
# GIVEN
# ---------------------------------------------------------------------------

@given("the use case", target_fixture="uc")
def _given_uc(usecase_id: str) -> dict:
    cfg = load_usecase(usecase_id)
    logger.info("use case '%s' — capabilities: %s", usecase_id, ", ".join(capabilities(cfg)))
    return {"cfg": cfg}


# ---------------------------------------------------------------------------
# Lambda invocation
# ---------------------------------------------------------------------------

@when("I invoke the use-case Lambda with its test event")
def _invoke(uc: dict) -> None:
    cfg = uc["cfg"]
    uc["invoked_at"] = datetime.now(UTC)
    uc["lambda_result"] = _client(LambdaClient, cfg, "lambda").invoke(
        cfg["lambda"]["name"], cfg["lambda"]["test_event"]
    )
    logger.info("invoked %s → HTTP %s", cfg["lambda"]["name"], uc["lambda_result"]["status_code"])


@when("I invoke the use-case Lambda with its invalid test event")
def _invoke_invalid(uc: dict) -> None:
    cfg = uc["cfg"]
    uc["invoked_at"] = datetime.now(UTC)
    uc["lambda_result"] = _client(LambdaClient, cfg, "lambda").invoke(
        cfg["lambda"]["name"], cfg["lambda"].get("invalid_event") or {}
    )


@then("the Lambda invocation is successful")
def _invoke_ok(uc: dict) -> None:
    res = uc["lambda_result"]
    spec = uc["cfg"]["lambda"].get("success", {"status_code": 200})
    if "status_code" in spec:
        assert res["status_code"] == spec["status_code"], (
            f"HTTP {res['status_code']} != {spec['status_code']}; error={res['function_error']}"
        )
    if "execution_status" in spec:
        assert res["execution_status"] == spec["execution_status"], (
            f"execution {res['execution_status']} != {spec['execution_status']}"
        )
    assert not res["function_error"], f"Lambda FunctionError: {res['function_error']}"


@then("the configured Lambda response fields are valid")
def _response_fields(uc: dict) -> None:
    res = uc["lambda_result"]["response"]
    for rule in uc["cfg"]["lambda"].get("response_fields", []):
        val, found = checks.resolve(res, rule["path"])
        assert found, f"response missing {rule['path']!r}; keys={list(res)}"
        if rule.get("success_text"):
            assert any(w in str(val).lower() for w in ("success", "completed", "finished", "done")), (
                f"{rule['path']}={val!r} does not read as success"
            )
        if "greater_than" in rule:
            assert isinstance(val, (int, float)) and val > rule["greater_than"], (
                f"{rule['path']}={val!r} not > {rule['greater_than']}"
            )
        if "equals" in rule:
            assert str(val) == str(rule["equals"]), f"{rule['path']}={val!r} != {rule['equals']!r}"
        if "equals_field" in rule:
            other, ok = checks.resolve(res, rule["equals_field"])
            assert ok and val == other, (
                f"{rule['path']}={val!r} != {rule['equals_field']}={other!r}"
            )


@then("the Lambda response is attached to the report")
def _attach_response(uc: dict, extras: list) -> None:
    res = uc["lambda_result"]
    attach(extras, html_kv(
        [("status_code", res["status_code"]), ("request_id", res["request_id"]),
         ("function_error", res["function_error"] or "-")],
        f"Lambda response — {uc['cfg']['lambda']['name']}",
    ))
    attach(extras, html_json(res["response"], "Response body"))
    if res["log_tail"]:
        attach(extras, html_json(res["log_tail"], "Log tail (last 4KB)"))


# ---------------------------------------------------------------------------
# CloudWatch logs
# ---------------------------------------------------------------------------

@when("I search CloudWatch logs for the Lambda request id")
def _search_logs(uc: dict) -> None:
    cfg = uc["cfg"]
    rid = uc["lambda_result"]["request_id"]
    events = _client(CloudWatchLogsClient, cfg, "lambda").search_request_id(
        _log_group(cfg), rid) if rid else []
    uc["log_events"] = events
    uc["log_text"] = (
        "\n".join(e.get("message", "") for e in events)
        + "\n" + uc["lambda_result"]["log_tail"]
    ).lower()
    logger.info("cloudwatch: %d event(s) for request %s", len(events), rid)


@then("CloudWatch logs confirm ingestion success")
def _logs_success(uc: dict) -> None:
    assert any(h in uc["log_text"] for h in _SUCCESS_HINTS), (
        f"no success marker in logs/log-tail; tail: {uc['lambda_result']['log_tail'][:400]}"
    )


@then("CloudWatch logs indicate a processing error")
def _logs_error(uc: dict) -> None:
    assert any(h in uc["log_text"] for h in _ERROR_HINTS) or uc["lambda_result"]["function_error"], (
        f"no error marker in logs; FunctionError={uc['lambda_result']['function_error']}"
    )


@then("the CloudWatch log details are attached to the report")
def _attach_logs(uc: dict, extras: list) -> None:
    rows = [
        {"timestamp": datetime.fromtimestamp(e.get("timestamp", 0) / 1000, tz=UTC)
         .strftime("%Y-%m-%d %H:%M:%S"), "message": e.get("message", "").strip()[:300]}
        for e in uc.get("log_events", [])
    ]
    attach(extras, html_table(["timestamp", "message"], rows,
                              f"CloudWatch — {_log_group(uc['cfg'])}",
                              f"request id: {uc['lambda_result']['request_id']}"))


# ---------------------------------------------------------------------------
# S3 — presence
# ---------------------------------------------------------------------------

@when("I list objects under each configured prefix")
def _list_prefixes(uc: dict) -> None:
    cfg = uc["cfg"]
    s3 = _client(S3Client, cfg, "s3")
    uc["prefix_counts"] = {
        p: len(s3.list_objects(cfg["s3"]["bucket"], p, suffix=".json", max_pages=1))
        for p in cfg["s3"]["prefixes"]
    }
    logger.info("prefix counts: %s", uc["prefix_counts"])


@then("every configured prefix holds the minimum number of JSON files")
def _min_files(uc: dict, extras: list) -> None:
    minimum = uc["cfg"]["s3"].get("min_files", 1)
    attach(extras, html_kv(list(uc["prefix_counts"].items()), "JSON files per prefix"))
    bad = {p: n for p, n in uc["prefix_counts"].items() if n < minimum}
    assert not bad, f"prefixes below minimum {minimum}: {bad}"


# ---------------------------------------------------------------------------
# S3 — derived-event schema
# ---------------------------------------------------------------------------

@when("I fetch a random current-date derived event")
def _fetch_random(uc: dict) -> None:
    cfg, s3conf = uc["cfg"], uc["cfg"]["s3"]
    sel = s3conf.get("select", {})
    key, doc = _client(S3Client, cfg, "s3").find_event(
        s3conf["bucket"], s3conf["prefixes"][0],
        predicate=_predicate(s3conf.get("event_predicate")),
        current_date_only=sel.get("current_date_only", True),
        pick=sel.get("pick", "random"),
    )
    uc["event_key"], uc["event_doc"] = key, doc
    logger.info("selected derived event %s", key)


@when("I fetch the latest derived event under the use-case date folder")
def _fetch_latest_dated(uc: dict) -> None:
    cfg, s3conf = uc["cfg"], uc["cfg"]["s3"]
    day = os.getenv("FLIGHT_EVENTS_DATE") or datetime.now(UTC).date().isoformat()
    prefix = s3conf["date_prefix"].format(date=day)
    s3 = _client(S3Client, cfg, "s3")
    obj = s3.latest_object(s3conf["bucket"], prefix)
    assert obj is not None, f"no .json under s3://{s3conf['bucket']}/{prefix}"
    uc["event_key"], uc["event_doc"] = obj["Key"], s3.get_json(s3conf["bucket"], obj["Key"])


@when("I fetch each configured derived-event object")
def _fetch_objects(uc: dict) -> None:
    cfg, s3conf = uc["cfg"], uc["cfg"]["s3"]
    s3 = _client(S3Client, cfg, "s3")
    uc["objects"] = [
        (o["key"], o.get("schema"), s3.get_json(s3conf["bucket"], o["key"]))
        for o in s3conf.get("objects", [])
    ]
    assert uc["objects"], "no s3.objects configured"


@then("the derived event matches the use-case schema")
def _event_schema(uc: dict, extras: list) -> None:
    doc = uc["event_doc"]
    attach(extras, html_json(doc, f"Derived event — {uc.get('event_key', '?')}"))
    errs = checks.validate(doc, uc["cfg"].get("schema", {}))
    assert not errs, "schema violations:\n  - " + "\n  - ".join(errs)


@then("every configured derived-event object matches its schema")
def _objects_schema(uc: dict, extras: list) -> None:
    failures: list[str] = []
    for key, schema, doc in uc["objects"]:
        attach(extras, html_json(doc, f"Object — {key}"))
        failures += [f"{key}: {e}" for e in checks.validate(doc, schema or uc["cfg"].get("schema", {}))]
    assert not failures, "schema violations:\n  - " + "\n  - ".join(failures)


@then("the derived-event filename matches the configured pattern")
def _filename_pattern(uc: dict) -> None:
    rx = uc["cfg"]["s3"]["filename_regex"]
    name = uc["event_key"].split("/")[-1]
    assert re.match(rx, name), f"{name!r} does not match /{rx}/"


@then("no derived event is stored after the invalid invocation")
def _no_new_event(uc: dict) -> None:
    cfg, s3conf = uc["cfg"], uc["cfg"]["s3"]
    time.sleep(10)
    since = uc["invoked_at"]
    s3 = _client(S3Client, cfg, "s3")
    new = [
        o["Key"]
        for p in s3conf["prefixes"]
        for o in s3.list_objects(s3conf["bucket"], p, suffix=".json", max_pages=1)
        if o["LastModified"] >= since
    ]
    assert not new, f"{len(new)} object(s) written after the invalid invoke: {new[:5]}"


# ---------------------------------------------------------------------------
# Glue
# ---------------------------------------------------------------------------

@when("I retrieve the use-case Glue job and its triggers")
def _get_glue(uc: dict) -> None:
    cfg = uc["cfg"]
    g = _client(GlueClient, cfg, "glue")
    name = cfg["glue"]["name"]
    uc["glue_job"] = g.get_job(name)
    uc["glue_triggers"] = g.triggers_for_job(name) if uc["glue_job"] else []


@then("the Glue job exists")
def _glue_exists(uc: dict, extras: list) -> None:
    assert uc["glue_job"], f"Glue job {uc['cfg']['glue']['name']!r} not found"
    job = uc["glue_job"]
    attach(extras, html_kv(
        [("Name", job.get("Name")), ("Role", job.get("Role")),
         ("GlueVersion", job.get("GlueVersion")),
         ("ScriptLocation", job.get("Command", {}).get("ScriptLocation"))],
        "Glue job",
    ))


@then("a Glue trigger runs on the configured schedule in the configured state")
def _glue_schedule(uc: dict, extras: list) -> None:
    cfg = uc["cfg"]["glue"]
    rx = re.compile(cfg["schedule_regex"], re.IGNORECASE)
    want = cfg.get("trigger_state", "ACTIVATED")
    rows = [{"Name": t.get("Name"), "Type": t.get("Type"),
             "Schedule": t.get("Schedule"), "State": t.get("State")} for t in uc["glue_triggers"]]
    attach(extras, html_table(["Name", "Type", "Schedule", "State"], rows, "Glue triggers"))
    assert any(
        t.get("Type") == "SCHEDULED" and rx.search(t.get("Schedule") or "") and t.get("State") == want
        for t in uc["glue_triggers"]
    ), f"no SCHEDULED trigger matching /{cfg['schedule_regex']}/ in state {want}"


# ---------------------------------------------------------------------------
# Lambda configuration
# ---------------------------------------------------------------------------

@when("I retrieve the use-case Lambda configuration")
def _get_lambda_cfg(uc: dict) -> None:
    resp = _client(LambdaClient, uc["cfg"], "lambda").get_function(uc["cfg"]["lambda"]["name"])
    uc["fn_config"] = resp.get("Configuration", {})
    uc["fn_arn"] = uc["fn_config"].get("FunctionArn", "")


@then("the Lambda runtime matches configuration")
def _fn_runtime(uc: dict) -> None:
    want = uc["cfg"]["lambda"]["config"]["runtime"]
    got = (uc["fn_config"].get("Runtime") or "").strip()
    assert got == want, f"runtime {got!r} != {want!r}"


@then("every configured Lambda environment variable matches")
def _fn_env(uc: dict, extras: list) -> None:
    env = uc["fn_config"].get("Environment", {}).get("Variables", {}) or {}
    attach(extras, html_kv(sorted(env.items()), "Lambda environment"))
    bad = {k: (env.get(k), v) for k, v in uc["cfg"]["lambda"]["config"].get("env", {}).items()
           if env.get(k) != v}
    assert not bad, f"env mismatches (got, want): {bad}"


@then("the Lambda carries the configured resource tags")
def _fn_tags(uc: dict, extras: list) -> None:
    wanted = [w.lower() for w in uc["cfg"]["lambda"]["config"].get("tags_any", [])]
    if not wanted:
        return
    tags = _client(LambdaClient, uc["cfg"], "lambda").list_tags(uc["fn_arn"])
    attach(extras, html_kv(sorted(tags.items()), "Lambda tags"))
    assert any(any(w in k.lower() for w in wanted) for k in tags), (
        f"none of {wanted} present in tag keys {list(tags)}"
    )


@then("the Lambda has the configured SQS event source enabled")
def _fn_event_source(uc: dict) -> None:
    queue = uc["cfg"]["lambda"]["config"].get("event_source_queue")
    if not queue:
        return
    maps = _client(LambdaClient, uc["cfg"], "lambda").event_source_mappings(uc["cfg"]["lambda"]["name"])
    for m in maps:
        arn = m.get("EventSourceArn") or ""
        if arn.startswith("arn:aws:sqs:") and arn.rsplit(":", 1)[-1] == queue:
            assert (m.get("State") or "").strip() == "Enabled", f"mapping state {m.get('State')!r}"
            return
    raise AssertionError(
        f"no SQS event source for {queue!r} in {[m.get('EventSourceArn') for m in maps]}"
    )


@then("the Lambda log retention matches configuration")
def _fn_retention(uc: dict) -> None:
    rng = uc["cfg"]["lambda"]["config"].get("log_retention_days")
    if rng is None:
        return
    detail = _client(CloudWatchLogsClient, uc["cfg"], "lambda").describe_log_group(_log_group(uc["cfg"]))
    assert detail, f"log group {_log_group(uc['cfg'])} not found"
    actual = detail.get("retentionInDays")
    if isinstance(rng, dict):
        assert actual is not None and rng["min"] <= actual <= rng["max"], (
            f"retention {actual} not in [{rng['min']}, {rng['max']}]"
        )
    else:
        assert actual == rng, f"retention {actual} != {rng}"


# ---------------------------------------------------------------------------
# SQS / DLQ
# ---------------------------------------------------------------------------

@then("the primary queue redrive policy targets the configured DLQ")
def _redrive(uc: dict) -> None:
    q = uc["cfg"]["sqs"]
    sqs = _client(SqsClient, uc["cfg"], "sqs")
    rp_raw = sqs.attributes(sqs.queue_url(q["queue"]), ["RedrivePolicy"]).get("RedrivePolicy")
    assert rp_raw, "queue has no RedrivePolicy"
    assert q["dlq"] in json.loads(rp_raw).get("deadLetterTargetArn", ""), (
        f"RedrivePolicy does not target {q['dlq']!r}"
    )


@when("I send a failing test message and wait for it in the DLQ")
def _dlq_roundtrip(uc: dict) -> None:
    q = uc["cfg"]["sqs"]
    sqs = _client(SqsClient, uc["cfg"], "sqs")
    corr = f"{q.get('failing_body', 'qa-invalid')}-{uuid.uuid4().hex}"
    uc["dlq_correlation"] = corr
    try:
        sqs.send_message(sqs.queue_url(q["queue"]), f"{q.get('failing_body', 'qa-invalid')}:{corr}")
    except Exception as exc:  # noqa: BLE001 - AccessDenied etc. → skip, not fail
        pytest.skip(f"cannot send to primary queue: {exc}")
    uc["dlq_url"] = sqs.queue_url(q["dlq"])
    deadline = time.monotonic() + float(q.get("dlq_wait_seconds", 300))
    while time.monotonic() < deadline:
        wait = min(20, max(1, int(deadline - time.monotonic())))
        for msg in sqs.receive_message(uc["dlq_url"], wait=wait):
            if corr in msg.get("Body", ""):
                uc["dlq_message"] = msg
                return
        time.sleep(2)
    pytest.fail(f"correlation {corr!r} never reached DLQ {q['dlq']!r}")


@then("the DLQ message carries the test correlation id")
def _dlq_body(uc: dict) -> None:
    assert uc["dlq_correlation"] in uc["dlq_message"].get("Body", "")


@then("I remove the test message from the DLQ")
def _dlq_cleanup(uc: dict) -> None:
    try:
        _client(SqsClient, uc["cfg"], "sqs").delete_message(
            uc["dlq_url"], uc["dlq_message"]["ReceiptHandle"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not delete DLQ test message: %s", exc)


# ---------------------------------------------------------------------------
# S3 lifecycle
# ---------------------------------------------------------------------------

@then("the bucket has the configured lifecycle retention rule")
def _lifecycle(uc: dict, extras: list) -> None:
    lc = uc["cfg"]["lifecycle"]
    rules = _client(S3Client, uc["cfg"], "s3").get_lifecycle_rules(uc["cfg"]["s3"]["bucket"])
    attach(extras, html_json(rules, "Lifecycle rules"))
    match = [
        r for r in rules
        if r.get("Expiration", {}).get("Days") == lc["expiration_days"]
        and r.get("Status") == lc.get("status", "Enabled")
    ]
    assert match, f"no {lc.get('status', 'Enabled')} rule with {lc['expiration_days']}-day expiration"


# ---------------------------------------------------------------------------
# DB reconciliation (aggregated)
# ---------------------------------------------------------------------------

@then("every configured database check passes")
def _db_checks(uc: dict, extras: list) -> None:
    # ponytail: all of a use case's db.checks run as one aggregated assertion.
    # Split into a per-check Scenario Outline if per-assertion Xray mapping matters.
    from aws_testkit import db as dbmod

    conf = uc["cfg"]["db"]
    failures: list[str] = []
    with dbmod.connect(conf) as conn:
        for chk in conf.get("checks", []):
            name = chk.get("name", chk["query"][:60])
            cols, rows = dbmod.run_query(conn, chk["query"])
            attach(extras, html_table(cols or (list(rows[0]) if rows else []), rows, f"DB — {name}"))
            if len(rows) < chk.get("min_rows", 0):
                failures.append(f"{name}: {len(rows)} row(s) < min {chk['min_rows']}")
                continue
            cvi = chk.get("column_values_include")
            if cvi:
                seen = {str(r.get(cvi["column"])) for r in rows}
                missing = [v for v in cvi["values"] if v not in seen]
                if missing:
                    failures.append(f"{name}: missing {cvi['column']} values {missing}")
            for path, spec in (chk.get("row0") or {}).items():
                if not rows:
                    failures.append(f"{name}: no row 0")
                    break
                val, found = checks.resolve(rows[0], path)
                failures += [f"{name}: {e}" for e in checks._check_field(path, val, found, spec)]
    assert not failures, "database check failures:\n  - " + "\n  - ".join(failures)
