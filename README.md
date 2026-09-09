# AWS Pipeline Automation Framework

End-to-end, config-driven BDD automation for AWS data pipelines. One generic
step library validates **any** pipeline — Lambda ingestion, S3 derived-event
output, Glue jobs, CloudWatch logs, SQS/DLQ routing, S3 lifecycle, and DB↔S3
reconciliation — entirely against **real AWS**. Adding a pipeline is a YAML file,
not code.

Built with **Python 3.11+**, **pytest-bdd**, **boto3**, **pytest-html**.
Currently drives the **ODH** data pipeline; the framework itself is
pipeline-agnostic.

---

## The idea

```
conf/usecases/<id>.yaml         you describe WHAT to check (lambda name, bucket,
       │                        prefixes, schema, queries, expected config …)
       ▼
aws_testkit.usecase.load_all()  loads + ${ENV}-expands + resolves profiles + VALIDATES
       │
       ▼
tests/features/steps/test_generic.py   each generic scenario is auto-parametrised
       │                               over the use cases that DECLARE its capability
       ▼
tests/features/generic/*.feature  capability scenarios (Lambda ingestion, S3 schema,
                                  Glue, DLQ, DB reconciliation, …) — fully generic
       ▼
one result per (capability × use case)  + a self-contained HTML report
```

No `Examples:` tables, no per-pipeline Python. A use case with a `glue:` block
automatically runs the Glue scenario; one with `s3.objects` runs the object
mapping scenario; and so on. `aws-testkit list` shows the mapping.

---

## Install

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"          # add ",db" for DB-reconciliation use cases: ".[dev,db]"
```

Credentials — named AWS profiles (SSO):

```bash
aws sso login --profile AC_Digital_BAT
aws sso login --profile ODH_UAT
cp .env.example .env             # profile overrides + DB secrets; then: set -a; . ./.env; set +a
```

`conf/profiles.yaml` maps logical names (`lambda`, `glue`, `s3`, `cdm`, `cep`) to
real profiles; override any with `<NAME>_AWS_PROFILE`. SSO token expiry mid-run
is handled automatically — the client re-runs `aws sso login` once and retries.

---

## The `aws-testkit` CLI

```bash
aws-testkit list                  # use cases and the capabilities each declares
aws-testkit show adm              # the fully-resolved config (env expanded)
aws-testkit validate              # offline lint of every use-case YAML (no AWS)
aws-testkit preflight             # read-only: STS identity + bucket/lambda/glue/queue reachability
aws-testkit preflight adm cdm     # ...for specific use cases
aws-testkit run                   # run the whole suite (real AWS)
aws-testkit run adm --html reports/adm.html      # one pipeline, with a report
aws-testkit run ground_time -- -k schedule       # args after -- go straight to pytest
```

Equivalent raw pytest:

```bash
pytest tests/features/steps/test_generic.py -k adm -v \
  --html=reports/adm_$(date +%F_%H-%M-%S).html --self-contained-html
pytest -m preflight -q            # reachability only
pytest tests/test_config.py -q    # offline config lint
```

---

## Adding a pipeline

1. `cp conf/usecases/_TEMPLATE.yaml conf/usecases/orders.yaml` and fill in the
   blocks that apply (`lambda`, `s3`, `schema`, `glue`, `db`, `sqs`,
   `lifecycle` — all optional).
2. If it has a Lambda test event: `conf/events/orders.json`.
3. `aws-testkit validate orders && aws-testkit preflight orders`.
4. `aws-testkit run orders`.

That's it — the relevant generic scenarios pick `orders` up automatically.

---

## Use-case config reference

`conf/usecases/<id>.yaml`. `${VAR}` / `${VAR:-default}` is expanded from the
environment (secrets never land in git). `profile:` resolves through
`conf/profiles.yaml`. See `conf/usecases/_TEMPLATE.yaml` for the annotated full
form; the shape:

```yaml
id: adm
region: ca-central-1

lambda:                              # -> lambda_ingestion / failed_ingestion / lambda_config
  name: "ac-odh-...-parser"
  profile: lambda
  test_event: events/adm_kafka.json
  invalid_event: events/adm_invalid.json
  success: {status_code: 200}        # or {execution_status: Succeeded}
  response_fields:
    - {path: "totalRecords", greater_than: 0}
    - {path: "successCount", equals_field: "totalRecords"}
    - {path: "message", success_text: true}
  config:                            # -> lambda_config
    runtime: "nodejs24.x"
    env: {EVENT_BUCKET: "...", ON_SUCCESS: "MOVE"}
    tags_any: ["dynatrace", "dt."]
    event_source_queue: "...-queue"
    log_retention_days: {min: 14, max: 15}

s3:                                  # -> s3_presence / s3_schema / s3_filename / s3_objects / s3_latest_dated
  profile: s3
  bucket: "ac-odh-derived-event-storage-uat-cac-1"
  prefixes: ["CDM/processed/CDM-FLIGHT-ODH-UAT/"]
  min_files: 1
  select: {current_date_only: true, pick: random}   # pick: random | latest
  event_predicate: ["eventType", "Id"]
  filename_regex: '^[0-9a-fA-F-]{36}\.json$'
  date_prefix: "flight-events/{date}/"
  objects:
    - {key: "CDM/processed/.../known.json", schema: {record_index: 0, fields: {...}}}

schema:                              # applied to the fetched derived event
  root: ["data.customData.getFlightFlattened"]   # optional envelope descent
  records: true                      # validate every element if the doc is an array
  fields:
    "eventType":                 {equals: "ADMInfo"}
    "Carousel":                  {type: int, nullable: true}
    "Id":                        {regex: '^[A-Z]{2}-\d+-\d{4}-\d{2}-\d{2}-[A-Z]{3}$'}
    "DepartureAirport.IATACode": {iata: true}
    "GoTime":                    {iso8601_utc: true}
  cross:
    - {left: "InboundFlight.ArrivalAirport.IATACode", equals: "Flight.DepartureAirport.IATACode"}

glue:      {name: "...", profile: glue, schedule_regex: 'rate\(\s*30\s+minutes?\s*\)', trigger_state: ACTIVATED}
sqs:       {profile: cdm, queue: "...-queue", dlq: "...-dlq", failing_body: "qa_invalid", dlq_wait_seconds: 300}
lifecycle: {expiration_days: 30, status: Enabled}

db:                                  # -> db_reconciliation  (needs the ".[db]" extra)
  ssm:   {instance_id: "${SSM_INSTANCE_ID}", region: ca-central-1, profile: glue}
  ssh:   {host: "${DB_SSH_HOST:-127.0.0.1}", port: "${DB_SSH_PORT:-13408}", username: "...", password: "${DB_SSH_PASSWORD}"}
  mysql: {host: "...", port: 3306, username: "...", password: "${DB_MYSQL_PASSWORD}", database: "..."}
  checks:
    - {name: "table exists", query: "SHOW TABLES LIKE 'FlightLegGroundTime'", min_rows: 1}
    - name: "9883 reconciles with S3"
      query: "SELECT * FROM FlightLegGroundTime WHERE FlightLegNumber = '9883'"
      min_rows: 1
      column_values_include: {column: "COLUMN_NAME", values: ["GoTime"]}
      row0:
        "FlightLegDepartureAirportCode": {equals: "YHZ"}
        "ScheduledGroundTime":           {minutes_equals: "24:49:00"}   # DB minutes vs HH:MM:SS
```

### Schema check vocabulary (`aws_testkit.checks`)

`present` · `equals` · `one_of` · `regex` · `type` (string/dict/list/int/float/number/bool) ·
`non_empty` · `iata` · `country` · `date` · `time` · `iso8601_utc` · `uuid` ·
`minutes_equals`. Modifiers: `nullable`, `optional`. `cross:` does field-to-field
equality. `python -c "from aws_testkit.checks import demo; demo()"` self-tests it.

---

## Package layout

```
conf/
  profiles.yaml               logical profile -> AWS profile (env-overridable)
  usecases/<id>.yaml          one file per pipeline  (_TEMPLATE.yaml = annotated blank)
  events/<id>.json            Lambda test-event payloads
src/aws_testkit/
  cli.py                      the `aws-testkit` command
  aws_clients.py              build_client() + S3/Logs/Lambda/Glue/Sqs/Sts wrappers, SSO auto-refresh
  usecase.py                  load / ${ENV}-expand / profile-resolve / validate ; capability map
  checks.py                   declarative value checks + validate(doc, schema)
  db.py                       MySQL over SSH/SSM tunnel  (extra: pip install -e ".[db]")
  report.py                   HTML fragments for pytest-html
  config.py                   AWS Settings (region / endpoint / creds / profile) only
  credential_refresh.py       `aws sso login` on token expiry
  xray_client.py              optional Xray/JIRA feature fetch + result import
tests/
  test_config.py             offline: every use-case YAML loads & validates
  test_preflight.py          `-m preflight`: read-only AWS reachability, per use case
  features/generic/*.feature  capability scenarios (generic)
  features/steps/test_generic.py   the one step library + capability parametrisation
```

---

## Xray / JIRA

`run_xray_tests.py QA-101 QA-102 [--import-results]` fetches Gherkin from Xray
into `tests/features/xray/`, runs it, optionally pushes Cucumber results back.

---

## CI

`.github/workflows/ci.yml`: an **offline** job on every push (ruff, `checks`
self-test, `aws-testkit validate`, config tests, generic step-resolution) and a
**live** job on manual dispatch (`inputs.run_live`) that runs preflight + the
full suite with credentials and uploads the HTML report.

---

## Known cut corners

- `db_reconciliation` runs a use case's DB checks as one aggregated step
  (`ponytail:` in `test_generic.py`). Split per-check for per-assertion Xray IDs.
- `checks.py` is a hand-rolled registry — swap for `jsonschema` only if the
  schema vocabulary proves too small.
- The DLQ round-trip scenario sends a real failing message; it `skip`s on
  `AccessDenied` rather than failing.
