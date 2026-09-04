# ODH QA Automation Framework

BDD-driven test automation framework for validating the **Operational Data Hub (ODH)** data pipeline across AWS Lambda ingestion, S3 derived event stores, AWS Glue ETL jobs, CloudWatch logging, and MySQL database connectivity.

Built with **Python**, **pytest-bdd** (Gherkin), and **boto3** — producing rich, self-contained HTML reports with styled tables and expandable log sections.

---

## Architecture Overview

```
                  ┌──────────────────────────────────────────────┐
                  │              QA Automation Framework         │
                  └──────────────┬───────────────────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────────┐
        │                        │                            │
   AC-Digital-BAT           AC-DATA-ODH-UAT           Digital-ODS BATCA1
   (Lambda / Glue)          (S3 Event Store)          (MySQL via SSH)
        │                        │                            │
  ┌─────┴──────┐          ┌──────┴──────┐             ┌───────┴───────┐
  │ Lambda     │          │ S3 Bucket   │             │ FlightLeg     │
  │ Invocation │          │ Validation  │             │ GroundTime DB │
  │ CloudWatch │          │ JSON Schema │             │ SSH Tunnel    │
  │ Glue Jobs  │          │ Event Types │             │ Query + Assert│
  └────────────┘          └─────────────┘             └───────────────┘
```

---

## Test Suites — 74 Scenarios

| Suite | Feature File | Scenarios | AWS Account | What It Validates |
|-------|-------------|:---------:|-------------|-------------------|
| **ADM Events** | `adm_validation.feature` | 9 | AC-Digital-BAT / AC-DATA-ODH-UAT | ADM Lambda ingestion, CloudWatch log search, failed ingestion guard, S3 event schema (`ADMInfo`), Carousel, Id format, DepartureAirport structure |
| **FDM Events** | `fdm_validation.feature` | 10 | AC-Digital-BAT / AC-DATA-ODH-UAT | FDM Lambda ingestion, CloudWatch log search, failed ingestion guard, S3 event schema (`FDMInfo`), Id format, flight status codes, origin/destination airports |
| **Ground Time** | `ground_time_validation.feature` | 19 | AC-Digital-BAT / AC-DATA-ODH-UAT / Digital-ODS | Glue job existence & 30-min schedule, S3 `FlightGroundTimeInfo` schema validation, MySQL DB connectivity via SSH tunnel, DB-to-S3 data reconciliation |
| **CloudWatch Logs** | `cloudwatch_logs.feature` | 7 | AC-DATA-ODH-UAT | Lambda invocation, execution status/message parsing, record counts, CloudWatch log group access, success message search |
| **S3 Validation** | `s3_validation.feature` | 12 | AC-DATA-ODH-UAT | Aircraft JSON event files in S3 — structure, field presence, naming patterns, data types |
| **S3 Lambda BDD** | `s3.feature` | 17 | Mocked (moto) | Aircraft Info Lambda behavior — DB extraction, retry logic, S3 storage |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| BDD / Gherkin | pytest-bdd |
| Test Runner | pytest |
| AWS SDK | boto3 |
| Authentication | AWS SSO (multi-profile) |
| Unit Mocks | moto |
| Local Cloud | LocalStack (Docker) |
| Reporting | pytest-html (self-contained HTML with styled tables) |
| CI | GitHub Actions |
| DB Access | mysql-connector-python + sshtunnel |

---

## Project Structure

```
QA_Automation_Test/
├── README.md
├── pyproject.toml                          # Build config, pytest settings, markers
├── requirements-dev.txt                    # Dev dependencies
├── docker-compose.localstack.yml           # LocalStack for integration tests
│
├── src/
│   └── aws_testkit/                        # Reusable AWS client library
│       ├── __init__.py
│       ├── config.py                       # Settings from env vars (region, profile, bucket)
│       ├── aws_clients.py                  # S3Client, CloudWatchLogsClient wrappers
│       ├── xray_client.py                  # X-Ray trace client
│       └── utils.py                        # Helpers (unique name generation)
│
├── tests/
│   ├── conftest.py                         # Shared fixtures, report hooks
│   ├── test_aws_connectivity.py            # AWS credential & connectivity smoke tests
│   │
│   ├── unit/
│   │   └── test_s3_unit.py                 # Fast moto-backed unit tests
│   │
│   ├── integration/
│   │   └── test_s3_integration.py          # LocalStack integration tests
│   │
│   └── features/                           # BDD feature files (Gherkin)
│       ├── adm_validation.feature          #   ADM event validation (9 scenarios)
│       ├── fdm_validation.feature          #   FDM event validation (10 scenarios)
│       ├── ground_time_validation.feature  #   Glue + S3 + DB validation (19 scenarios)
│       ├── cloudwatch_logs.feature         #   Lambda + CloudWatch validation (7 scenarios)
│       ├── s3_validation.feature           #   S3 aircraft event validation (12 scenarios)
│       ├── s3.feature                      #   Mocked Lambda BDD scenarios (17 scenarios)
│       └── steps/                          # Step definitions (Python)
│           ├── test_adm_validation_steps.py
│           ├── test_fdm_validation_steps.py
│           ├── test_ground_time_validation_steps.py
│           ├── test_cloudwatch_logs_steps.py
│           ├── test_s3_validation_steps.py
│           └── test_s3_steps.py
│
├── reports/                                # Auto-generated HTML reports
└── .github/
    └── workflows/
        └── ci.yml                          # CI pipeline
```

---

## Prerequisites

| Requirement | Version | Purpose |
|-------------|---------|---------|
| Python | 3.11+ | Runtime |
| AWS CLI v2 | Latest | SSO login & profile management |
| Docker | Latest | LocalStack (optional, for integration tests) |
| SSH access | — | MySQL tunnel to Digital-ODS (for ground time DB tests) |

---

## Quick Start

### 1. Clone and install

```bash
git clone <repo-url> && cd QA_Automation_Test
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -e ".[dev]"
pip install pytest-html mysql-connector-python sshtunnel
```

### 2. Configure AWS SSO profiles

The framework uses **two AWS SSO profiles** that auto-switch per service:

| Profile | AWS Account | Used For |
|---------|-------------|----------|
| `AC_Digital_BAT` | AC-Digital-BAT | Lambda invocation, Glue job validation, CloudWatch Logs |
| `ODH_UAT` | AC-DATA-ODH-UAT | S3 derived event store reads |

Configure each profile:

```bash
aws configure sso --profile AC_Digital_BAT
aws configure sso --profile ODH_UAT
```

### 3. Login to SSO

```bash
aws sso login --profile AC_Digital_BAT
aws sso login --profile ODH_UAT
```

Verify:

```bash
aws sts get-caller-identity --profile AC_Digital_BAT
aws sts get-caller-identity --profile ODH_UAT
```

---

## Running Tests

### ADM Event Validation

Validates ADM Lambda ingestion, CloudWatch logs, and S3 `ADMInfo` event schema.

```powershell
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
py -m pytest tests/features/steps/test_adm_validation_steps.py -v `
  --html="reports/adm_report_$ts.html" --self-contained-html
```

**Scenarios:**
- Lambda invocation with Kafka test event (status 200)
- CloudWatch log search by request ID — ingestion success confirmation
- Failed ingestion with invalid event — error in logs, no S3 files stored
- S3 files exist under `CDM/processed/CDM-FLIGHT-ODH-UAT/`
- Random current-date `ADMInfo` event — valid JSON, schema checks
- `eventType` = `"ADMInfo"`, `Carousel` (int or null), `Id` format (`AC-400-2026-03-30-YYZ`)
- `DepartureAirport` structure: `IATACode`, `Terminal`, `Gate`

### FDM Event Validation

Validates FDM Lambda ingestion, CloudWatch logs, and S3 `FDMInfo` event schema.

```powershell
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
py -m pytest tests/features/steps/test_fdm_validation_steps.py -v `
  --html="reports/fdm_report_$ts.html" --self-contained-html
```

**Scenarios:**
- Lambda invocation with Kafka test event (status 200)
- CloudWatch log search — ingestion success (46 log events)
- Failed ingestion guard — error confirmed, no new S3 files
- `eventType` = `"FDMInfo"`, `Id` format validation
- Status fields: `FlightStateCode`, `DepartureStatus.Code`, `ArrivalStatus.Code`, `OverallStatus.Code`
- Origin (`DepartureAirport`) and destination (`ArrivalAirport`) with valid IATA codes

### Ground Time Validation

Validates Glue ETL job, S3 output, and MySQL database data.

```powershell
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
py -m pytest tests/features/steps/test_ground_time_validation_steps.py -v `
  --html="reports/ground_time_report_$ts.html" --self-contained-html
```

**Scenarios:**
- Glue job `ac-odh-flight-leg-ground-times-mst-batca1-data-job` exists with 30-minute schedule
- S3 `FlightGroundTimeInfo` events — schema, field types, time formats, nested structures
- MySQL connectivity via SSH tunnel to Digital-ODS BATCA1
- `FlightLegGroundTime` table existence, schema, row counts
- DB-to-S3 data reconciliation for specific flight legs

### CloudWatch Logs Validation

```powershell
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
py -m pytest tests/features/steps/test_cloudwatch_logs_steps.py -v `
  --html="reports/cloudwatch_report_$ts.html" --self-contained-html
```

### S3 Aircraft Event Validation

```powershell
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
py -m pytest tests/features/steps/test_s3_validation_steps.py -v `
  --html="reports/s3_validation_report_$ts.html" --self-contained-html
```

### Run All Feature Tests

```powershell
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
py -m pytest tests/features/ -v `
  --html="reports/full_report_$ts.html" --self-contained-html
```

### Run Specific Scenarios by Keyword

```powershell
py -m pytest tests/features/steps/test_adm_validation_steps.py -v -k "CloudWatch"
py -m pytest tests/features/steps/test_ground_time_validation_steps.py -v -k "not Glue"
```

### Unit Tests (no AWS credentials needed)

```bash
py -m pytest -m unit -v
```

---

## HTML Reports

Reports are generated as **self-contained HTML** files in the `reports/` directory, timestamped for traceability.

### Report Features

- **Styled HTML tables** for query results, event data, and log events
- **Expandable sections** for Lambda log tails and full event JSON
- **Color-coded** pass/fail with clear error messages
- **Key-value tables** for Lambda responses, Glue job config, DB connection details

### Sample Report Sections

| Section | Content |
|---------|---------|
| Lambda Response | Status code, request ID, response body |
| CloudWatch Logs | Timestamped log events in a scrollable table |
| Lambda Log Tail | Last 4KB of execution logs (collapsible) |
| S3 Event JSON | Full pretty-printed event with syntax highlighting |
| DB Query Results | Table rows rendered as HTML with column headers |
| Glue Job Config | Job name, schedule, state, trigger details |

---

## AWS Profiles & Auto-Switching

The framework automatically selects the correct AWS profile per service:

```
Lambda / Glue / CloudWatch  →  AC_Digital_BAT  (AC-Digital-BAT account)
S3 Reads                    →  ODH_UAT         (AC-DATA-ODH-UAT account)
```

Override via environment variables:

```powershell
$env:LAMBDA_AWS_PROFILE = "MyCustomLambdaProfile"
$env:S3_AWS_PROFILE = "MyCustomS3Profile"
$env:GLUE_AWS_PROFILE = "MyCustomGlueProfile"
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LAMBDA_AWS_PROFILE` | `AC_Digital_BAT` | Profile for Lambda, CloudWatch, Glue |
| `S3_AWS_PROFILE` | `ODH_UAT` | Profile for S3 event store reads |
| `GLUE_AWS_PROFILE` | `AC_Digital_BAT` | Profile for Glue job validation |
| `AWS_REGION` | `ca-central-1` | AWS region |
| `DB_SSH_HOST` | `127.0.0.1` | SSH tunnel host for MySQL |
| `DB_SSH_PORT` | `13408` | SSH tunnel port |
| `DB_SSH_USER` | `ec2devuser` | SSH username |
| `DB_HOST` | *(configured in steps)* | MySQL RDS hostname |
| `DB_USER` | `dbdevuser` | MySQL username |

---

## Key Validation Patterns

### Event Type Checks
```gherkin
Then the ADM event field "eventType" should equal "ADMInfo"
Then the FDM event field "eventType" should equal "FDMInfo"
```

### ID Format Validation
```gherkin
# Pattern: {Carrier}-{Number}-{YYYY-MM-DD}-{Airport}
# Example: AC-400-2026-03-30-YYZ
Then the ADM event "Id" should match pattern "Carrier-Number-Date-Airport"
```

### Airport Structure
```gherkin
Then FDM "DepartureAirport.IATACode" should be a valid 3-letter IATA code
And FDM "DepartureAirport.Gate" should be present in the event
```

### CloudWatch Log Search
```gherkin
When I search FDM CloudWatch logs for the Lambda request ID
Then the FDM CloudWatch logs should contain ingestion success
```

### Failed Ingestion Guard
```gherkin
When I invoke the FDM Lambda with an invalid empty event
Then the FDM CloudWatch logs should indicate a processing error
And no new FDM S3 files should be stored for the invalid event
```

---

## Troubleshooting

### SSO Token Expired

```
botocore.exceptions.TokenRetrievalError: Token has expired and refresh failed
```

**Fix:** Re-login to both profiles:

```bash
aws sso login --profile AC_Digital_BAT
aws sso login --profile ODH_UAT
```

### AccessDenied on S3

Ensure your IAM role has `s3:ListBucket` and `s3:GetObject` on the target bucket. Verify you are using the correct profile (`ODH_UAT` for S3).

### No FDMInfo/ADMInfo Events Found

The S3 prefix contains mixed event types. The framework scans up to 20,000 objects and randomly selects from today's files, retrying up to 50 times. If tests are flaky, the data volume for that event type may be low on the current date.

### SSH Tunnel Failures (Ground Time DB)

Ensure port `13408` is open and the SSH credentials are valid. The tunnel connects to the Digital-ODS BATCA1 MySQL RDS cluster.

### Proxy Issues with pip

```powershell
$env:HTTP_PROXY=""; $env:HTTPS_PROXY=""
pip install -e ".[dev]"
```

---

## CI / GitHub Actions

The `.github/workflows/ci.yml` pipeline runs unit and mocked BDD tests on every push. Integration and live AWS tests require SSO credentials and are gated to manual or scheduled runs.

---

## License

MIT
