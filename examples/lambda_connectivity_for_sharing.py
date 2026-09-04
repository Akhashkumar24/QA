"""
AWS Lambda connectivity — patterns extracted from QA_Automation_Test
=====================================================================

Your friend can copy this file as-is (needs: pip install boto3) and set credentials
via ~/.aws/credentials, AWS_PROFILE, or SSO (aws sso login --profile <name>).

Framework sources (for reference):
  - tests/features/steps/test_adm_validation_steps.py   — profile-based Lambda + invoke + LogType Tail
  - tests/features/steps/test_aircraft_event_validation_steps.py — Settings + profile/env chain
  - src/aws_testkit/credential_refresh.py             — optional retry after SSO token expiry
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any

import boto3


# ---------------------------------------------------------------------------
# 1) Build a Lambda client (named profile — same idea as LAMBDA_AWS_PROFILE in ADM tests)
# ---------------------------------------------------------------------------

def lambda_client_for_profile(
    region: str,
    profile: str | None = None,
) -> Any:
    """Use a named CLI profile (e.g. AC_Digital_BAT, ODH_UAT)."""
    profile = profile or os.getenv("AWS_PROFILE", "default")
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("lambda")


def lambda_client_from_default_chain(region: str) -> Any:
    """Use default credential chain (env vars, shared credentials, SSO, instance role)."""
    return boto3.client("lambda", region_name=region)


# ---------------------------------------------------------------------------
# 2) Synchronous invoke (RequestResponse) — same pattern as ADM / aircraft tests
# ---------------------------------------------------------------------------

def invoke_lambda_sync(
    client: Any,
    function_name: str,
    payload: dict[str, Any],
    *,
    log_type: str | None = "Tail",
) -> dict[str, Any]:
    """
    Invoke Lambda and return a dict with status, body, optional log tail, errors.

    log_type="Tail" asks Lambda to return last 4KB of logs in LogResult (base64).
    """
    raw = json.dumps(payload).encode("utf-8")
    resp = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=raw,
        **({"LogType": log_type} if log_type else {}),
    )

    status_code = resp.get("StatusCode", 0)
    request_id = resp.get("ResponseMetadata", {}).get("RequestId", "")
    payload_bytes = resp["Payload"].read()
    payload_str = payload_bytes.decode("utf-8")

    log_tail = ""
    log_b64 = resp.get("LogResult")
    if log_b64:
        try:
            log_tail = base64.b64decode(log_b64).decode("utf-8", errors="replace")
        except Exception:
            log_tail = str(log_b64)

    out: dict[str, Any] = {
        "status_code": status_code,
        "request_id": request_id,
        "function_error": resp.get("FunctionError"),
        "raw_response": payload_str,
        "log_tail": log_tail,
    }

    try:
        out["parsed"] = json.loads(payload_str)
    except json.JSONDecodeError:
        out["parsed"] = {"raw": payload_str}

    return out


# ---------------------------------------------------------------------------
# 3) Minimal CLI-style usage (optional)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Invoke an AWS Lambda function (boto3).")
    parser.add_argument("function_name", help="Lambda function name or ARN")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ca-central-1"))
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument("--payload", default='{"key":"value"}', help="JSON string")
    args = parser.parse_args()

    client = (
        lambda_client_for_profile(args.region, args.profile)
        if args.profile
        else lambda_client_from_default_chain(args.region)
    )
    result = invoke_lambda_sync(client, args.function_name, json.loads(args.payload))
    print(json.dumps(result, indent=2, default=str))
