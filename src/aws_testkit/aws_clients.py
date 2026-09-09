"""boto3 client factory + service wrappers with transparent SSO refresh.

``build_client`` is the one place a :class:`~aws_testkit.config.Settings` (plus an
optional per-call profile/region override) becomes a boto3 client. Every wrapper
extends :class:`_Service`, whose ``call()`` retries an operation once after
running ``aws sso login`` when it sees an expired-token error — so long test runs
survive SSO session expiry without babysitting.
"""

from __future__ import annotations

import base64
import json
import logging
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from aws_testkit.config import Settings
from aws_testkit.credential_refresh import is_expired_token_error, try_refresh_sso_credentials

logger = logging.getLogger("aws_testkit.clients")

_RETRY_CONFIG = BotoConfig(retries={"max_attempts": 5, "mode": "standard"})


def build_client(
    service: str,
    *,
    settings: Settings | None = None,
    profile: str | None = None,
    region: str | None = None,
) -> Any:
    """Return a boto3 client for *service*.

    ``profile`` / ``region`` override *settings* for this one client (different
    AWS accounts back different services).
    """
    settings = settings or Settings()
    profile = profile if profile is not None else settings.aws_profile
    kwargs: dict[str, Any] = {
        "service_name": service,
        "region_name": region or settings.aws_region,
        "config": _RETRY_CONFIG,
    }
    if settings.aws_access_key_id is not None:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key is not None:
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_session_token is not None:
        kwargs["aws_session_token"] = settings.aws_session_token
    if settings.aws_endpoint_url:
        kwargs["endpoint_url"] = settings.aws_endpoint_url
    if profile:
        return boto3.Session(profile_name=profile).client(**kwargs)
    return boto3.client(**kwargs)


class _Service:
    """Base for the service wrappers: holds a client, re-logs-in on token expiry."""

    service: str = ""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        profile: str | None = None,
        region: str | None = None,
    ) -> None:
        self.settings = settings or Settings()
        self.profile = profile if profile is not None else self.settings.aws_profile
        self.region = region or self.settings.aws_region
        self._client = self._build()

    def _build(self) -> Any:
        return build_client(
            self.service, settings=self.settings, profile=self.profile, region=self.region
        )

    @property
    def raw(self) -> Any:
        """The underlying boto3 client (for operations the wrapper doesn't cover)."""
        return self._client

    def call(self, fn: Callable[[Any], Any]) -> Any:
        """Run ``fn(client)``; on an expired-token error, ``aws sso login`` + retry once."""
        try:
            return fn(self._client)
        except Exception as exc:  # noqa: BLE001 - re-raised unless it's a token expiry
            if not (self.profile and is_expired_token_error(exc)):
                raise
            logger.warning(
                "%s: credentials expired — running 'aws sso login --profile %s'",
                self.service, self.profile,
            )
            if not try_refresh_sso_credentials(self.profile):
                raise
            self._client = self._build()
            return fn(self._client)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

class S3Client(_Service):
    service = "s3"

    # -- objects --------------------------------------------------------

    def put_object(self, bucket: str, key: str, body: bytes | str) -> dict[str, Any]:
        data = body.encode("utf-8") if isinstance(body, str) else body
        return self.call(lambda c: c.put_object(Bucket=bucket, Key=key, Body=data))

    def get_object(self, bucket: str, key: str) -> bytes:
        return self.call(lambda c: c.get_object(Bucket=bucket, Key=key)["Body"].read())

    def get_json(self, bucket: str, key: str) -> Any:
        return json.loads(self.get_object(bucket, key).decode("utf-8"))

    def delete_object(self, bucket: str, key: str) -> None:
        self.call(lambda c: c.delete_object(Bucket=bucket, Key=key))

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self.call(lambda c: c.head_bucket(Bucket=bucket))
            return True
        except ClientError:
            return False

    # -- listing / selection -----------------------------------------

    def list_objects(
        self,
        bucket: str,
        prefix: str,
        *,
        suffix: str = "",
        page_size: int = 1000,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """All objects under *prefix* (optionally ending *suffix*), paginated."""
        out: list[dict[str, Any]] = []
        token: str | None = None
        pages = 0
        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": page_size}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.call(lambda c, k=kwargs: c.list_objects_v2(**k))
            out.extend(o for o in resp.get("Contents", []) if o["Key"].endswith(suffix))
            pages += 1
            if not resp.get("IsTruncated") or (max_pages and pages >= max_pages):
                return out
            token = resp.get("NextContinuationToken")

    def latest_object(self, bucket: str, prefix: str, *, suffix: str = ".json") -> dict[str, Any] | None:
        objs = self.list_objects(bucket, prefix, suffix=suffix)
        return max(objs, key=lambda o: o["LastModified"]) if objs else None

    def find_event(
        self,
        bucket: str,
        prefix: str,
        *,
        predicate: Callable[[Any], bool] | None = None,
        current_date_only: bool = True,
        pick: str = "random",
        max_pages: int = 25,
        max_tries: int = 50,
    ) -> tuple[str, Any]:
        """One JSON object under *prefix* whose parsed body satisfies *predicate*.

        Raises :class:`LookupError` with a diagnostic if nothing matches.
        """
        today = datetime.now(UTC).date()
        objs = self.list_objects(bucket, prefix, suffix=".json", max_pages=max_pages)
        if not objs:
            raise LookupError(f"No JSON objects under s3://{bucket}/{prefix}")
        candidates = objs
        if current_date_only:
            todays = [o for o in objs if o["LastModified"].date() == today]
            if todays:
                candidates = todays
            else:
                logger.warning(
                    "no objects dated %s under s3://%s/%s — falling back to all %d",
                    today, bucket, prefix, len(objs),
                )
        candidates = (
            sorted(candidates, key=lambda o: o["LastModified"], reverse=True)
            if pick == "latest"
            else random.sample(candidates, k=len(candidates))
        )
        skipped: list[str] = []
        for obj in candidates[:max_tries]:
            try:
                doc = self.get_json(bucket, obj["Key"])
            except Exception as exc:  # noqa: BLE001 - unreadable / not JSON, try next
                skipped.append(f"{obj['Key']}: {exc}")
                continue
            if predicate is None or predicate(doc):
                return obj["Key"], doc
        raise LookupError(
            f"tried {min(max_tries, len(candidates))} object(s) under "
            f"s3://{bucket}/{prefix}; none matched the predicate"
            + (f"; skipped {skipped[:3]}" if skipped else "")
        )

    def get_lifecycle_rules(self, bucket: str) -> list[dict[str, Any]]:
        try:
            return self.call(
                lambda c: c.get_bucket_lifecycle_configuration(Bucket=bucket)
            ).get("Rules", [])
        except ClientError as exc:
            if "NoSuchLifecycleConfiguration" in str(exc):
                return []
            raise


# ---------------------------------------------------------------------------
# CloudWatch Logs
# ---------------------------------------------------------------------------

class CloudWatchLogsClient(_Service):
    service = "logs"

    def log_group_exists(self, name: str) -> bool:
        try:
            resp = self.call(lambda c: c.describe_log_groups(logGroupNamePrefix=name))
            return any(g["logGroupName"] == name for g in resp.get("logGroups", []))
        except ClientError:
            return False

    def describe_log_group(self, name: str) -> dict[str, Any] | None:
        resp = self.call(lambda c: c.describe_log_groups(logGroupNamePrefix=name, limit=50))
        return next((g for g in resp.get("logGroups", []) if g["logGroupName"] == name), None)

    def recent_streams(self, name: str, *, limit: int = 5) -> list[dict[str, Any]]:
        return self.call(
            lambda c: c.describe_log_streams(
                logGroupName=name, orderBy="LastEventTime", descending=True, limit=limit
            )
        ).get("logStreams", [])

    def stream_events(self, group: str, stream: str, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.call(
            lambda c: c.get_log_events(
                logGroupName=group, logStreamName=stream, limit=limit, startFromHead=False
            )
        ).get("events", [])

    def filter_events(
        self,
        group: str,
        *,
        pattern: str = "",
        limit: int = 100,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {"logGroupName": group, "limit": limit, "interleaved": True}
        if pattern:
            kwargs["filterPattern"] = pattern
        if start_time is not None:
            kwargs["startTime"] = start_time
        if end_time is not None:
            kwargs["endTime"] = end_time
        events: list[dict[str, Any]] = []
        while True:
            resp = self.call(lambda c, k=kwargs: c.filter_log_events(**k))
            events.extend(resp.get("events", []))
            token = resp.get("nextToken")
            if not token or len(events) >= limit:
                return events[:limit]
            kwargs["nextToken"] = token

    def search_request_id(
        self,
        group: str,
        request_id: str,
        *,
        lookback_seconds: int = 120,
        initial_wait: float = 10.0,
        retries: int = 5,
    ) -> list[dict[str, Any]]:
        """Poll ``filter_log_events`` for a Lambda request id (logs lag ingestion)."""
        start_ms = int((datetime.now(UTC).timestamp() - lookback_seconds) * 1000)
        time.sleep(initial_wait)
        for attempt in range(1, retries + 1):
            try:
                events = self.filter_events(
                    group, pattern=f'"{request_id}"', start_time=start_ms, limit=100
                )
                if events:
                    return events
            except ClientError as exc:
                logger.warning("filter_log_events attempt %d failed: %s", attempt, exc)
            if attempt < retries:
                time.sleep(10 * attempt)
        return []


# ---------------------------------------------------------------------------
# Lambda
# ---------------------------------------------------------------------------

class LambdaClient(_Service):
    service = "lambda"

    def invoke(self, name: str, payload: Any, *, log_type: str = "Tail") -> dict[str, Any]:
        """Synchronous invoke → normalised dict.

        Keys: ``status_code``, ``request_id``, ``function_error``, ``raw``,
        ``response`` (parsed; nested JSON ``body`` merged up), ``log_tail``,
        ``execution_status``.
        """
        resp = self.call(
            lambda c: c.invoke(
                FunctionName=name,
                InvocationType="RequestResponse",
                LogType=log_type,
                Payload=json.dumps(payload).encode("utf-8"),
            )
        )
        raw = resp["Payload"].read().decode("utf-8")
        out: dict[str, Any] = {
            "status_code": resp.get("StatusCode", 0),
            "request_id": resp.get("ResponseMetadata", {}).get("RequestId", ""),
            "function_error": resp.get("FunctionError"),
            "raw": raw,
            "log_tail": "",
        }
        if resp.get("LogResult"):
            try:
                out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                out["log_tail"] = str(resp["LogResult"])
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            if isinstance(parsed, dict) and isinstance(parsed.get("body"), str):
                try:
                    body = json.loads(parsed["body"])
                    if isinstance(body, dict):
                        parsed = {**parsed, **body}
                except (json.JSONDecodeError, TypeError):
                    pass
            out["response"] = parsed if isinstance(parsed, dict) else {"value": parsed}
        except (json.JSONDecodeError, TypeError):
            out["response"] = {"raw": raw}
        out["execution_status"] = "Failed" if out["function_error"] else "Succeeded"
        return out

    def get_function(self, name: str) -> dict[str, Any]:
        return self.call(lambda c: c.get_function(FunctionName=name))

    def list_tags(self, arn: str) -> dict[str, str]:
        return self.call(lambda c: c.list_tags(Resource=arn)).get("Tags", {})

    def event_source_mappings(self, name: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for page in self.call(
            lambda c: c.get_paginator("list_event_source_mappings").paginate(FunctionName=name)
        ):
            out.extend(page.get("EventSourceMappings", []))
        return out


# ---------------------------------------------------------------------------
# Glue
# ---------------------------------------------------------------------------

class GlueClient(_Service):
    service = "glue"

    def get_job(self, name: str) -> dict[str, Any] | None:
        try:
            return self.call(lambda c: c.get_job(JobName=name)).get("Job", {})
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                return None
            raise

    def triggers_for_job(self, job_name: str) -> list[dict[str, Any]]:
        """Every trigger whose actions target *job_name*."""
        out: list[dict[str, Any]] = []
        for page in self.call(lambda c: c.get_paginator("get_triggers").paginate()):
            for trig in page.get("Triggers", []):
                if any(a.get("JobName") == job_name for a in trig.get("Actions", [])):
                    out.append(trig)
        return out


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------

class SqsClient(_Service):
    service = "sqs"

    def queue_url(self, name: str) -> str:
        return self.call(lambda c: c.get_queue_url(QueueName=name))["QueueUrl"]

    def attributes(self, url: str, names: list[str]) -> dict[str, str]:
        return self.call(
            lambda c: c.get_queue_attributes(QueueUrl=url, AttributeNames=names)
        ).get("Attributes", {})

    def send_message(self, url: str, body: str) -> dict[str, Any]:
        return self.call(lambda c: c.send_message(QueueUrl=url, MessageBody=body))

    def receive_message(self, url: str, *, wait: int = 10, max_messages: int = 10) -> list[dict[str, Any]]:
        return self.call(
            lambda c: c.receive_message(
                QueueUrl=url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait,
                AttributeNames=["All"],
            )
        ).get("Messages", [])

    def delete_message(self, url: str, receipt_handle: str) -> None:
        self.call(lambda c: c.delete_message(QueueUrl=url, ReceiptHandle=receipt_handle))


# ---------------------------------------------------------------------------
# STS — used by preflight
# ---------------------------------------------------------------------------

class StsClient(_Service):
    service = "sts"

    def caller_identity(self) -> dict[str, str]:
        return self.call(lambda c: c.get_caller_identity())
