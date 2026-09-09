"""Load, expand and validate a use case from ``conf/usecases/<id>.yaml``.

* ``${VAR}`` / ``${VAR:-default}`` anywhere in the YAML is expanded from the
  environment — this is how secrets and per-environment values get in without
  being committed.
* ``profile:`` values are resolved through ``conf/profiles.yaml`` (a logical
  name like ``s3`` → an AWS profile), falling back to the literal string.
* ``test_event`` / ``invalid_event`` string values are read as JSON files
  (relative to ``conf/``).
* :func:`load_usecase` validates structure and raises :class:`ConfigError` with a
  readable message on any problem.

``conf/`` is ``$AWS_TESTKIT_CONF`` or ``<repo>/conf``.
"""

from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_EVENT_KEYS = {"test_event", "invalid_event"}

# capability -> the config that must be present for that feature to run
CAPABILITIES: dict[str, Any] = {
    "lambda_ingestion":  lambda c: _dig(c, "lambda", "test_event") is not None,
    "failed_ingestion":  lambda c: _dig(c, "lambda", "invalid_event") is not None,
    "lambda_config":     lambda c: _dig(c, "lambda", "config") is not None,
    "s3_presence":       lambda c: bool(_dig(c, "s3", "prefixes")),
    "s3_schema":         lambda c: bool(_dig(c, "s3", "prefixes")) and bool(c.get("schema")),
    "s3_filename":       lambda c: _dig(c, "s3", "filename_regex") is not None,
    "s3_objects":        lambda c: bool(_dig(c, "s3", "objects")),
    "s3_latest_dated":   lambda c: _dig(c, "s3", "date_prefix") is not None,
    "glue":              lambda c: bool(c.get("glue")),
    "db":                lambda c: bool(_dig(c, "db", "checks")),
    "sqs":               lambda c: bool(c.get("sqs")),
    "lifecycle":         lambda c: bool(c.get("lifecycle")),
}


class ConfigError(ValueError):
    """A use-case YAML is missing/invalid. The message says exactly what."""


def conf_dir() -> Path:
    return Path(os.getenv("AWS_TESTKIT_CONF") or Path(__file__).resolve().parents[2] / "conf")


def _dig(cfg: dict, *keys: str) -> Any:
    cur: Any = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _expand(node: Any) -> Any:
    if isinstance(node, str):
        return _ENV_RE.sub(lambda m: os.getenv(m.group(1), m.group(2) or ""), node)
    if isinstance(node, dict):
        return {k: _expand(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_expand(v) for v in node]
    return node


@lru_cache(maxsize=1)
def _profiles() -> dict[str, str]:
    path = conf_dir() / "profiles.yaml"
    return _expand(yaml.safe_load(path.read_text()) or {}) if path.exists() else {}


def resolve_profile(value: str | None) -> str | None:
    """Map a logical profile name to a real one (identity if unmapped)."""
    return _profiles().get(value, value) if value else None


def _resolve_profiles(node: Any) -> Any:
    if isinstance(node, dict):
        return {
            k: (resolve_profile(v) if k == "profile" and isinstance(v, str) else _resolve_profiles(v))
            for k, v in node.items()
        }
    if isinstance(node, list):
        return [_resolve_profiles(v) for v in node]
    return node


def _load_event_refs(node: Any, base: Path) -> Any:
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for k, v in node.items():
            if k in _EVENT_KEYS and isinstance(v, str):
                p = Path(v) if os.path.isabs(v) else base / v
                if not p.exists():
                    raise ConfigError(f"{k}: event file not found: {p}")
                try:
                    out[k] = json.loads(p.read_text())
                except json.JSONDecodeError as exc:
                    raise ConfigError(f"{k}: {p} is not valid JSON: {exc}") from exc
            else:
                out[k] = _load_event_refs(v, base)
        return out
    if isinstance(node, list):
        return [_load_event_refs(v, base) for v in node]
    return node


def load_usecase(usecase_id: str) -> dict[str, Any]:
    """Read, expand, resolve and validate ``conf/usecases/<id>.yaml``."""
    base = conf_dir()
    path = base / "usecases" / f"{usecase_id}.yaml"
    if not path.exists():
        raise ConfigError(f"no such use case: {path}")
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top level must be a mapping")

    cfg = _load_event_refs(_resolve_profiles(_expand(raw)), base)
    cfg.setdefault("id", usecase_id)
    errors = validate_config(cfg)
    if errors:
        raise ConfigError(f"{path} is invalid:\n  - " + "\n  - ".join(errors))
    return cfg


def validate_config(cfg: dict[str, Any]) -> list[str]:
    """Return a list of human-readable problems (empty == OK). Never raises."""
    e: list[str] = []
    if not cfg.get("region"):
        e.append("region: required (e.g. ca-central-1)")

    lam = cfg.get("lambda")
    if lam is not None:
        if not isinstance(lam, dict) or not lam.get("name"):
            e.append("lambda.name: required when a 'lambda:' block is present")
        succ = lam.get("success", {}) if isinstance(lam, dict) else {}
        if succ and not ({"status_code", "execution_status"} & set(succ)):
            e.append("lambda.success: must set status_code or execution_status")
        cf = lam.get("config") if isinstance(lam, dict) else None
        if cf is not None and "env" in cf and not isinstance(cf["env"], dict):
            e.append("lambda.config.env: must be a mapping")

    s3 = cfg.get("s3")
    if s3 is not None:
        if not isinstance(s3, dict) or not s3.get("bucket"):
            e.append("s3.bucket: required when an 's3:' block is present")
        if "prefixes" in s3 and not isinstance(s3["prefixes"], list):
            e.append("s3.prefixes: must be a list")
        for i, obj in enumerate(s3.get("objects", []) or []):
            if not obj.get("key"):
                e.append(f"s3.objects[{i}].key: required")

    e += [f"schema: {m}" for m in _validate_schema(cfg.get("schema"))]
    for i, obj in enumerate((cfg.get("s3", {}) or {}).get("objects", []) or []):
        e += [f"s3.objects[{i}].schema: {m}" for m in _validate_schema(obj.get("schema"))]

    glue = cfg.get("glue")
    if glue is not None and not (isinstance(glue, dict) and glue.get("name") and glue.get("schedule_regex")):
        e.append("glue: needs 'name' and 'schedule_regex'")

    sqs = cfg.get("sqs")
    if sqs is not None and not (isinstance(sqs, dict) and sqs.get("queue") and sqs.get("dlq")):
        e.append("sqs: needs 'queue' and 'dlq'")

    db = cfg.get("db")
    if db is not None:
        for sect in ("ssh", "mysql"):
            if not isinstance(db.get(sect), dict):
                e.append(f"db.{sect}: required mapping when a 'db:' block is present")
        for i, chk in enumerate(db.get("checks", []) or []):
            if not chk.get("query"):
                e.append(f"db.checks[{i}].query: required")

    lc = cfg.get("lifecycle")
    if lc is not None and "expiration_days" not in lc:
        e.append("lifecycle.expiration_days: required")

    if not any(pred(cfg) for pred in CAPABILITIES.values()):
        e.append("declares no runnable capability (add a lambda/s3/glue/db/sqs/lifecycle block)")
    return e


_KNOWN_CHECKS = {
    "present", "equals", "one_of", "regex", "type", "non_empty", "iata", "country",
    "date", "time", "iso8601_utc", "uuid", "minutes_equals", "nullable", "optional",
}


def _validate_schema(schema: Any) -> list[str]:
    if schema is None:
        return []
    if not isinstance(schema, dict):
        return ["must be a mapping"]
    out: list[str] = []
    for path, spec in (schema.get("fields") or {}).items():
        if isinstance(spec, dict):
            unknown = set(spec) - _KNOWN_CHECKS
            if unknown:
                out.append(f"{path}: unknown check(s) {sorted(unknown)}")
    for i, rule in enumerate(schema.get("cross") or []):
        if "left" not in rule or "equals" not in rule:
            out.append(f"cross[{i}]: needs 'left' and 'equals'")
    return out


def capabilities(cfg: dict[str, Any]) -> list[str]:
    """The capability names this use case opts into."""
    return [name for name, pred in CAPABILITIES.items() if pred(cfg)]


def list_usecases() -> list[str]:
    d = conf_dir() / "usecases"
    return sorted(p.stem for p in d.glob("*.yaml") if not p.stem.startswith("_")) if d.exists() else []


def load_all() -> dict[str, Any]:
    """{id: cfg-or-ConfigError} for every use case. Used by the test collector & CLI."""
    out: dict[str, Any] = {}
    for uid in list_usecases():
        try:
            out[uid] = load_usecase(uid)
        except ConfigError as exc:
            out[uid] = exc
    return out
