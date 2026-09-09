"""Declarative value checks + a tiny schema validator.

A use-case schema (from ``conf/usecases/<id>.yaml``) looks like::

    schema:
      record_index: 0          # optional: if the doc is a list, validate this element
      records: true            # optional: if the doc is a list, validate EVERY element
      fields:
        "eventType":               {equals: "ADMInfo"}
        "Carousel":                {type: int, nullable: true}
        "Id":                      {regex: "^[A-Z]{2}-\\d+-\\d{4}-\\d{2}-\\d{2}-[A-Z]{3}$"}
        "DepartureAirport":        {type: dict}
        "DepartureAirport.IATACode": {iata: true}
        "DepartureAirport.Gate":   {present: true}
        "ScheduledGroundTime":     {time: true}
        "GoTime":                  {iso8601_utc: true}
      cross:
        - {left: "InboundFlight.ArrivalAirport.IATACode", equals: "Flight.DepartureAirport.IATACode"}

``validate`` returns a list of human-readable error strings (empty == pass).
Run ``python -m aws_testkit.checks`` for a self-check.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

# ponytail: hand-rolled check registry (covers everything the current suites
# assert). Swap for `jsonschema` if use-case schemas outgrow this vocabulary.
_IATA = re.compile(r"^[A-Z]{3}$")
_COUNTRY = re.compile(r"^[A-Z]{2}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIME = re.compile(r"^\d{2}:\d{2}:\d{2}$")
_ISO8601_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

_TYPES: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "str": str,
    "dict": dict,
    "object": dict,
    "list": list,
    "array": list,
    "int": int,
    "integer": int,
    "float": float,
    "number": (int, float),
    "bool": bool,
    "boolean": bool,
}

_MISSING = object()


def to_minutes(value: str) -> int:
    """'HH:MM:SS' or ISO-8601 datetime -> minutes-from-midnight."""
    if "T" in value:
        return _dt_minutes(value)
    h, m, *_ = value.split(":")
    return int(h) * 60 + int(m)


def _dt_minutes(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.hour * 60 + dt.minute


def resolve(doc: Any, dot_path: str) -> tuple[Any, bool]:
    """Navigate ``a.b.c`` through nested mappings. Returns (value, found).

    A literal key that itself contains dots (some publishers store flattened
    dotted keys) is matched first, before falling back to nested traversal.
    """
    if isinstance(doc, dict) and dot_path in doc:
        return doc[dot_path], True
    cur = doc
    for key in dot_path.split("."):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return None, False
    return cur, True


def _check_field(path: str, value: Any, found: bool, spec: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    nullable = bool(spec.get("nullable"))
    optional = bool(spec.get("optional"))

    if not found:
        if not optional:
            errs.append(f"{path}: missing")
        return errs
    if value is None:
        if not (nullable or optional):
            errs.append(f"{path}: is null")
        return errs

    for key, param in spec.items():
        if key in ("nullable", "optional"):
            continue
        if key == "present":
            continue  # already satisfied by `found`
        if key == "equals":
            if value != param and str(value) != str(param):
                errs.append(f"{path}: expected {param!r}, got {value!r}")
        elif key == "one_of":
            if value not in param and str(value) not in [str(p) for p in param]:
                errs.append(f"{path}: {value!r} not in {param!r}")
        elif key == "regex":
            if not (isinstance(value, str) and re.match(param, value)):
                errs.append(f"{path}: {value!r} does not match /{param}/")
        elif key == "type":
            py = _TYPES.get(param)
            if py is None:
                errs.append(f"{path}: unknown type {param!r} in schema")
            elif py is int and isinstance(value, bool):
                errs.append(f"{path}: expected {param}, got bool")
            elif not isinstance(value, py):
                errs.append(f"{path}: expected {param}, got {type(value).__name__}")
        elif key == "non_empty":
            if not (isinstance(value, str) and value.strip()):
                errs.append(f"{path}: expected non-empty string, got {value!r}")
        elif key == "iata":
            if not (isinstance(value, str) and _IATA.match(value)):
                errs.append(f"{path}: {value!r} is not a 3-letter IATA code")
        elif key == "country":
            if not (isinstance(value, str) and _COUNTRY.match(value)):
                errs.append(f"{path}: {value!r} is not a 2-letter country code")
        elif key == "date":
            if not (isinstance(value, str) and _DATE.match(value)):
                errs.append(f"{path}: {value!r} is not YYYY-MM-DD")
        elif key == "time":
            if not (isinstance(value, str) and _TIME.match(value)):
                errs.append(f"{path}: {value!r} is not HH:MM:SS")
        elif key == "iso8601_utc":
            if not (isinstance(value, str) and _ISO8601_UTC.match(value)):
                errs.append(f"{path}: {value!r} is not ISO-8601 UTC")
        elif key == "uuid":
            if not (isinstance(value, str) and _UUID.match(value)):
                errs.append(f"{path}: {value!r} is not a UUID")
        elif key == "minutes_equals":
            try:
                got, want = int(value), to_minutes(str(param))
            except (TypeError, ValueError):
                errs.append(f"{path}: {value!r} is not an integer minute count")
            else:
                if got != want:
                    errs.append(f"{path}: {got} min != {want} min (from {param!r})")
        else:
            errs.append(f"{path}: unknown check {key!r} in schema")
    return errs


def _validate_one(doc: Any, schema: dict[str, Any], where: str = "") -> list[str]:
    errs: list[str] = []
    prefix = f"{where}: " if where else ""
    for path, spec in (schema.get("fields") or {}).items():
        if not isinstance(spec, dict):
            spec = {"equals": spec}
        value, found = resolve(doc, path)
        errs.extend(prefix + e for e in _check_field(path, value, found, spec))
    for rule in schema.get("cross") or []:
        left = rule["left"]
        lv, lf = resolve(doc, left)
        if "equals" in rule:
            right = rule["equals"]
            rv, rf = resolve(doc, right)
            if not (lf and rf):
                errs.append(f"{prefix}cross: {left!r} or {right!r} missing")
            elif lv != rv:
                errs.append(f"{prefix}cross: {left}={lv!r} != {right}={rv!r}")
    return errs


def _apply_root(doc: Any, roots: Any) -> tuple[Any, str | None]:
    """Descend into the first of ``roots`` that resolves to a mapping."""
    if not roots:
        return doc, None
    if isinstance(roots, str):
        roots = [roots]
    for r in roots:
        val, found = resolve(doc, r)
        if found and isinstance(val, dict):
            return val, r
    return doc, f"none of roots {roots!r} resolved to an object"


def validate(doc: Any, schema: dict[str, Any]) -> list[str]:
    """Return a list of error strings; empty means the document conforms."""
    if not schema:
        return []
    if "root" in schema:
        doc, err = _apply_root(doc, schema["root"])
        if err:
            return [err]
    if isinstance(doc, list):
        if schema.get("record_index") is not None:
            i = schema["record_index"]
            if i >= len(doc):
                return [f"record_index {i} out of range ({len(doc)} records)"]
            return _validate_one(doc[i], schema, f"record[{i}]")
        if schema.get("records"):
            if not doc:
                return ["document is an empty array"]
            out: list[str] = []
            for i, rec in enumerate(doc):
                out.extend(_validate_one(rec, schema, f"record[{i}]"))
            return out
    return _validate_one(doc, schema)


def demo() -> None:
    good = {
        "eventType": "ADMInfo",
        "Carousel": None,
        "Id": "AC-400-2026-03-30-YYZ",
        "DepartureAirport": {"IATACode": "YYZ", "Gate": "D28"},
        "Flight": {"DepartureAirport": {"IATACode": "YHZ"}},
        "InboundFlight": {"ArrivalAirport": {"IATACode": "YHZ"}},
        "GoTime": "2026-03-21T15:55:00.000Z",
    }
    schema = {
        "fields": {
            "eventType": {"equals": "ADMInfo"},
            "Carousel": {"type": "int", "nullable": True},
            "Id": {"regex": r"^[A-Z]{2}-\d+-\d{4}-\d{2}-\d{2}-[A-Z]{3}$"},
            "DepartureAirport": {"type": "dict"},
            "DepartureAirport.IATACode": {"iata": True},
            "DepartureAirport.Gate": {"present": True},
            "GoTime": {"iso8601_utc": True},
        },
        "cross": [
            {
                "left": "InboundFlight.ArrivalAirport.IATACode",
                "equals": "Flight.DepartureAirport.IATACode",
            }
        ],
    }
    assert validate(good, schema) == [], validate(good, schema)

    bad = {**good, "eventType": "FDMInfo", "DepartureAirport": {"IATACode": "yyz"}}
    errs = validate(bad, schema)
    assert any("eventType" in e for e in errs), errs
    assert any("IATACode" in e for e in errs), errs
    assert any("Gate" in e for e in errs), errs  # dropped by the override above

    assert validate([{"x": 1}], {"records": True, "fields": {"x": {"equals": 1}}}) == []
    assert validate([], {"records": True, "fields": {}}) == ["document is an empty array"]
    print("checks.demo OK")


if __name__ == "__main__":
    demo()
