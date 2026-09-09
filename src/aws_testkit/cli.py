"""``aws-testkit`` — command line for the pipeline automation framework.

    aws-testkit list                 use cases and the capabilities each declares
    aws-testkit show <id>            print the fully-resolved config
    aws-testkit validate [id...]     offline config lint (no AWS)
    aws-testkit preflight [id...]    check AWS reachability/permissions (read-only)
    aws-testkit run [id...] [-- ...] run the pytest suite (extra args after -- go to pytest)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

from aws_testkit.config import Settings
from aws_testkit.usecase import (
    ConfigError,
    capabilities,
    conf_dir,
    list_usecases,
    load_all,
    load_usecase,
)

_OK, _BAD, _WARN = "✓", "✗", "!"


def _targets(ids: list[str]) -> list[str]:
    known = list_usecases()
    if not ids:
        return known
    bad = [i for i in ids if i not in known]
    if bad:
        sys.exit(f"unknown use case(s): {', '.join(bad)} (have: {', '.join(known)})")
    return ids


def cmd_list(_: argparse.Namespace) -> int:
    rows = load_all()
    if not rows:
        print(f"no use cases in {conf_dir() / 'usecases'}")
        return 0
    width = max(len(k) for k in rows)
    for uid, cfg in rows.items():
        if isinstance(cfg, ConfigError):
            print(f"  {_BAD} {uid:<{width}}  INVALID — run 'aws-testkit validate {uid}'")
        else:
            print(f"  {_OK} {uid:<{width}}  {', '.join(capabilities(cfg)) or '(none)'}")
    return 0


def cmd_show(ns: argparse.Namespace) -> int:
    print(json.dumps(load_usecase(ns.id), indent=2, default=str))
    return 0


def cmd_validate(ns: argparse.Namespace) -> int:
    failed = 0
    for uid in _targets(ns.ids):
        try:
            cfg = load_usecase(uid)
        except ConfigError as exc:
            failed += 1
            print(f"{_BAD} {uid}\n    " + str(exc).replace("\n", "\n    "))
            continue
        print(f"{_OK} {uid}  [{', '.join(capabilities(cfg))}]")
    print(f"\n{len(_targets(ns.ids)) - failed} ok, {failed} invalid")
    return 1 if failed else 0


def _probe(label: str, fn, *args) -> tuple[bool, str]:
    """Run ``fn(*args)`` (construction included); classify the outcome."""
    try:
        result = fn(*args)
    except Exception as exc:  # noqa: BLE001 - report every failure shape
        return False, f"{label}: {type(exc).__name__}: {str(exc)[:160]}"
    if result is False or result is None:
        return False, f"{label}: not found / not accessible"
    return True, label


def _sts(st: Settings, profile, region) -> dict:
    from aws_testkit.aws_clients import StsClient
    return StsClient(st, profile=profile, region=region).caller_identity()


def _lambda_head(st: Settings, profile, region, name) -> dict:
    from aws_testkit.aws_clients import LambdaClient
    return LambdaClient(st, profile=profile, region=region).get_function(name)


def _bucket_head(st: Settings, profile, region, bucket) -> bool:
    from aws_testkit.aws_clients import S3Client
    return S3Client(st, profile=profile, region=region).bucket_exists(bucket)


def _glue_head(st: Settings, profile, region, name):
    from aws_testkit.aws_clients import GlueClient
    return GlueClient(st, profile=profile, region=region).get_job(name)


def _queue_head(st: Settings, profile, region, name) -> str:
    from aws_testkit.aws_clients import SqsClient
    return SqsClient(st, profile=profile, region=region).queue_url(name)


def _preflight_one(usecase_id: str) -> tuple[bool, list[tuple[bool, str]]]:
    cfg = load_usecase(usecase_id)
    region = cfg["region"]
    st = Settings(aws_region=region)
    out: list[tuple[bool, str]] = []

    profiles: dict[str | None, None] = {}
    for section in ("lambda", "glue", "s3", "sqs"):
        blk = cfg.get(section)
        if isinstance(blk, dict):
            profiles.setdefault(blk.get("profile"), None)
    if cfg.get("db"):
        profiles.setdefault((cfg["db"].get("ssm") or {}).get("profile"), None)
    for prof in profiles:
        out.append(_probe(
            f"sts:GetCallerIdentity (profile={prof or 'default'})", _sts, st, prof, region))

    lam = cfg.get("lambda") or {}
    if lam.get("name"):
        out.append(_probe(
            f"lambda:GetFunction {lam['name']}", _lambda_head, st, lam.get("profile"), region, lam["name"]))
    s3 = cfg.get("s3") or {}
    if s3.get("bucket"):
        out.append(_probe(
            f"s3:HeadBucket {s3['bucket']}", _bucket_head, st, s3.get("profile"), region, s3["bucket"]))
    glue = cfg.get("glue") or {}
    if glue.get("name"):
        out.append(_probe(
            f"glue:GetJob {glue['name']}", _glue_head, st, glue.get("profile"), region, glue["name"]))
    sqs = cfg.get("sqs") or {}
    if sqs.get("queue"):
        out.append(_probe(
            f"sqs:GetQueueUrl {sqs['queue']}", _queue_head, st, sqs.get("profile"), region, sqs["queue"]))
    return all(r[0] for r in out), out


def cmd_preflight(ns: argparse.Namespace) -> int:
    targets = _targets(ns.ids)
    failed = 0
    for uid in targets:
        try:
            ok, results = _preflight_one(uid)
        except ConfigError as exc:
            failed += 1
            print(f"{_BAD} {uid}: config invalid — {exc}")
            continue
        failed += 0 if ok else 1
        print(f"{_OK if ok else _BAD} {uid}")
        for good, msg in results:
            print(f"    {_OK if good else _BAD} {msg}")
    print(f"\n{len(targets) - failed} ready, {failed} with problems")
    return 1 if failed else 0


def cmd_run(ns: argparse.Namespace) -> int:
    ids = _targets(ns.ids)
    cmd = [sys.executable, "-m", "pytest", "tests/features/steps/test_generic.py", "-v"]
    if ids != list_usecases():
        cmd += ["-k", " or ".join(ids)]
    if ns.html:
        cmd += [f"--html={ns.html}", "--self-contained-html"]
    cmd += ns.pytest_args
    print("+", " ".join(cmd))
    return subprocess.call(cmd)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="aws-testkit", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="list use cases and capabilities").set_defaults(fn=cmd_list)

    sp = sub.add_parser("show", help="print resolved config for one use case")
    sp.add_argument("id")
    sp.set_defaults(fn=cmd_show)

    sp = sub.add_parser("validate", help="offline config lint")
    sp.add_argument("ids", nargs="*", metavar="id")
    sp.set_defaults(fn=cmd_validate)

    sp = sub.add_parser("preflight", help="check AWS reachability (read-only)")
    sp.add_argument("ids", nargs="*", metavar="id")
    sp.set_defaults(fn=cmd_preflight)

    sp = sub.add_parser("run", help="run the suite for the given use cases")
    sp.add_argument("ids", nargs="*", metavar="id")
    sp.add_argument("--html", help="write a self-contained HTML report here")
    sp.add_argument("pytest_args", nargs="*", metavar="-- ...", help="args after -- go to pytest")
    sp.set_defaults(fn=cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    ns = build_parser().parse_args(argv)
    return ns.fn(ns)


if __name__ == "__main__":
    sys.exit(main())
