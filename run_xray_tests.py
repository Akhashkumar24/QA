#!/usr/bin/env python3
"""CLI runner — Fetch Gherkin features from Xray and execute them with pytest.

Usage
-----
# Fetch and run a single Xray test
python run_xray_tests.py QA-101

# Fetch and run multiple tests
python run_xray_tests.py QA-101 QA-102 QA-103

# Run with HTML report
python run_xray_tests.py QA-101 --html reports/xray-report.html

# Run and push results back to Xray
python run_xray_tests.py QA-101 --import-results

# Use Xray Server/DC instead of Cloud
python run_xray_tests.py QA-101 --mode server --base-url https://jira.company.com

# List test info without running
python run_xray_tests.py QA-101 --info-only

Environment variables (set before running)
------------------------------------------
XRAY_CLIENT_ID       – Xray Cloud client ID
XRAY_CLIENT_SECRET   – Xray Cloud client secret
JIRA_TOKEN           – JIRA PAT (Server/DC) or API token (Cloud)
JIRA_EMAIL           – JIRA email (Cloud basic auth)
XRAY_BASE_URL        – Override default base URL
XRAY_MODE            – "cloud" or "server"
"""

from __future__ import annotations

import argparse
import logging
import sys

from aws_testkit.xray_client import XrayClient, XraySettings, fetch_and_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("xray_runner")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Gherkin features from Xray and run with pytest.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "test_keys",
        nargs="+",
        help="Xray test issue keys (e.g. QA-101 QA-102)",
    )
    parser.add_argument(
        "--mode",
        choices=["cloud", "server"],
        default=None,
        help="Xray mode: cloud (default) or server/DC",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Xray/JIRA base URL (overrides XRAY_BASE_URL env var)",
    )
    parser.add_argument(
        "--features-dir",
        default="tests/features/xray",
        help="Directory to save fetched .feature files (default: tests/features/xray)",
    )
    parser.add_argument(
        "--html",
        default=None,
        help="Generate HTML report at this path (e.g. reports/xray-report.html)",
    )
    parser.add_argument(
        "--import-results",
        action="store_true",
        help="Import Cucumber JSON results back to Xray after test run",
    )
    parser.add_argument(
        "--info-only",
        action="store_true",
        help="Fetch and display test info from Xray without running tests",
    )
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Fetch feature files from Xray but do not run them",
    )

    args = parser.parse_args()

    # Build settings from CLI args + env vars
    overrides = {}
    if args.mode:
        overrides["mode"] = args.mode
    if args.base_url:
        overrides["base_url"] = args.base_url
    if args.features_dir:
        overrides["features_dir"] = args.features_dir

    settings = XraySettings(**overrides) if overrides else XraySettings()

    # --info-only: just show test metadata
    if args.info_only:
        client = XrayClient(settings)
        client.authenticate()
        for key in args.test_keys:
            logger.info("Fetching info for: %s", key)
            info = client.get_test_info(key)
            import json
            print(json.dumps(info, indent=2, default=str))
        return 0

    # --fetch-only: download features without running
    if args.fetch_only:
        client = XrayClient(settings)
        client.authenticate()
        files = client.fetch_features(args.test_keys)
        logger.info("Fetched %d feature file(s):", len(files))
        for f in files:
            logger.info("  %s", f)
        return 0

    # Full run: fetch + execute + optional import
    extra_args = []
    if args.html:
        extra_args.extend(["--html", args.html, "--self-contained-html"])

    exit_code = fetch_and_run(
        test_keys=args.test_keys,
        settings=settings,
        extra_pytest_args=extra_args if extra_args else None,
        import_results=args.import_results,
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
