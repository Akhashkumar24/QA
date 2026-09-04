"""Xray (JIRA) integration utility for fetching and running Gherkin features.

Supports both Xray Cloud and Xray Server/Data Center APIs.
Credentials are resolved from environment variables.

Environment variables
---------------------
XRAY_BASE_URL       – Xray Cloud: https://xray.cloud.getxray.app
                      Xray Server: https://your-jira.atlassian.net
XRAY_CLIENT_ID      – Xray Cloud API client ID
XRAY_CLIENT_SECRET  – Xray Cloud API client secret
JIRA_TOKEN          – JIRA personal access token (Server/DC) or API token (Cloud basic auth)
JIRA_EMAIL          – JIRA email for basic auth (Cloud only, used with JIRA_TOKEN)
XRAY_MODE           – "cloud" or "server" (default: cloud)
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import subprocess
import sys
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class XraySettings:
    """Xray / JIRA connection settings resolved from environment variables."""

    base_url: str = field(
        default_factory=lambda: os.getenv(
            "XRAY_BASE_URL", "https://xray.cloud.getxray.app"
        )
    )
    client_id: str | None = field(
        default_factory=lambda: os.getenv("XRAY_CLIENT_ID")
    )
    client_secret: str | None = field(
        default_factory=lambda: os.getenv("XRAY_CLIENT_SECRET")
    )
    jira_token: str | None = field(
        default_factory=lambda: os.getenv("JIRA_TOKEN")
    )
    jira_email: str | None = field(
        default_factory=lambda: os.getenv("JIRA_EMAIL")
    )
    mode: str = field(
        default_factory=lambda: os.getenv("XRAY_MODE", "cloud").lower()
    )
    features_dir: str = field(
        default_factory=lambda: os.getenv(
            "XRAY_FEATURES_DIR", "tests/features/xray"
        )
    )

    @property
    def is_cloud(self) -> bool:
        return self.mode == "cloud"


# ---------------------------------------------------------------------------
# Xray Client
# ---------------------------------------------------------------------------

class XrayClient:
    """Fetches Gherkin feature files from Xray (Cloud or Server/DC)."""

    def __init__(self, settings: XraySettings | None = None) -> None:
        self.settings = settings or XraySettings()
        self._token: str | None = None

    # -- Authentication ----------------------------------------------------

    def authenticate(self) -> str:
        """Authenticate with Xray Cloud and return a bearer token.

        For Server/DC mode, authentication uses basic auth or PAT per-request,
        so this method simply validates that credentials are present.
        """
        if self.settings.is_cloud:
            return self._authenticate_cloud()
        return self._validate_server_credentials()

    def _authenticate_cloud(self) -> str:
        if not self.settings.client_id or not self.settings.client_secret:
            raise ValueError(
                "XRAY_CLIENT_ID and XRAY_CLIENT_SECRET are required for Xray Cloud."
            )
        url = f"{self.settings.base_url}/api/v2/authenticate"
        payload = {
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
        }
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()

        self._token = resp.json() if isinstance(resp.json(), str) else resp.text.strip('"')
        logger.info("Authenticated with Xray Cloud successfully")
        return self._token

    def _validate_server_credentials(self) -> str:
        if not self.settings.jira_token:
            raise ValueError(
                "JIRA_TOKEN is required for Xray Server/DC mode."
            )
        self._token = self.settings.jira_token
        logger.info("Using JIRA PAT/token for Xray Server/DC")
        return self._token

    # -- Headers -----------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        if self.settings.is_cloud:
            return {"Authorization": f"Bearer {self._token}"}
        return {"Authorization": f"Bearer {self.settings.jira_token}"}

    def _auth_tuple(self) -> tuple[str, str] | None:
        """Return basic-auth tuple for JIRA Cloud (email + API token)."""
        if self.settings.jira_email and self.settings.jira_token:
            return (self.settings.jira_email, self.settings.jira_token)
        return None

    # -- Fetch Features ----------------------------------------------------

    def fetch_features(self, test_keys: list[str]) -> list[Path]:
        """Fetch Gherkin .feature files for the given Xray test issue keys.

        Parameters
        ----------
        test_keys : list[str]
            JIRA issue keys, e.g. ["QA-101", "QA-102"] or ["QA-101;QA-102"].

        Returns
        -------
        list[Path]
            Paths to the downloaded .feature files.
        """
        if self.settings.is_cloud:
            return self._fetch_features_cloud(test_keys)
        return self._fetch_features_server(test_keys)

    def _fetch_features_cloud(self, test_keys: list[str]) -> list[Path]:
        """Xray Cloud: GET /api/v2/export/cucumber?keys=KEY1;KEY2"""
        if not self._token:
            self.authenticate()

        keys_param = ";".join(test_keys)
        url = f"{self.settings.base_url}/api/v2/export/cucumber"
        params = {"keys": keys_param}

        logger.info("Fetching features from Xray Cloud: %s", keys_param)
        resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
        resp.raise_for_status()

        return self._extract_features(resp.content, test_keys)

    def _fetch_features_server(self, test_keys: list[str]) -> list[Path]:
        """Xray Server/DC: GET /rest/raven/1.0/export/test?keys=KEY1;KEY2"""
        keys_param = ";".join(test_keys)
        url = f"{self.settings.base_url}/rest/raven/1.0/export/test"
        params = {"keys": keys_param}

        logger.info("Fetching features from Xray Server: %s", keys_param)
        resp = requests.get(
            url, headers=self._headers(), params=params, timeout=60
        )
        resp.raise_for_status()

        return self._extract_features(resp.content, test_keys)

    def _extract_features(
        self, content: bytes, test_keys: list[str]
    ) -> list[Path]:
        """Extract .feature files from the Xray API response.

        The response can be a ZIP archive (multiple tests) or a single
        .feature file body.
        """
        features_dir = Path(self.settings.features_dir)
        features_dir.mkdir(parents=True, exist_ok=True)

        saved_files: list[Path] = []

        if self._is_zip(content):
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if name.endswith(".feature"):
                        dest = features_dir / Path(name).name
                        dest.write_bytes(zf.read(name))
                        saved_files.append(dest)
                        logger.info("Extracted: %s", dest)
        else:
            text = content.decode("utf-8")
            if text.strip().startswith("Feature:") or text.strip().startswith("@"):
                filename = self._sanitize_filename(test_keys) + ".feature"
                dest = features_dir / filename
                dest.write_text(text, encoding="utf-8")
                saved_files.append(dest)
                logger.info("Saved: %s", dest)
            else:
                raise ValueError(
                    f"Unexpected response format. First 200 chars: {text[:200]}"
                )

        logger.info("Total feature files fetched: %d", len(saved_files))
        return saved_files

    # -- Fetch Test Details ------------------------------------------------

    def get_test_info(self, test_key: str) -> dict[str, Any]:
        """Fetch test issue details from Xray/JIRA.

        Returns basic info: key, summary, labels, Gherkin definition.
        """
        if self.settings.is_cloud:
            return self._get_test_info_cloud(test_key)
        return self._get_test_info_server(test_key)

    def _get_test_info_cloud(self, test_key: str) -> dict[str, Any]:
        if not self._token:
            self.authenticate()

        url = f"{self.settings.base_url}/api/v2/graphql"
        query = """
        query GetTest($issueId: String!) {
            getTests(jql: "key = $issueId", limit: 1) {
                results {
                    issueId
                    testType { name }
                    gherkin
                }
            }
        }
        """.replace("$issueId", test_key)

        resp = requests.post(
            url, headers=self._headers(),
            json={"query": query}, timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    def _get_test_info_server(self, test_key: str) -> dict[str, Any]:
        url = f"{self.settings.base_url}/rest/raven/1.0/api/test/{test_key}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    # -- Import Results ----------------------------------------------------

    def import_results(self, results_file: Path) -> dict[str, Any]:
        """Import test execution results back to Xray.

        Parameters
        ----------
        results_file : Path
            Path to a Cucumber JSON results file.
        """
        if self.settings.is_cloud:
            return self._import_results_cloud(results_file)
        return self._import_results_server(results_file)

    def _import_results_cloud(self, results_file: Path) -> dict[str, Any]:
        if not self._token:
            self.authenticate()

        url = f"{self.settings.base_url}/api/v2/import/execution/cucumber"
        headers = {**self._headers(), "Content-Type": "application/json"}
        data = results_file.read_text(encoding="utf-8")

        logger.info("Importing results to Xray Cloud from: %s", results_file)
        resp = requests.post(url, headers=headers, data=data, timeout=60)
        resp.raise_for_status()

        result = resp.json()
        logger.info("Import response: %s", json.dumps(result, indent=2))
        return result

    def _import_results_server(self, results_file: Path) -> dict[str, Any]:
        url = f"{self.settings.base_url}/rest/raven/1.0/import/execution/cucumber"
        headers = {**self._headers(), "Content-Type": "application/json"}
        data = results_file.read_text(encoding="utf-8")

        logger.info("Importing results to Xray Server from: %s", results_file)
        resp = requests.post(url, headers=headers, data=data, timeout=60)
        resp.raise_for_status()

        result = resp.json()
        logger.info("Import response: %s", json.dumps(result, indent=2))
        return result

    # -- Helpers -----------------------------------------------------------

    @staticmethod
    def _is_zip(content: bytes) -> bool:
        return content[:4] == b"PK\x03\x04"

    @staticmethod
    def _sanitize_filename(test_keys: list[str]) -> str:
        raw = "_".join(test_keys)
        return re.sub(r"[^a-zA-Z0-9_\-]", "_", raw)


# ---------------------------------------------------------------------------
# Runner — Fetch from Xray, run pytest, optionally push results back
# ---------------------------------------------------------------------------

def fetch_and_run(
    test_keys: list[str],
    *,
    settings: XraySettings | None = None,
    extra_pytest_args: list[str] | None = None,
    import_results: bool = False,
) -> int:
    """End-to-end: fetch features from Xray, run them, optionally import results.

    Parameters
    ----------
    test_keys : list[str]
        Xray test issue keys (e.g. ["QA-101", "QA-102"]).
    settings : XraySettings, optional
        Override default env-based settings.
    extra_pytest_args : list[str], optional
        Additional args passed to pytest (e.g. ["--html=report.html"]).
    import_results : bool
        If True, push Cucumber JSON results back to Xray after the run.

    Returns
    -------
    int
        pytest exit code (0 = all passed).
    """
    settings = settings or XraySettings()
    client = XrayClient(settings)

    # Step 1: Authenticate
    logger.info("=" * 60)
    logger.info("XRAY INTEGRATION — Fetch & Run")
    logger.info("=" * 60)
    client.authenticate()

    # Step 2: Fetch feature files
    feature_files = client.fetch_features(test_keys)
    if not feature_files:
        logger.error("No feature files fetched. Aborting.")
        return 1

    logger.info("Feature files ready:")
    for f in feature_files:
        logger.info("  %s", f)

    # Step 3: Generate step stubs if needed
    features_dir = Path(settings.features_dir)
    steps_dir = features_dir / "steps"
    steps_dir.mkdir(parents=True, exist_ok=True)
    conftest = steps_dir / "conftest.py"
    if not conftest.exists():
        conftest.write_text(
            '"""Auto-generated conftest for Xray-fetched features."""\n',
            encoding="utf-8",
        )

    # Step 4: Run pytest
    cucumber_json = features_dir / "cucumber_results.json"
    pytest_cmd = [
        sys.executable, "-m", "pytest",
        str(features_dir),
        "-v", "--tb=short",
    ]
    if import_results:
        pytest_cmd.extend([
            f"--cucumber-json={cucumber_json}",
        ])
    if extra_pytest_args:
        pytest_cmd.extend(extra_pytest_args)

    logger.info("Running: %s", " ".join(pytest_cmd))
    result = subprocess.run(pytest_cmd, cwd=os.getcwd())

    # Step 5: Import results back to Xray (optional)
    if import_results and cucumber_json.exists():
        try:
            client.import_results(cucumber_json)
            logger.info("Results imported to Xray successfully")
        except Exception as exc:
            logger.error("Failed to import results to Xray: %s", exc)

    logger.info("=" * 60)
    logger.info("Exit code: %d", result.returncode)
    logger.info("=" * 60)

    return result.returncode
