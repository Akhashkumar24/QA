"""Cross-cutting pytest hooks: @skip Gherkin tags + HTML report naming."""

from __future__ import annotations

import os
import shutil
from datetime import datetime

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if "skip" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="tagged @skip in the feature file"))


def pytest_sessionfinish(session, exitstatus):
    """Copy the pytest-html report to reports/Test_Report_<ts>_<status>.html."""
    html_path = session.config.getoption("htmlpath", default=None)
    if not html_path or not os.path.exists(html_path):
        return
    status = "Passed" if exitstatus == 0 else "Failed"
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dest = os.path.join(os.path.dirname(html_path) or ".", f"Test_Report_{ts}_{status}.html")
    shutil.copy2(html_path, dest)
    print(f"\n>> report: {dest}")
