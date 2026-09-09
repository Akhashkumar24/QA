"""Offline gate: every use-case config must load, resolve and validate.

No AWS, no network — pure YAML/schema lint. Run in CI on every push.
"""

from __future__ import annotations

import pytest

from aws_testkit.usecase import capabilities, list_usecases, load_usecase

_IDS = list_usecases()


def test_at_least_one_usecase_exists():
    assert _IDS, "no conf/usecases/*.yaml found"


@pytest.mark.parametrize("usecase_id", _IDS)
def test_usecase_config_is_valid(usecase_id):
    cfg = load_usecase(usecase_id)  # raises ConfigError with a readable message
    assert cfg["id"] == usecase_id
    assert capabilities(cfg), f"{usecase_id} declares no runnable capability"
