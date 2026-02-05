"""Shared test fixtures.

Provides environment setup for unit and integration tests.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _set_test_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure all tests run with DAGSTER_DEPLOYMENT=local."""
    monkeypatch.setenv("DAGSTER_DEPLOYMENT", "local")


@pytest.fixture
def set_deployment_env(monkeypatch: pytest.MonkeyPatch):
    """Factory fixture to set a specific deployment environment."""

    def _set(env: str) -> None:
        monkeypatch.setenv("DAGSTER_DEPLOYMENT", env)
        # Clear the lru_cache on get_deployment_environment
        from platform_pipelines.utils.environment import get_deployment_environment

        get_deployment_environment.cache_clear()

    return _set
