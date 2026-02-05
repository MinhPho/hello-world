"""Tests for the Dagster definitions module.

Validates that the Definitions object loads correctly with all
expected assets, jobs, schedules, and sensors.
"""

from __future__ import annotations

import pytest
from dagster import Definitions


def test_definitions_loads() -> None:
    """The Definitions object should load without errors."""
    from platform_pipelines.definitions import defs

    assert isinstance(defs, Definitions)


def test_definitions_has_assets() -> None:
    """All expected asset keys should be present."""
    from platform_pipelines.definitions import defs

    repo = defs.get_repository_def()
    asset_keys = {str(key) for key in repo.asset_graph.all_asset_keys}

    expected_keys = {
        "AssetKey(['etl', 'raw_events'])",
        "AssetKey(['etl', 'raw_users'])",
        "AssetKey(['etl', 'cleaned_events'])",
        "AssetKey(['etl', 'enriched_events'])",
        "AssetKey(['etl', 'daily_aggregates'])",
        "AssetKey(['ml', 'user_features'])",
        "AssetKey(['ml', 'churn_model'])",
        "AssetKey(['ml', 'model_evaluation'])",
    }

    for key in expected_keys:
        assert key in asset_keys, f"Missing asset: {key}"


def test_definitions_has_jobs() -> None:
    """All expected jobs should be registered."""
    from platform_pipelines.definitions import defs

    repo = defs.get_repository_def()
    job_names = {job.name for job in repo.get_all_jobs()}

    expected_jobs = {
        "etl_ingestion_job",
        "etl_transform_job",
        "etl_full_job",
        "ml_feature_job",
        "ml_training_job",
        "ml_full_job",
    }

    for name in expected_jobs:
        assert name in job_names, f"Missing job: {name}"


def test_definitions_has_schedules() -> None:
    """All expected schedules should be registered."""
    from platform_pipelines.definitions import defs

    repo = defs.get_repository_def()
    schedule_names = {s.name for s in repo.schedule_defs}

    assert "daily_etl_schedule" in schedule_names
    assert "monthly_ml_schedule" in schedule_names


def test_definitions_has_sensors() -> None:
    """All expected sensors should be registered."""
    from platform_pipelines.definitions import defs

    repo = defs.get_repository_def()
    sensor_names = {s.name for s in repo.sensor_defs}

    assert "new_data_sensor" in sensor_names
