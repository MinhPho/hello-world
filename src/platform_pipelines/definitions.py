"""Dagster Definitions — the single entry point for all platform code.

This module assembles all assets, jobs, schedules, sensors, and resources
into a single `Definitions` object that Dagster loads.

Resource configuration is environment-aware: the same code works in
local development (LocalRay, MinIO) and production (KubeRay, S3).

See: https://docs.dagster.io/guides/build/projects/project-structure/project-overview
"""

from __future__ import annotations

from dagster import Definitions, load_assets_from_modules

from platform_pipelines.assets import etl, ml
from platform_pipelines.jobs import (
    etl_full_job,
    etl_ingestion_job,
    etl_transform_job,
    ml_feature_job,
    ml_full_job,
    ml_training_job,
)
from platform_pipelines.resources import build_ray_resource
from platform_pipelines.schedules import daily_etl_schedule, monthly_ml_schedule
from platform_pipelines.sensors import new_data_sensor
from platform_pipelines.utils.logging import configure_logging

# ---------------------------------------------------------------------------
# Bootstrap structured logging
# ---------------------------------------------------------------------------
configure_logging()

# ---------------------------------------------------------------------------
# Load assets from domain modules
# ---------------------------------------------------------------------------
etl_assets = load_assets_from_modules([etl.ingestion, etl.transform])
ml_assets = load_assets_from_modules([ml.feature_engineering, ml.training])

all_assets = [*etl_assets, *ml_assets]

# ---------------------------------------------------------------------------
# Resources — configured per DTAP environment
# ---------------------------------------------------------------------------
resources = {
    # Ray cluster: LocalRay for local dev, KubeRayInteractiveJob in k8s
    "ray_cluster": build_ray_resource(),
}

# ---------------------------------------------------------------------------
# Assemble Definitions
# ---------------------------------------------------------------------------
defs = Definitions(
    assets=all_assets,
    jobs=[
        etl_ingestion_job,
        etl_transform_job,
        etl_full_job,
        ml_feature_job,
        ml_training_job,
        ml_full_job,
    ],
    schedules=[
        daily_etl_schedule,
        monthly_ml_schedule,
    ],
    sensors=[
        new_data_sensor,
    ],
    resources=resources,
)
