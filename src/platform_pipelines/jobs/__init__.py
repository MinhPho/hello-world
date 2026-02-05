"""Job definitions for the platform.

Jobs define selections of assets to materialize together.
They can be triggered manually, by schedules, or by sensors.
"""

from dagster import AssetSelection, define_asset_job

# ---------------------------------------------------------------------------
# ETL Jobs
# ---------------------------------------------------------------------------

etl_ingestion_job = define_asset_job(
    name="etl_ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    description="Ingest raw data from source systems.",
    tags={"domain": "etl", "stage": "ingestion"},
)

etl_transform_job = define_asset_job(
    name="etl_transform_job",
    selection=AssetSelection.groups("transformation"),
    description="Transform and enrich raw data into analytics-ready datasets.",
    tags={"domain": "etl", "stage": "transformation"},
)

etl_full_job = define_asset_job(
    name="etl_full_job",
    selection=AssetSelection.groups("ingestion", "transformation"),
    description="Full ETL pipeline: ingest + transform.",
    tags={"domain": "etl", "stage": "full"},
)

# ---------------------------------------------------------------------------
# ML Jobs
# ---------------------------------------------------------------------------

ml_feature_job = define_asset_job(
    name="ml_feature_job",
    selection=AssetSelection.groups("feature_engineering"),
    description="Compute ML features from enriched data.",
    tags={"domain": "ml", "stage": "features"},
)

ml_training_job = define_asset_job(
    name="ml_training_job",
    selection=AssetSelection.groups("training"),
    description="Train and evaluate ML models.",
    tags={"domain": "ml", "stage": "training"},
)

ml_full_job = define_asset_job(
    name="ml_full_job",
    selection=AssetSelection.groups("feature_engineering", "training"),
    description="Full ML pipeline: features + training + evaluation.",
    tags={"domain": "ml", "stage": "full"},
)
