"""ETL Transformation assets — data cleaning and enrichment.

These assets consume raw data and produce cleaned, enriched datasets
ready for analytics or ML consumption.

Ray Data is used for distributed transformations when data doesn't
fit in single-node memory.
"""

from __future__ import annotations

import pandas as pd
import ray
import ray.data
from dagster import AssetExecutionContext, AssetKey, asset
from dagster_ray import RayResource

from platform_pipelines.partitions import daily_partitions


@asset(
    key=AssetKey(["etl", "cleaned_events"]),
    partitions_def=daily_partitions,
    group_name="transformation",
    deps=[AssetKey(["etl", "raw_events"])],
    description="Cleaned and validated event data with invalid records removed.",
    kinds={"ray", "parquet"},
)
def cleaned_events(
    context: AssetExecutionContext,
    ray_cluster: RayResource,
    raw_events: pd.DataFrame,
) -> pd.DataFrame:
    """Clean and validate raw event data using Ray Data.

    Steps:
    1. Remove records with null event_id
    2. Filter out negative values
    3. Standardize event_type to lowercase
    """
    context.log.info(f"Cleaning {len(raw_events)} raw events")

    # Use Ray Data for distributed transformation
    ds = ray.data.from_pandas(raw_events)

    ds = ds.filter(lambda row: row["event_id"] is not None)
    ds = ds.filter(lambda row: row["value"] >= 0)
    ds = ds.map(lambda row: {**row, "event_type": row["event_type"].lower()})

    df = ds.to_pandas()
    context.log.info(f"Cleaned events: {len(df)} rows (dropped {len(raw_events) - len(df)})")
    return df


@asset(
    key=AssetKey(["etl", "enriched_events"]),
    partitions_def=daily_partitions,
    group_name="transformation",
    description="Events enriched with user dimension data for analytics.",
    kinds={"ray", "parquet"},
)
def enriched_events(
    context: AssetExecutionContext,
    ray_cluster: RayResource,
    cleaned_events: pd.DataFrame,
    raw_users: pd.DataFrame,
) -> pd.DataFrame:
    """Join cleaned events with user dimensions.

    Uses Ray Data for the distributed join when datasets are large.
    Falls back to pandas merge for smaller datasets for simplicity.
    """
    context.log.info(
        f"Enriching {len(cleaned_events)} events with {len(raw_users)} user records"
    )

    # For datasets that fit in memory, pandas merge is simpler and faster
    df = cleaned_events.merge(raw_users, on="user_id", how="left")

    # Add derived columns
    df["event_date"] = pd.to_datetime(df["timestamp"]).dt.date

    null_users = df["country"].isna().sum()
    context.log.info(
        f"Enriched events: {len(df)} rows, {null_users} events without user match"
    )
    return df


@asset(
    key=AssetKey(["etl", "daily_aggregates"]),
    partitions_def=daily_partitions,
    group_name="transformation",
    deps=[AssetKey(["etl", "enriched_events"])],
    description="Daily aggregated metrics by country and event type.",
    kinds={"ray", "parquet"},
)
def daily_aggregates(
    context: AssetExecutionContext,
    enriched_events: pd.DataFrame,
) -> pd.DataFrame:
    """Compute daily aggregate metrics from enriched events."""
    context.log.info(f"Aggregating {len(enriched_events)} enriched events")

    df = (
        enriched_events.groupby(["event_date", "country", "event_type"])
        .agg(
            event_count=("event_id", "count"),
            total_value=("value", "sum"),
            avg_value=("value", "mean"),
            unique_users=("user_id", "nunique"),
        )
        .reset_index()
    )

    context.log.info(f"Produced {len(df)} aggregate rows")
    return df
