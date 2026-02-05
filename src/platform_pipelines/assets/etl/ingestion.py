"""ETL Ingestion assets — data extraction and landing zone.

These assets represent the first stage of the ETL pipeline:
reading raw data from external sources and landing it in the
platform's storage layer.

Ray is used here via ray.data for scalable, distributed reads
when data volumes exceed single-node capacity.
"""

from __future__ import annotations

import pandas as pd
import ray
from dagster import AssetExecutionContext, AssetKey, asset
from dagster_ray import RayResource

from platform_pipelines.partitions import daily_partitions
from platform_pipelines.utils.logging import get_logger

logger = get_logger(__name__)


@asset(
    key=AssetKey(["etl", "raw_events"]),
    partitions_def=daily_partitions,
    group_name="ingestion",
    description="Raw event data ingested from source systems. "
    "Uses Ray Data to parallelize reads from large source files.",
    kinds={"ray", "parquet"},
)
def raw_events(
    context: AssetExecutionContext,
    ray_cluster: RayResource,
) -> pd.DataFrame:
    """Ingest raw events for a given partition date.

    In a real implementation, this would read from an API, message queue,
    or external database.  The example generates synthetic data to
    demonstrate the Ray Data integration.
    """
    partition_date = context.partition_key
    context.log.info(f"Ingesting raw events for {partition_date}")

    # Demonstrate Ray Data usage for distributed reading
    @ray.remote
    def generate_partition_data(date_str: str, num_rows: int = 10_000) -> pd.DataFrame:
        """Simulate reading a partition of raw event data."""
        import numpy as np

        rng = np.random.default_rng(hash(date_str) % 2**32)
        return pd.DataFrame(
            {
                "event_id": [f"evt_{date_str}_{i}" for i in range(num_rows)],
                "timestamp": pd.date_range(date_str, periods=num_rows, freq="s"),
                "user_id": rng.integers(1, 100_000, size=num_rows),
                "event_type": rng.choice(
                    ["page_view", "click", "purchase", "signup"], size=num_rows
                ),
                "value": rng.exponential(scale=10.0, size=num_rows).round(2),
            }
        )

    future = generate_partition_data.remote(partition_date)
    df = ray.get(future)

    context.log.info(f"Ingested {len(df)} events for {partition_date}")
    return df


@asset(
    key=AssetKey(["etl", "raw_users"]),
    group_name="ingestion",
    description="User dimension data extracted from the source system.",
    kinds={"ray", "parquet"},
)
def raw_users(
    context: AssetExecutionContext,
    ray_cluster: RayResource,
) -> pd.DataFrame:
    """Extract user dimension data.

    In production, this would connect to a user database or API.
    """
    context.log.info("Extracting user dimension data")

    @ray.remote
    def generate_users(num_users: int = 1_000) -> pd.DataFrame:
        import numpy as np

        rng = np.random.default_rng(42)
        return pd.DataFrame(
            {
                "user_id": range(1, num_users + 1),
                "signup_date": pd.date_range("2023-01-01", periods=num_users, freq="h"),
                "country": rng.choice(["US", "UK", "DE", "FR", "NL", "JP"], size=num_users),
                "tier": rng.choice(["free", "basic", "premium"], size=num_users, p=[0.6, 0.3, 0.1]),
            }
        )

    df = ray.get(generate_users.remote())
    context.log.info(f"Extracted {len(df)} users")
    return df
