"""ML Feature Engineering assets.

Transforms enriched data into ML-ready feature sets.
Ray Data is used for scalable feature computation.
"""

from __future__ import annotations

import pandas as pd
import ray
from dagster import AssetExecutionContext, AssetKey, asset
from dagster_ray import RayResource

from platform_pipelines.partitions import monthly_partitions


@asset(
    key=AssetKey(["ml", "user_features"]),
    partitions_def=monthly_partitions,
    group_name="feature_engineering",
    description="Aggregated user-level features for ML models. "
    "Computed monthly from enriched event data.",
    kinds={"ray", "parquet"},
)
def user_features(
    context: AssetExecutionContext,
    ray_cluster: RayResource,
) -> pd.DataFrame:
    """Compute user-level features from event data.

    In production, this would read from the enriched_events asset
    for the relevant time window.  The example generates synthetic
    features to demonstrate the pattern.
    """
    partition_month = context.partition_key
    context.log.info(f"Computing user features for {partition_month}")

    @ray.remote
    def compute_features(month: str, num_users: int = 1_000) -> pd.DataFrame:
        import numpy as np

        rng = np.random.default_rng(hash(month) % 2**32)
        return pd.DataFrame(
            {
                "user_id": range(1, num_users + 1),
                "month": month,
                # Behavioral features
                "total_events": rng.poisson(lam=50, size=num_users),
                "total_value": rng.exponential(scale=100.0, size=num_users).round(2),
                "unique_event_types": rng.integers(1, 5, size=num_users),
                "days_active": rng.integers(1, 31, size=num_users),
                # Derived features
                "avg_events_per_day": rng.uniform(0.5, 10.0, size=num_users).round(2),
                "purchase_ratio": rng.uniform(0.0, 0.5, size=num_users).round(4),
                # Target variable for churn prediction
                "churned": rng.choice([0, 1], size=num_users, p=[0.85, 0.15]),
            }
        )

    df = ray.get(compute_features.remote(partition_month))
    context.log.info(f"Computed features for {len(df)} users")
    return df
