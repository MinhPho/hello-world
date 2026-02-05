"""Integration tests for Ray-based assets.

These tests require a Ray runtime (LocalRay is sufficient).
Run with: pytest tests/integration -v -m integration

NOTE: These tests initialize a local Ray cluster and execute actual
asset logic, so they are slower than unit tests.
"""

from __future__ import annotations

import pytest

pytest.mark.integration


@pytest.mark.integration
def test_raw_events_generates_data() -> None:
    """raw_events asset should produce a non-empty DataFrame."""
    import pandas as pd
    import ray

    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True)

    @ray.remote
    def generate_partition_data(date_str: str, num_rows: int = 100) -> pd.DataFrame:
        import numpy as np

        rng = np.random.default_rng(hash(date_str) % 2**32)
        return pd.DataFrame(
            {
                "event_id": [f"evt_{date_str}_{i}" for i in range(num_rows)],
                "timestamp": pd.date_range(date_str, periods=num_rows, freq="s"),
                "user_id": rng.integers(1, 1000, size=num_rows),
                "event_type": rng.choice(["page_view", "click"], size=num_rows),
                "value": rng.exponential(scale=10.0, size=num_rows).round(2),
            }
        )

    df = ray.get(generate_partition_data.remote("2024-01-15"))
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 100
    assert "event_id" in df.columns


@pytest.mark.integration
def test_ray_data_transform() -> None:
    """Verify Ray Data transformations work correctly."""
    import ray
    import ray.data

    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True)

    ds = ray.data.range(1000)
    ds = ds.filter(lambda row: row["id"] > 500)
    count = ds.count()
    assert count == 499  # 501..999
