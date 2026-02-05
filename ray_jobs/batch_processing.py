"""External Ray job script for heavy batch processing.

This script is designed to be submitted as a standalone Ray job via
Dagster Pipes (PipesKubeRayJobClient) for workloads that need their
own dedicated Ray cluster (e.g., large-scale data processing, GPU training).

It can also be run directly for testing:
    python ray_jobs/batch_processing.py

When executed via Dagster Pipes, it receives context and can report
metadata back to the Dagster UI.

See: https://danielgafni.github.io/dagster-ray/latest/
"""

from __future__ import annotations

import ray
import ray.data


def run_batch_processing() -> dict:
    """Execute a distributed batch processing job on Ray.

    Returns a summary dict with processing metrics.
    """
    # Initialize Ray (connects to existing cluster if RAY_ADDRESS is set)
    if not ray.is_initialized():
        ray.init()

    print(f"Ray cluster resources: {ray.cluster_resources()}")

    # Example: distributed data processing with Ray Data
    # In production, replace with actual data source (S3, Delta Lake, etc.)
    ds = ray.data.range(100_000)

    # Apply distributed transformations
    ds = ds.map(lambda row: {"id": row["id"], "value": row["id"] * 2.5})
    ds = ds.filter(lambda row: row["value"] > 100)

    count = ds.count()
    print(f"Processed records: {count}")

    result = {
        "records_processed": count,
        "cluster_nodes": len(ray.nodes()),
        "status": "completed",
    }

    return result


if __name__ == "__main__":
    # Support Dagster Pipes context when available
    try:
        from dagster_pipes import open_dagster_pipes

        with open_dagster_pipes() as context:
            result = run_batch_processing()
            context.report_asset_materialization(metadata=result)
    except ImportError:
        # Running standalone (local testing)
        result = run_batch_processing()
        print(f"Result: {result}")
