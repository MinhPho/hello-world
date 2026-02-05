"""Sensor definitions for the platform.

Sensors enable event-driven pipeline execution — e.g., triggering
a pipeline when new files land in object storage.
"""

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from platform_pipelines.jobs import etl_ingestion_job


@sensor(
    name="new_data_sensor",
    job=etl_ingestion_job,
    minimum_interval_seconds=300,
    description="Monitors object storage for new raw data files and triggers ingestion.",
)
def new_data_sensor(context: SensorEvaluationContext) -> SensorResult:
    """Check for new data files in the landing zone.

    This is a skeleton sensor. In production, implement the check against
    your actual data source (S3 prefix listing, database query, etc.).

    The cursor tracks the last processed marker to ensure idempotency.
    """
    last_cursor = context.cursor or ""

    # Placeholder: in production, list new files from storage
    # new_files = list_new_files_since(last_cursor)
    new_files: list[str] = []

    if not new_files:
        return SensorResult(run_requests=[], cursor=last_cursor)

    run_requests = []
    latest_cursor = last_cursor

    for file_path in new_files:
        run_requests.append(
            RunRequest(
                run_key=f"ingest-{file_path}",
                tags={"source_file": file_path},
            )
        )
        latest_cursor = file_path

    context.log.info(f"Detected {len(run_requests)} new files to ingest")
    return SensorResult(run_requests=run_requests, cursor=latest_cursor)
