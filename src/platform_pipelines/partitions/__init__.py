"""Partition definitions shared across assets.

Centralizing partition definitions ensures consistency between
assets that participate in the same partitioned workflows.
"""

from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

# Daily partitions — used for incremental ETL pipelines
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

# Monthly partitions — used for ML training/retraining cycles
monthly_partitions = MonthlyPartitionsDefinition(start_date="2024-01-01")
