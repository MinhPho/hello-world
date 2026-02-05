"""Parquet-based IO Manager for Dagster assets.

Stores asset outputs as Parquet files in the configured object storage
(MinIO locally, S3 in deployed environments).  Supports both pandas
DataFrames and PyArrow Tables.

This IO manager uses the StorageConfig resource to resolve paths, so
the same code works unchanged across all DTAP environments.
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    InputContext,
    OutputContext,
    UPathIOManager,
)
from upath import UPath

from platform_pipelines.resources.storage_resource import StorageConfig

SupportedTypes = Union[pd.DataFrame, pa.Table]


class ParquetIOManager(UPathIOManager):
    """Read and write Parquet files to the configured storage backend.

    Works transparently with local filesystems, MinIO, and S3
    via fsspec / universal_pathlib.
    """

    def __init__(self, storage_config: StorageConfig) -> None:
        self._storage_config = storage_config
        base = UPath(storage_config.root_path)
        super().__init__(base_path=base)

    def dump_to_path(self, context: OutputContext, obj: SupportedTypes, path: UPath) -> None:
        target = path.with_suffix(".parquet")
        context.log.info(f"Writing parquet to {target}")

        if isinstance(obj, pd.DataFrame):
            table = pa.Table.from_pandas(obj)
        elif isinstance(obj, pa.Table):
            table = obj
        else:
            raise TypeError(f"Unsupported type {type(obj)}. Expected DataFrame or Table.")

        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as f:
            pq.write_table(table, f)

        context.add_output_metadata(
            {
                "num_rows": table.num_rows,
                "num_columns": table.num_columns,
                "path": str(target),
            }
        )

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        target = path.with_suffix(".parquet")
        context.log.info(f"Reading parquet from {target}")

        with target.open("rb") as f:
            table = pq.read_table(f)
        return table.to_pandas()
