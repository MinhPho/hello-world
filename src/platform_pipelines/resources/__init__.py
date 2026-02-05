"""Dagster resources for the platform.

Resources encapsulate external dependencies (Ray, storage, databases)
and are configured per-environment using DTAP conventions.
"""

from platform_pipelines.resources.ray_resource import build_ray_resource
from platform_pipelines.resources.storage_resource import (
    StorageConfig,
    build_storage_config,
)

__all__ = [
    "build_ray_resource",
    "build_storage_config",
    "StorageConfig",
]
