"""Storage resource configuration for different DTAP environments.

Provides a consistent interface for object storage that works with:
  - MinIO locally (S3-compatible)
  - AWS S3 in deployed environments

Assets use fsspec-based paths so the same code works across backends.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import fsspec

from platform_pipelines.utils.environment import (
    DeploymentEnvironment,
    get_deployment_environment,
)


@dataclass(frozen=True)
class StorageConfig:
    """Resolved storage configuration for the current environment."""

    bucket: str
    protocol: str  # "s3" or "file"
    base_path: str
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None

    @property
    def root_path(self) -> str:
        """Full root path including protocol, bucket and environment prefix."""
        env = get_deployment_environment()
        if self.protocol == "file":
            return f"{self.base_path}/{env.storage_prefix}"
        return f"s3://{self.bucket}/{env.storage_prefix}"

    def get_filesystem(self) -> fsspec.AbstractFileSystem:
        """Return an fsspec filesystem configured for the current environment."""
        if self.protocol == "file":
            return fsspec.filesystem("file")

        storage_options: dict[str, str] = {}
        if self.endpoint_url:
            storage_options["client_kwargs"] = {"endpoint_url": self.endpoint_url}  # type: ignore[assignment]
        if self.access_key:
            storage_options["key"] = self.access_key
        if self.secret_key:
            storage_options["secret"] = self.secret_key
        return fsspec.filesystem("s3", **storage_options)


def build_storage_config() -> StorageConfig:
    """Build storage configuration based on the deployment environment.

    LOCAL/DEV: Points to MinIO or local filesystem.
    TEST/ACC/PROD: Points to S3 with IAM-based or env-var credentials.
    """
    env = get_deployment_environment()
    bucket = os.environ.get("STORAGE_BUCKET", "platform-data")

    if env == DeploymentEnvironment.LOCAL:
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")
        return StorageConfig(
            bucket=bucket,
            protocol="s3",
            base_path="",
            endpoint_url=endpoint_url,
            access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )

    # Deployed environments: use S3 with IAM or injected credentials
    return StorageConfig(
        bucket=bucket,
        protocol="s3",
        base_path="",
        access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
