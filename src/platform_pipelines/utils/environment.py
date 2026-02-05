"""Environment detection and DTAP configuration utilities.

DTAP = Development, Testing, Acceptance, Production.
The deployment environment is determined by the DAGSTER_DEPLOYMENT env var.
"""

from __future__ import annotations

import os
from enum import Enum
from functools import lru_cache


class DeploymentEnvironment(str, Enum):
    """Supported deployment environments following the DTAP model."""

    LOCAL = "local"
    DEV = "dev"
    TEST = "test"
    ACC = "acc"
    PROD = "prod"

    @property
    def is_local(self) -> bool:
        return self == DeploymentEnvironment.LOCAL

    @property
    def is_production(self) -> bool:
        return self == DeploymentEnvironment.PROD

    @property
    def is_deployed(self) -> bool:
        """True for any non-local environment (running in a cluster)."""
        return self != DeploymentEnvironment.LOCAL

    @property
    def storage_prefix(self) -> str:
        """Return the base path prefix for data storage per environment."""
        return f"{self.value}"


@lru_cache(maxsize=1)
def get_deployment_environment() -> DeploymentEnvironment:
    """Resolve the current deployment environment from DAGSTER_DEPLOYMENT.

    Defaults to LOCAL when unset, which is the safe default for developers.
    """
    raw = os.environ.get("DAGSTER_DEPLOYMENT", "local").lower()
    try:
        return DeploymentEnvironment(raw)
    except ValueError:
        valid = ", ".join(e.value for e in DeploymentEnvironment)
        raise ValueError(
            f"Invalid DAGSTER_DEPLOYMENT='{raw}'. Must be one of: {valid}"
        ) from None


def is_running_in_kubernetes() -> bool:
    """Detect if the process is running inside a Kubernetes pod."""
    return os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token")
