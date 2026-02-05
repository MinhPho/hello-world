"""Tests for the environment detection utilities."""

from __future__ import annotations

import pytest

from platform_pipelines.utils.environment import (
    DeploymentEnvironment,
    get_deployment_environment,
)


class TestDeploymentEnvironment:
    def test_local_is_default(self, set_deployment_env) -> None:
        set_deployment_env("local")
        env = get_deployment_environment()
        assert env == DeploymentEnvironment.LOCAL
        assert env.is_local is True
        assert env.is_production is False
        assert env.is_deployed is False

    def test_prod_environment(self, set_deployment_env) -> None:
        set_deployment_env("prod")
        env = get_deployment_environment()
        assert env == DeploymentEnvironment.PROD
        assert env.is_local is False
        assert env.is_production is True
        assert env.is_deployed is True

    @pytest.mark.parametrize(
        "env_value,expected",
        [
            ("dev", DeploymentEnvironment.DEV),
            ("test", DeploymentEnvironment.TEST),
            ("acc", DeploymentEnvironment.ACC),
            ("prod", DeploymentEnvironment.PROD),
        ],
    )
    def test_all_dtap_environments(self, set_deployment_env, env_value, expected) -> None:
        set_deployment_env(env_value)
        env = get_deployment_environment()
        assert env == expected
        assert env.is_deployed is True

    def test_invalid_environment_raises(self, set_deployment_env) -> None:
        set_deployment_env("invalid")
        with pytest.raises(ValueError, match="Invalid DAGSTER_DEPLOYMENT"):
            get_deployment_environment()

    def test_storage_prefix(self) -> None:
        assert DeploymentEnvironment.LOCAL.storage_prefix == "local"
        assert DeploymentEnvironment.PROD.storage_prefix == "prod"
