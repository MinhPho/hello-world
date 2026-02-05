"""Tests for storage resource configuration."""

from __future__ import annotations

from platform_pipelines.resources.storage_resource import StorageConfig, build_storage_config


class TestStorageConfig:
    def test_local_storage_config(self, set_deployment_env) -> None:
        set_deployment_env("local")
        config = build_storage_config()
        assert config.protocol == "s3"
        assert config.endpoint_url is not None
        assert "localhost" in config.endpoint_url
        assert "local" in config.root_path

    def test_prod_storage_config(self, set_deployment_env) -> None:
        set_deployment_env("prod")
        config = build_storage_config()
        assert config.protocol == "s3"
        assert config.endpoint_url is None
        assert "prod" in config.root_path

    def test_root_path_includes_env(self, set_deployment_env) -> None:
        set_deployment_env("acc")
        config = build_storage_config()
        assert "acc" in config.root_path
