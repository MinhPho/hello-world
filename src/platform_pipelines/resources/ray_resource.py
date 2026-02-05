"""Ray cluster resource configuration for Dagster.

Uses dagster-ray to provide a RayResource that automatically selects:
  - LocalRay   for local development (no cluster required)
  - KubeRayInteractiveJob for Kubernetes deployments

This follows the dagster-ray recommended pattern:
  https://github.com/danielgafni/dagster-ray

The resource exposes a connected Ray runtime to assets and ops so that
they can submit Ray tasks/actors transparently.
"""

from __future__ import annotations

from dagster import EnvVar
from dagster_ray import LocalRay, RayResource

from platform_pipelines.utils.environment import (
    get_deployment_environment,
    is_running_in_kubernetes,
)


def build_ray_resource() -> RayResource:
    """Build the appropriate Ray resource based on the deployment environment.

    - LOCAL: Uses LocalRay which initializes a single-node Ray runtime
      in the same process.  Great for development and testing.
    - Kubernetes: Uses KubeRayInteractiveJob which creates an ephemeral
      RayJob CR and connects in client mode.

    Returns:
        A configured RayResource instance.
    """
    env = get_deployment_environment()

    if is_running_in_kubernetes():
        # Import KubeRay only when in k8s to avoid hard dependency locally
        from dagster_ray.kuberay import KubeRayInteractiveJob

        return KubeRayInteractiveJob(
            ray_cluster={
                "apiVersion": "ray.io/v1",
                "kind": "RayJob",
                "metadata": {
                    "generateName": f"dagster-ray-{env.value}-",
                },
                "spec": {
                    "entrypoint": "python -c 'import ray; ray.init()'",
                    "rayClusterSpec": {
                        "headGroupSpec": {
                            "rayStartParams": {"dashboard-host": "0.0.0.0"},
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "ray-head",
                                            "image": EnvVar("RAY_IMAGE").get_value(
                                                default="rayproject/ray:2.38.0-py310"
                                            ),
                                            "resources": {
                                                "requests": {"cpu": "1", "memory": "2Gi"},
                                                "limits": {"cpu": "2", "memory": "4Gi"},
                                            },
                                        }
                                    ]
                                }
                            },
                        },
                        "workerGroupSpecs": [
                            {
                                "groupName": "workers",
                                "replicas": 2,
                                "minReplicas": 1,
                                "maxReplicas": 10,
                                "rayStartParams": {},
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {
                                                "name": "ray-worker",
                                                "image": EnvVar("RAY_IMAGE").get_value(
                                                    default="rayproject/ray:2.38.0-py310"
                                                ),
                                                "resources": {
                                                    "requests": {
                                                        "cpu": "2",
                                                        "memory": "4Gi",
                                                    },
                                                    "limits": {
                                                        "cpu": "4",
                                                        "memory": "8Gi",
                                                    },
                                                },
                                            }
                                        ]
                                    }
                                },
                            }
                        ],
                    },
                },
            },
        )

    # Local / non-k8s: use LocalRay (single-node, in-process)
    return LocalRay()
