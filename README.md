# Dagster + Ray Platform

Data and ML platform using **Dagster OSS** as the orchestrator and **Ray** as the distributed processing engine, following DTAP (Development, Testing, Acceptance, Production) best practices.

## Architecture Overview

```
                    ┌─────────────────────────────┐
                    │       Dagster Webserver      │  ← UI + API
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │       Dagster Daemon         │  ← Schedules, Sensors, Run Queue
                    └──────────────┬──────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
     ┌────────┴────────┐  ┌───────┴───────┐  ┌────────┴────────┐
     │   ETL Assets    │  │   ML Assets   │  │  Ray Jobs       │
     │  (ingestion,    │  │  (features,   │  │  (Pipes-based   │
     │   transform)    │  │   training)   │  │   external)     │
     └────────┬────────┘  └───────┬───────┘  └────────┬────────┘
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │     Ray Cluster              │
                    │  LocalRay (dev) / KubeRay    │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │    Object Storage            │
                    │  MinIO (dev) / S3 (prod)     │
                    └─────────────────────────────┘
```

## Repository Structure

```
.
├── src/platform_pipelines/          # Main Dagster code location
│   ├── definitions.py               # Single Definitions entry point
│   ├── assets/
│   │   ├── etl/                     # ETL domain assets
│   │   │   ├── ingestion.py         #   Raw data extraction
│   │   │   └── transform.py         #   Cleaning, enrichment, aggregation
│   │   └── ml/                      # ML domain assets
│   │       ├── feature_engineering.py#   Feature computation
│   │       └── training.py          #   Model training + evaluation
│   ├── resources/                   # Environment-aware resources
│   │   ├── ray_resource.py          #   LocalRay / KubeRay selection
│   │   └── storage_resource.py      #   MinIO / S3 configuration
│   ├── io/                          # IO managers
│   │   └── parquet_io_manager.py    #   Parquet read/write via fsspec
│   ├── jobs/                        # Job definitions (asset selections)
│   ├── schedules/                   # Time-based triggers
│   ├── sensors/                     # Event-driven triggers
│   ├── partitions/                  # Shared partition definitions
│   └── utils/                       # Logging, environment detection
├── ray_jobs/                        # Standalone Ray scripts (Dagster Pipes)
│   ├── batch_processing.py
│   └── ml_training_job.py
├── tests/
│   ├── unit/                        # Fast, no external dependencies
│   └── integration/                 # Requires Ray runtime
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile               # Dagster webserver/daemon image
│   │   └── Dockerfile.ray           # Custom Ray worker image
│   ├── helm/                        # Kubernetes Helm values
│   │   ├── values-base.yaml
│   │   ├── values-dev.yaml
│   │   └── values-prod.yaml
│   └── envs/                        # DTAP environment configs
│       ├── local/.env
│       ├── dev/.env.template
│       ├── test/.env.template
│       ├── acc/.env.template
│       └── prod/.env.template
├── data/                            # Local data directory (gitignored)
├── docker-compose.yaml              # Full local dev stack
├── workspace.yaml                   # Dagster code location config
├── dagster.yaml                     # Dagster instance config
├── pyproject.toml                   # Project & tool configuration
└── Makefile                         # Developer commands
```

## DTAP Environments

| Environment | Ray Backend | Storage | Database | Purpose |
|-------------|-------------|---------|----------|---------|
| **local**   | LocalRay (in-process) | MinIO | Postgres (docker) | Developer laptop |
| **dev**     | KubeRay cluster | S3 | Managed Postgres | Shared development |
| **test**    | KubeRay cluster | S3 | Managed Postgres | Integration testing |
| **acc**     | KubeRay cluster | S3 | Managed Postgres | Pre-production validation |
| **prod**    | KubeRay cluster | S3 | Managed Postgres | Production |

The environment is selected by the `DAGSTER_DEPLOYMENT` environment variable. Code never changes between environments — only configuration does.

## Quick Start (Local Development)

### Option 1: Native (recommended for development)

```bash
# Install dependencies
make install

# Copy environment variables
cp .env.example .env

# Start Dagster dev server (uses LocalRay, no Docker needed)
make dev
# → UI at http://localhost:3000
```

### Option 2: Docker Compose (full stack)

```bash
# Start Postgres + MinIO + Dagster
make dev-docker
# → Dagster UI at http://localhost:3000
# → MinIO Console at http://localhost:9001
```

### Running Tests

```bash
make test              # Unit tests only
make test-integration  # Integration tests (needs Ray)
make test-all          # All tests with coverage
```

### Code Quality

```bash
make lint        # Ruff linter
make format      # Auto-format
make typecheck   # mypy
make check       # All checks
```

## Key Design Decisions

### Why Dagster as orchestrator?
- **Asset-centric model**: Pipeline logic is defined around the data it produces, giving automatic lineage, caching, and observability
- **Environment-aware resources**: Same code, different configs per environment via `EnvVar` and resource injection
- **Rich UI**: Built-in asset graph, run history, and metadata visualization

### Why Ray as processing engine?
- **Unified compute**: Same engine for data processing (Ray Data) and ML training (Ray Train)
- **Local-to-cluster transparency**: Code runs identically on a laptop (LocalRay) and on a 100-node cluster (KubeRay)
- **dagster-ray integration**: First-class Dagster resource that manages Ray cluster lifecycle

### Why this repo structure?
- **Organized by domain** (ETL, ML) rather than by technology, following Dagster's recommended pattern for teams with mixed data engineering and ML workloads
- **Single code location** to start simple, with the structure ready to split into multiple locations when team boundaries demand it
- **`ray_jobs/` separation**: Heavy batch jobs that need their own Ray cluster are kept as standalone scripts, submitted via Dagster Pipes

## Deploying to Kubernetes

The project includes Helm value files compatible with the official [Dagster Helm chart](https://docs.dagster.io/deployment/guides/kubernetes/deploying-with-helm):

```bash
# Add the Dagster Helm repo
helm repo add dagster https://dagster-io.github.io/helm

# Deploy to dev
helm install dagster dagster/dagster \
  -f deploy/helm/values-base.yaml \
  -f deploy/helm/values-dev.yaml

# Deploy to production
helm install dagster dagster/dagster \
  -f deploy/helm/values-base.yaml \
  -f deploy/helm/values-prod.yaml
```

KubeRay must be installed on the cluster for Ray integration:
```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator
```

## References

- [Dagster Docs — Project Structure](https://docs.dagster.io/guides/build/projects/project-structure/project-overview)
- [Dagster Docs — Environment Variables](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets)
- [Dagster Blog — How to Structure Your Project](https://dagster.io/blog/how-to-structure-your-dagster-project)
- [dagster-ray — GitHub](https://github.com/danielgafni/dagster-ray)
- [dagster-ray — Documentation](https://danielgafni.github.io/dagster-ray/latest/)
- [Ray Docs — Ray Data](https://docs.ray.io/en/latest/data/data.html)
- [Ray Docs — Ray Train](https://docs.ray.io/en/latest/train/train.html)
- [KubeRay — GitHub](https://github.com/ray-project/kuberay)
