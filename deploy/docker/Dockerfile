# Multi-stage Dockerfile for the Dagster + Ray platform.
#
# Stage 1: Build dependencies (cached layer)
# Stage 2: Runtime image with only what's needed
#
# Build: docker build -f deploy/docker/Dockerfile -t dagster-ray-platform .
# Run:   docker run --env-file .env dagster-ray-platform

# ---------------------------------------------------------------------------
# Stage 1: Build
# ---------------------------------------------------------------------------
FROM python:3.10-slim AS builder

WORKDIR /build

# Install build deps
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy project definition first (cache-friendly)
COPY pyproject.toml .
RUN pip install --no-cache-dir --prefix=/install .

# Copy source
COPY src/ src/
RUN pip install --no-cache-dir --prefix=/install .

# ---------------------------------------------------------------------------
# Stage 2: Runtime
# ---------------------------------------------------------------------------
FROM python:3.10-slim AS runtime

# Non-root user for security
RUN groupadd -r dagster && useradd -r -g dagster -d /app dagster

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy source and config
COPY src/ src/
COPY workspace.yaml .
COPY dagster.yaml .
COPY ray_jobs/ ray_jobs/

# Own the app directory
RUN chown -R dagster:dagster /app

USER dagster

# Dagster webserver default port
EXPOSE 3000

# Default: run dagster-webserver (override in docker-compose for daemon, etc.)
ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
