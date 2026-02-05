# ===========================================================================
# Dagster + Ray Platform — Developer Makefile
# ===========================================================================
# Run `make help` to see all available commands.

.DEFAULT_GOAL := help
.PHONY: help install dev dev-docker down clean test lint format check ray-job

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
PYTHON ?= python3
PIP ?= pip
DAGSTER_HOME ?= $(CURDIR)

export DAGSTER_HOME

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
install: ## Install all dependencies (with dev extras)
	$(PIP) install -e ".[dev]"

install-kuberay: ## Install with KubeRay support
	$(PIP) install -e ".[dev,kuberay]"

# ---------------------------------------------------------------------------
# Local Development
# ---------------------------------------------------------------------------
dev: ## Start Dagster webserver for local development
	@echo "Starting Dagster dev server..."
	@echo "UI available at http://localhost:3000"
	dagster dev -w workspace.yaml

dev-docker: ## Start full local stack via docker-compose
	docker compose up -d
	@echo ""
	@echo "Services started:"
	@echo "  Dagster UI:    http://localhost:3000"
	@echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo ""
	@echo "Run 'make logs' to follow logs"

down: ## Stop docker-compose services
	docker compose down

down-clean: ## Stop docker-compose and remove volumes
	docker compose down -v

logs: ## Follow docker-compose logs
	docker compose logs -f

logs-dagster: ## Follow Dagster webserver logs only
	docker compose logs -f dagster-webserver dagster-daemon

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test: ## Run all unit tests
	$(PYTHON) -m pytest tests/unit -v

test-integration: ## Run integration tests (requires running services)
	$(PYTHON) -m pytest tests/integration -v

test-all: ## Run all tests with coverage
	$(PYTHON) -m pytest tests/ -v --cov=platform_pipelines --cov-report=term-missing

# ---------------------------------------------------------------------------
# Code Quality
# ---------------------------------------------------------------------------
lint: ## Run ruff linter
	ruff check src/ tests/

format: ## Auto-format code with ruff
	ruff format src/ tests/
	ruff check --fix src/ tests/

typecheck: ## Run mypy type checker
	mypy src/

check: lint typecheck ## Run all code quality checks

# ---------------------------------------------------------------------------
# Ray Jobs (standalone execution)
# ---------------------------------------------------------------------------
ray-batch: ## Run the batch processing Ray job locally
	$(PYTHON) ray_jobs/batch_processing.py

ray-train: ## Run the ML training Ray job locally
	$(PYTHON) ray_jobs/ml_training_job.py

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
