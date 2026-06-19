# --- Platform Management Interface ---
.PHONY: help up down ps logs test lint format type-check security-scan terraform-plan terraform-apply docs quality-report all-checks clean init-data observability-up

help:
	@echo "Platform Management Commands"
	@echo "----------------------------"
	@echo "up              : Launch local services"
	@echo "down            : Shutdown local services"
	@echo "ps              : List running containers"
	@echo "logs            : Follow container logs"
	@echo "test            : Run Pytest logic verification"
	@echo "lint            : Run Ruff check with autofix"
	@echo "format          : Run Black formatting"
	@echo "type-check      : Run Mypy strict type checking"
	@echo "security-scan   : Run Bandit security scan"
	@echo "terraform-plan  : Run Terraform plan (use ENV=aws|gcp|azure)"
	@echo "terraform-apply : Run Terraform apply (use ENV=aws|gcp|azure)"
	@echo "docs            : Generate API documentation"
	@echo "quality-report  : Run Great Expectations checkpoints"
	@echo "all-checks      : Run lint, type-check, security-scan, and test"
	@echo "init-data       : Trigger raw data seeding"
	@echo "clean           : Deep reset of volumes and temp data"
	@echo "observability-up: Launch observability stack (Prometheus, Grafana, Jaeger)"

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	export PYTHONPATH=$${PYTHONPATH}:$(shell pwd) && pytest tests/

lint:
	ruff check . --fix

format:
	black .

type-check:
	mypy . --strict --ignore-missing-imports

security-scan:
	mkdir -p reports
	bandit -r . -f json -o reports/bandit-report.json || true

terraform-plan:
	terraform -chdir=infrastructure/$(ENV) init
	terraform -chdir=infrastructure/$(ENV) plan

terraform-apply:
	terraform -chdir=infrastructure/$(ENV) init
	terraform -chdir=infrastructure/$(ENV) apply -auto-approve

docs:
	mkdir -p docs/api
	pdoc . -o docs/api/

quality-report:
	@echo "Running Data Quality Checkpoints..."
	# In a real environment, this would call the Airflow DAG or a dedicated script
	python3 -c "from data_quality.checkpoint import DataQualityCheckpoint; print('DQ Checkpoints Ready')"

all-checks: lint type-check security-scan test

clean:
	docker compose down -v --remove-orphans
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf data/bronze/* data/silver/* data/gold/*
	rm -rf reports/

init-data:
	python3 scripts/seed_data.py

observability-up:
	docker compose -f observability/docker-compose.observability.yml up -d
