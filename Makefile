# --- Project Orchestration ---
.PHONY: help up down ps logs test lint clean init-data

help:
	@echo "Enterprise Data Platform Management"
	@echo "------------------------------------"
	@echo "up        : Start all local containers"
	@echo "down      : Stop all local containers"
	@echo "ps        : View service status"
	@echo "logs      : Stream service logs"
	@echo "test      : Execute Pytest suite"
	@echo "lint      : Execute Ruff/Flake8 linting"
	@echo "init-data : Seed raw data for pipeline kick-off"
	@echo "clean     : Deep clean of environment and volumes"

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
	ruff check .
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

clean:
	docker compose down -v --remove-orphans
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf data/bronze/* data/silver/* data/gold/*

init-data:
	python3 scripts/seed_data.py
