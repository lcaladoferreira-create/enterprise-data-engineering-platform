# --- Platform Management Interface ---
.PHONY: help up down ps logs test lint clean init-data

help:
	@echo "Platform Management Commands"
	@echo "----------------------------"
	@echo "up        : Launch local services"
	@echo "down      : Shutdown local services"
	@echo "ps        : List running containers"
	@echo "logs      : Follow container logs"
	@echo "test      : Run Pytest logic verification"
	@echo "lint      : Run Ruff/Flake8 static analysis"
	@echo "init-data : Trigger raw data seeding"
	@echo "clean     : Deep reset of volumes and temp data"

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

clean:
	docker compose down -v --remove-orphans
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf data/bronze/* data/silver/* data/gold/*

init-data:
	python3 scripts/seed_data.py
