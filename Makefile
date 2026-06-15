.PHONY: help up down ps logs test lint clean shell-airflow shell-spark

help:
	@echo "Available commands:"
	@echo "  up             - Start all services with docker-compose"
	@echo "  down           - Stop all services"
	@echo "  ps             - Show running services"
	@echo "  logs           - Follow all logs"
	@echo "  test           - Run all tests using pytest"
	@echo "  lint           - Run flake8 linting"
	@echo "  clean          - Remove temp files and containers"
	@echo "  init-data      - Run scripts to initialize sample data"

up:
	docker-compose up -d

down:
	docker-compose down

ps:
	docker-compose ps

logs:
	docker-compose logs -f

test:
	pytest tests/

lint:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	docker-compose down -v --remove-orphans

init-data:
	python3 scripts/seed_data.py
