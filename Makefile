.PHONY: install install-dev test lint format build up down logs clean help pylint mypy ci pre-commit pre-commit-install

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt
	pip install -e ".[dev]"

test:
	pytest -v

lint:
	ruff check .

format:
	ruff format .
	ruff check --fix .

dev:
	uvicorn main:app --reload --host 0.0.0.0 --port 8000

build:
	docker build -t cenotoo-api .

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f api

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf dist/ build/ *.egg-info/

help:
	@echo "Available targets:"
	@echo "  install      Install production dependencies"
	@echo "  install-dev  Install production + dev dependencies"
	@echo "  test         Run test suite"
	@echo "  lint         Run ruff linter"
	@echo "  format       Auto-format code with ruff"
	@echo "  dev          Start dev server with hot reload"
	@echo "  build        Build Docker image"
	@echo "  up           Start all services (docker compose)"
	@echo "  down         Stop all services"
	@echo "  logs         Tail API container logs"
	@echo "  clean        Remove caches and build artifacts"

pylint:
	pylint main.py config.py dependencies.py api/ core/ models/ routers/ services/ utilities/ tests/

mypy:
	mypy main.py config.py dependencies.py api/ core/ models/ routers/ services/ utilities/ --ignore-missing-imports

pre-commit-install:
	pre-commit install

pre-commit:
	pre-commit run --all-files

ci: lint test pylint mypy pre-commit
