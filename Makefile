# Makefile for procurement-intelligence project

.PHONY: help install test lint format clean mypy coverage run run-full run-incremental setup pre-commit

# Default target
help:
	@echo "Available targets:"
	@echo "  make install          - Install dependencies"
	@echo "  make test             - Run all tests"
	@echo "  make lint             - Run ruff linter"
	@echo "  make format           - Format code with ruff"
	@echo "  make mypy             - Run type checking"
	@echo "  make coverage         - Run tests with coverage"
	@echo "  make clean            - Clean cache files"
	@echo "  make setup            - Install dev dependencies"
	@echo "  make pre-commit       - Run all quality checks"
	@echo "  make run              - Run ETL pipeline (dry-run)"
	@echo "  make run-full         - Run ETL pipeline with DB"
	@echo "  make run-incremental  - Run ETL in incremental mode"
	@echo "  make run-reload       - Run ETL pipeline full reload"

# Install dependencies
install:
	uv sync

# Install dev dependencies
setup:
	uv sync
	uv add --dev ruff black mypy pytest pytest-cov

# Run all tests
test:
	uv run pytest data_engineer/tests/ -v

# Run linter (ruff)
lint:
	ruff check data_engineer/src/

# Format code (ruff)
format:
	ruff format data_engineer/src/
	ruff check data_engineer/src/ --fix

# Type checking (mypy)
mypy:
	@echo "Running mypy (informational)..."
	@uv run mypy data_engineer/src/ || true

# Run tests with coverage
coverage:
	uv run pytest data_engineer/tests/ --cov=data_engineer/src --cov-report=term-missing --cov-report=html

# Clean cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf data_engineer/data/errors/*.jsonl 2>/dev/null || true

# Run all quality checks (pre-commit)
pre-commit: lint format mypy test

# Run ETL pipeline (dry-run mode)
run:
	cd data_engineer && uv run python -m src.etl.main

# Run ETL pipeline with database
run-full:
	@echo "Note: Requires PostgreSQL connection"
	@cd data_engineer && uv run python -m src.etl.main --db || echo "Error: PostgreSQL not available"

# Run ETL pipeline in incremental mode
run-incremental:
	@cd data_engineer && uv run python -m src.etl.main --db --incremental

# Run ETL pipeline full reload
run-reload:
	@echo "Note: Requires PostgreSQL connection"
	@cd data_engineer && uv run python -m src.etl.main --db --full || echo "Error: PostgreSQL not available"
