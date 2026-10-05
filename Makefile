# Makefile for procurement-intelligence project

.PHONY: help install test lint format clean mypy coverage run run-full run-incremental setup pre-commit

# Default target
help:
	@echo "Available targets:"
	@echo "  make install          - Install dependencies"
	@echo "  make test             - Run all tests"
	@echo "  make lint             - Run ruff linter"
	@echo "  make format           - Format code with ruff"
	@echo "  make typecheck        - Run type checking"
	@echo "  make coverage         - Run tests with coverage"
	@echo "  make clean            - Clean cache files"
	@echo "  make setup            - Install dev dependencies"
	@echo "  make pre-commit       - Run all quality checks"
	@echo ""
	@echo "  ETL Pipeline targets:"
	@echo "  make run              - Run ETL (dry-run, no DB write)"
	@echo "  make run-full         - Full load to DB (loads all data)"
	@echo "  make run-incremental  - Incremental load to DB (only new data)"
	@echo "  make run-reload       - Full reload (clears tables, loads fresh)"

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
typecheck:
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

# ETL Pipeline Targets
# Run ETL pipeline (dry-run mode - no database write)
run:
	cd data_engineer && uv run python -m src.etl

# Run ETL pipeline in full mode (overwrites existing data with UPSERT)
run-full:
	@echo "Note: Requires PostgreSQL connection"
	@cd data_engineer && uv run python -m src.etl --db --full || echo "Error: PostgreSQL not available"

# Run ETL pipeline in incremental mode (only loads new data since last run)
run-incremental:
	@cd data_engineer && uv run python -m src.etl --db --incremental || echo "Error: PostgreSQL not available"

# Run ETL pipeline full reload (clears watermark and does full load)
run-reload:
	@echo "Note: Requires PostgreSQL connection"
	@echo "Resetting watermark for full reload..."
	@rm -f data_engineer/.watermark
	@cd data_engineer && uv run python -m src.etl --db --full || echo "Error: PostgreSQL not available"
