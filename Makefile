.PHONY: help install test test-cov test-fast lint format clean

help:
	@echo "Available commands:"
	@echo "  make install     - Install dependencies"
	@echo "  make test        - Run all tests"
	@echo "  make test-cov    - Run tests with coverage report"
	@echo "  make test-fast   - Run tests in parallel"
	@echo "  make lint        - Run linting checks"
	@echo "  make format      - Format code"
	@echo "  make clean       - Clean up generated files"

install:
	pip install -r requirements.txt
	pip install -r requirements-test.txt

test:
	pytest -v

test-cov:
	pytest -v --cov=stac_fastapi.globus_search --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/index.html"

test-fast:
	pytest -v -n auto

test-unit:
	pytest -v -m unit

test-integration:
	pytest -v -m integration

test-watch:
	pytest-watch

lint:
	flake8 src tests
	@echo "Linting complete"

format:
	black src tests
	isort src tests
	@echo "Formatting complete"

clean:
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	@echo "Clean complete"

.DEFAULT_GOAL := help