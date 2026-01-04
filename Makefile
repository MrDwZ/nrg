.PHONY: install test run dry-run coverage clean help

# Default target
help:
	@echo "NRG - Narrative Risk Guard"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  install    Install NRG in development mode"
	@echo "  test       Run all tests"
	@echo "  coverage   Run tests with coverage report"
	@echo "  run        Run NRG (full mode)"
	@echo "  dry-run    Run NRG in dry-run mode (no external writes)"
	@echo "  clean      Remove build artifacts"

# Install in development mode
install:
	pip3 install -e .

# Run all tests
test:
	python3 -m pytest tests/ -v

# Run tests with coverage
coverage:
	python3 -m pytest tests/ --cov=src --cov-report=term-missing

# Run NRG (full mode)
run:
	python3 -m src.main

# Run in dry-run mode
dry-run:
	python3 -m src.main --dry-run --no-sheets

# Clean build artifacts
clean:
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
