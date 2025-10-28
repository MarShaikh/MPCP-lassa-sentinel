.PHONY: test test-unit test-integration test-all test-coverage clean help install-test-deps

# Default target
help:
	@echo "Available targets:"
	@echo "  make test              - Run unit tests only (default)"
	@echo "  make test-unit         - Run unit tests only"
	@echo "  make test-integration  - Run integration tests (requires Azurite)"
	@echo "  make test-all          - Run all tests with coverage"
	@echo "  make test-coverage     - Run tests and open coverage report"
	@echo "  make test-watch        - Run tests in watch mode (requires pytest-watch)"
	@echo "  make clean             - Remove test artifacts and coverage data"
	@echo "  make install-test-deps - Install test dependencies"
	@echo ""
	@echo "Examples:"
	@echo "  make test                    # Quick unit test run"
	@echo "  make test-all                # Full test suite"
	@echo "  make test-coverage           # Tests + view coverage"

# Default test target - runs unit tests
test: test-unit

# Run only unit tests (fast)
test-unit:
	@echo "Running unit tests..."
	@chmod +x run_unit_tests.sh
	@./run_unit_tests.sh

# Run only integration tests (requires Azurite)
test-integration:
	@echo "Running integration tests..."
	@chmod +x run_integration_tests.sh
	@./run_integration_tests.sh

# Run all tests
test-all:
	@echo "Running complete test suite..."
	@chmod +x run_tests.sh
	@./run_tests.sh --with-integration

# Run tests and open coverage report in browser
test-coverage:
	@echo "Running tests with coverage..."
	@chmod +x run_tests.sh
	@./run_tests.sh --with-integration
	@echo "Opening coverage report..."
	@if command -v xdg-open > /dev/null; then \
		xdg-open htmlcov/index.html; \
	elif command -v open > /dev/null; then \
		open htmlcov/index.html; \
	else \
		echo "Please open htmlcov/index.html manually"; \
	fi

# Run tests in watch mode (re-run on file changes)
test-watch:
	@if ! command -v ptw > /dev/null; then \
		echo "pytest-watch not found. Install with: pip install pytest-watch"; \
		exit 1; \
	fi
	@echo "Running tests in watch mode (Ctrl+C to stop)..."
	@ptw tests/unit/ src/batch_processing/ -- -v --tb=short

# Clean test artifacts
clean:
	@echo "Cleaning test artifacts..."
	@rm -rf .pytest_cache
	@rm -rf htmlcov/
	@rm -rf .coverage*
	@rm -rf coverage.xml
	@rm -rf azurite/
	@rm -rf .mypy_cache/
	@rm -rf **/__pycache__/
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete
	@echo "✅ Cleaned test artifacts"

# Install test dependencies
install-test-deps:
	@echo "Installing test dependencies..."
	@pip install -r tests/requirements-test.txt
	@echo "✅ Test dependencies installed"
	@echo ""
	@echo "Optional: Install Azurite for integration tests"
	@echo "  npm install -g azurite"

# Quick test (unit tests without coverage)
quick:
	@echo "Running quick unit tests (no coverage)..."
	@chmod +x run_tests.sh
	@./run_tests.sh --no-coverage

# Run specific test file
# Usage: make test-file FILE=tests/unit/test_stac_conversion.py
test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE not specified"; \
		echo "Usage: make test-file FILE=tests/unit/test_stac_conversion.py"; \
		exit 1; \
	fi
	@echo "Running tests in $(FILE)..."
	@pytest $(FILE) -v --tb=short

# Run tests matching a pattern
# Usage: make test-pattern PATTERN=test_extract_metadata
test-pattern:
	@if [ -z "$(PATTERN)" ]; then \
		echo "Error: PATTERN not specified"; \
		echo "Usage: make test-pattern PATTERN=test_extract_metadata"; \
		exit 1; \
	fi
	@echo "Running tests matching pattern: $(PATTERN)"
	@pytest tests/ -v -k "$(PATTERN)" --tb=short

# Check test coverage percentage
coverage-check:
	@echo "Checking test coverage..."
	@chmod +x run_tests.sh
	@./run_tests.sh
	@python -m coverage report --fail-under=80 || \
		(echo "⚠️  Warning: Coverage is below 80%"; exit 1)