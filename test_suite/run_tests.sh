#!/bin/bash

# Master test runner - runs all tests (unit + integration)
# with combined coverage reporting

set -e  # Exit on first error

echo "================================================"
echo "Running Complete Test Suite"
echo "================================================"

# Set minimal required environment variables
export STORAGE_ACCOUNT_URL="https://testaccount.blob.core.windows.net"
export COG_CONTAINER_SAS="mock_sas"
export RAW_CONTAINER_SAS="mock_sas"
export LOGS_CONTAINER_SAS="mock_sas"
export STAC_CONTAINER_SAS="mock_sas"
export AZ_BATCH_TASK_ID="test_task_001"
export AZ_BATCH_TASK_WORKING_DIR="/tmp/test_batch"

export PYTHONPATH="${PYTHONPATH}:$(dirname $PWD)"

# Parse command line arguments
RUN_UNIT=true
RUN_INTEGRATION=false
COVERAGE=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-unit)
            RUN_UNIT=false
            shift
            ;;
        --with-integration)
            RUN_INTEGRATION=true
            shift
            ;;
        --no-coverage)
            COVERAGE=false
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-unit            Skip unit tests"
            echo "  --with-integration   Run integration tests (requires Azurite)"
            echo "  --no-coverage        Skip coverage reporting"
            echo "  --help               Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                           # Run unit tests only"
            echo "  $0 --with-integration        # Run all tests"
            echo "  $0 --no-coverage             # Run tests without coverage"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Clean previous coverage data
rm -rf .coverage* htmlcov/

# Run unit tests
if [ "$RUN_UNIT" = true ]; then
    echo ""
    echo "================================================"
    echo "1. Running Unit Tests"
    echo "================================================"
    
    if [ "$COVERAGE" = true ]; then
        pytest tests/unit/ -v \
          --cov=src.batch_processing \
          --cov-append \
          --cov-report= \
          --tb=short
    else
        pytest tests/unit/ -v --tb=short
    fi
    
    UNIT_EXIT_CODE=$?
    
    if [ $UNIT_EXIT_CODE -ne 0 ]; then
        echo "Unit tests failed"
        exit $UNIT_EXIT_CODE
    fi
    
    echo "Unit tests passed"
fi

# Run integration tests if requested
if [ "$RUN_INTEGRATION" = true ]; then
    echo ""
    echo "================================================"
    echo "2. Running Integration Tests (with Azurite)"
    echo "================================================"
    
    # Check if Azurite is installed
    if ! command -v azurite &> /dev/null; then
        echo "Azurite is not installed"
        echo "Install with: npm install -g azurite"
        exit 1
    fi
    
    # Create Azurite directory
    mkdir -p ./azurite
    
    # Start Azurite
    echo "Starting Azurite..."
    azurite --silent --location ./azurite --debug ./azurite/debug.log &
    AZURITE_PID=$!
    sleep 3
    
    # Update environment for Azurite
    export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    export STORAGE_ACCOUNT_URL="http://127.0.0.1:10000/devstoreaccount1"
    
    echo "Azurite started (PID: $AZURITE_PID)"
    
    # Run integration tests
    if [ "$COVERAGE" = true ]; then
        pytest tests/integration/ -v \
          --cov=src.batch_processing \
          --cov-append \
          --cov-report= \
          --tb=short
    else
        pytest tests/integration/ -v --tb=short
    fi
    
    INTEGRATION_EXIT_CODE=$?
    
    # Stop Azurite
    echo "Stopping Azurite..."
    kill $AZURITE_PID 2>/dev/null
    
    if [ $INTEGRATION_EXIT_CODE -ne 0 ]; then
        echo "❌ Integration tests failed"
        exit $INTEGRATION_EXIT_CODE
    fi
    
    echo "Integration tests passed"
fi

# Generate coverage reports
if [ "$COVERAGE" = true ]; then
    echo ""
    echo "================================================"
    echo "Generating Coverage Reports"
    echo "================================================"
    
    pytest --cov-report=html --cov-report=term --cov-report=xml --cov=src.batch_processing tests/ --collect-only 2>/dev/null || true
    
    # Generate actual coverage report from .coverage file
    python -m coverage html
    python -m coverage report
    python -m coverage xml
    
    echo ""
    echo "Coverage reports generated:"
    echo "   - HTML: htmlcov/index.html"
    echo "   - XML:  coverage.xml"
fi

echo ""
echo "================================================"
echo "All Tests Passed!"
echo "================================================"

if [ "$RUN_INTEGRATION" = true ]; then
    echo "Tests run: Unit + Integration"
else
    echo "Tests run: Unit only"
    echo "💡 Tip: Use --with-integration to run integration tests"
fi