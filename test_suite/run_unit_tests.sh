#!/bin/bash

# Script for running unit tests only
echo "================================================"
echo "Running Unit Tests (Mocked - No Azurite)"
echo "================================================"

# Set minimal environment variables needed for imports
export STORAGE_ACCOUNT_URL="https://testaccount.blob.core.windows.net"
export COG_CONTAINER_SAS="mock_sas"
export RAW_CONTAINER_SAS="mock_sas"
export LOGS_CONTAINER_SAS="mock_sas"
export AZ_BATCH_TASK_ID="test_task_001"
export AZ_BATCH_TASK_WORKING_DIR="/tmp/test_batch"

export PYTHONPATH="${PYTHONPATH}:$(dirname $PWD)"

# Run unit tests with coverage
pytest tests/unit/ -v \
  --cov=src.batch_processing \
  --cov-report=html:htmlcov/unit \
  --cov-report=term \
  --tb=short

# Check if tests passed
if [ $? -eq 0 ]; then
    echo ""
    echo "All unit tests passed!"
    echo "Coverage report: htmlcov/unit/index.html"
else
    echo ""
    echo "❌ Some unit tests failed"
    exit 1
fi