#!/bin/bash

# Script for running integration tests with Azurite
# These tests actually interact with Azure Storage emulator

echo "================================================"
echo "Running Integration Tests (with Azurite)"
echo "================================================"

# Check if Azurite is installed
if ! command -v azurite &> /dev/null; then
    echo "Azurite is not installed"
    echo "Install with: npm install -g azurite"
    exit 1
fi

# Create Azurite directory if it doesn't exist
mkdir -p ./azurite

# Start Azurite for Azure Storage emulation
echo "🚀 Starting Azurite..."
azurite --silent --location ./azurite --debug ./azurite/debug.log &
AZURITE_PID=$!

# Wait for Azurite to start
sleep 3

# Export Azurite connection string for tests
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

# Export other required environment variables
export STORAGE_ACCOUNT_URL="http://127.0.0.1:10000/devstoreaccount1"
export COG_CONTAINER_SAS="test_sas"
export RAW_CONTAINER_SAS="test_sas"
export LOGS_CONTAINER_SAS="test_sas"
export STAC_CONTAINER_SAS="test_sas"
export AZ_BATCH_TASK_ID="test_task_001"
export AZ_BATCH_TASK_WORKING_DIR="/tmp/test_batch"

export PYTHONPATH="${PYTHONPATH}:$(dirname $PWD)"

# Verify Azurite is running
sleep 1
if ! ps -p $AZURITE_PID > /dev/null; then
    echo "Failed to start Azurite"
    exit 1
fi

echo "Azurite started (PID: $AZURITE_PID)"
echo ""

# Run integration tests with coverage
pytest tests/integration/ -v \
  --cov=src.batch_processing \
  --cov-report=html:htmlcov/integration \
  --cov-report=term \
  --tb=short

TEST_EXIT_CODE=$?

# Stop Azurite
echo ""
echo "Stopping Azurite..."
kill $AZURITE_PID 2>/dev/null

# Clean up Azurite data (optional - comment out to keep data)
# rm -rf ./azurite

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "All integration tests passed!"
    echo "Coverage report: htmlcov/integration/index.html"
else
    echo "Some integration tests failed"
    exit 1
fi