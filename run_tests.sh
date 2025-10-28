#!/bin/bash

# Start Azurite for Azure Storage emulation
azurite --silent --location ./azurite --debug ./azurite/debug.log &
AZURITE_PID=$!

# Wait for Azurite to start
sleep 2

# Export Azurite connection string for tests
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

# Run tests with coverage
pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Stop Azurite
kill $AZURITE_PID

echo "Tests complete! Coverage report available in htmlcov/index.html"