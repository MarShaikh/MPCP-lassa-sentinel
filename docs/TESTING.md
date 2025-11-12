# Testing Guide

This guide covers how to run and manage tests for the Microsoft Planetary Computer Pipelines project.

## Table of Contents

1. [Overview](#overview)
2. [Running Tests](#running-tests)
3. [Setting Up Azurite](#setting-up-azurite-for-integration-tests)
4. [Test Structure](#test-structure)

---

## Overview

The project includes comprehensive unit and integration tests. Tests are located in `test_suite/` and use pytest with Azurite (local Azure Storage emulator) for integration tests.

### Test Types

- **Unit Tests** (`tests/unit/`) - Fast unit tests with mocked Azure dependencies
- **Integration Tests** (`tests/integration/`) - Integration tests using Azurite

---

## Running Tests

From the `test_suite/` directory:

### All Tests with Coverage

```bash
make test-all
```

This runs both unit and integration tests with coverage reporting.

### Unit Tests Only (Fast)

```bash
make test-unit
```

Unit tests run quickly and don't require Azurite.

### Integration Tests Only

```bash
make test-integration
```

**Note:** Integration tests require Azurite to be running (see setup below).

### Run Specific Test File

```bash
make test-file FILE=tests/unit/test_stac_conversion.py
```

### Run Tests Matching Pattern

```bash
make test-pattern PATTERN=test_extract_metadata
```

This runs all tests whose names match the given pattern.

### Clean Test Artifacts

```bash
make clean
```

Removes `__pycache__`, `.pytest_cache`, coverage files, and other test artifacts.

---

## Setting Up Azurite for Integration Tests

Integration tests require Azurite, a local Azure Storage emulator.

### Install Azurite

```bash
npm install -g azurite
```

### Start Azurite

```bash
azurite --silent --location ./azurite --debug ./azurite/debug.log
```

This starts Azurite in the background with:
- Blob storage endpoint at `http://127.0.0.1:10000`
- Debug logging to `./azurite/debug.log`

### Set Environment Variable

```bash
export AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```

This connection string is the standard Azurite connection string and doesn't need to be changed.

### Verify Azurite is Running

```bash
curl http://127.0.0.1:10000/devstoreaccount1?comp=list
```

You should receive an XML response listing containers (empty if none created yet).

---

## Test Structure

### Directory Layout

```
test_suite/
├── tests/
│   ├── unit/               # Unit tests with mocked dependencies
│   │   ├── test_stac_conversion.py
│   │   ├── test_batch_utils.py
│   │   └── ...
│   ├── integration/        # Integration tests with Azurite
│   │   ├── test_storage_integration.py
│   │   └── ...
│   └── conftest.py           # Shared pytest fixtures  
│
├── Makefile                # Test commands
│── run_integration_tests.sh
├── run_tests.sh
└── run_unit_tests.sh

```

### Shared Fixtures

Common pytest fixtures are defined in `tests/conftest.py` and include:
- Mock Azure credentials
- Azurite blob service clients
- Sample STAC items and collections
- Test data files

### Writing New Tests

When writing new tests:

1. **Unit Tests**: Mock all Azure dependencies
   ```python
   @patch('src.utils.azure_storage_utils.BlobServiceClient')
   def test_function(mock_blob_client):
       # Your test code
   ```

2. **Integration Tests**: Use Azurite connection string
   ```python
   from azure.storage.blob import BlobServiceClient

   def test_integration():
       conn_string = os.environ["AZURITE_CONNECTION_STRING"]
       client = BlobServiceClient.from_connection_string(conn_string)
       # Your test code
   ```

3. **Use Fixtures**: Leverage shared fixtures for common setup
   ```python
   def test_with_fixture(mock_blob_client):
       # fixture automatically injected
   ```

---

## Troubleshooting Tests

### Issue: Integration Tests Failing

**Symptoms:**
- Connection errors to blob storage
- "Container not found" errors

**Solution:**

1. Ensure Azurite is running:
   ```bash
   azurite --silent --location ./azurite --debug ./azurite/debug.log
   ```

2. Verify connection string is set:
   ```bash
   echo $AZURITE_CONNECTION_STRING
   ```

3. Check Azurite debug log for errors:
   ```bash
   tail -f ./azurite/debug.log
   ```

### Issue: Mock Paths Not Found

**Symptoms:**
- `AttributeError: Mock object has no attribute...`
- Mocks not being applied

**Solution:**
Verify mock paths match refactored imports:

```python
# Check imports in the module you're testing
# If the module imports: from src.utils.azure_storage_utils import ...
# Then mock: @patch('src.utils.azure_storage_utils...')
```

### Issue: Slow Test Execution

**Solution:**
Run only unit tests for faster feedback:
```bash
make test-unit
```

Integration tests are slower due to Azurite I/O operations.

---

## Additional Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Azurite Documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)

---

**Last Updated:** November 2025
