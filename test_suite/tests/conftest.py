"""
Pytest configuration and shared fixtures for all tests.
"""
import os
import pytest
import tempfile
import shutil
from unittest.mock import patch
import gzip

# Configure pytest to find the source modules
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def sample_chirps_data():
    """Create sample CHIRPS TIFF data (minimal valid GeoTIFF structure)."""
    # This is a minimal GeoTIFF header for testing
    # In practice, you'd use a real small CHIRPS file
    return b'II*\x00\x08\x00\x00\x00' + b'\x00' * 1000  # Simplified TIFF structure

@pytest.fixture
def sample_chirps_compressed(sample_chirps_data):
    """Create gzip compressed CHIRPS data."""
    return gzip.compress(sample_chirps_data)

@pytest.fixture
def mock_azure_storage_env():
    """Set up environment variables for Azure storage."""
    env_vars = {
        'STORAGE_ACCOUNT_URL': 'https://testaccount.blob.core.windows.net',
        'COG_CONTAINER_SAS': 'mock_cog_sas_token',
        'RAW_CONTAINER_SAS': 'mock_raw_sas_token',
        'LOGS_CONTAINER_SAS': 'mock_logs_sas_token',
        'AZ_BATCH_TASK_ID': 'test_task_001',
        'AZ_BATCH_TASK_WORKING_DIR': '/tmp/test_batch'
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars

@pytest.fixture
def sample_work_item():
    """Create a sample work item for testing."""
    return {
        'year': '1981',
        'url': 'https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/1981/chirps-v2.0.1981.01.01.tif.gz'
    }

@pytest.fixture
def sample_work_items():
    """Create multiple sample work items."""
    items = []
    for day in range(1, 11):  # 10 days of data
        items.append({
            'year': '1981',
            'url': f'https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/1981/chirps-v2.0.1981.01.{day:02d}.tif.gz'
        })
    return items

@pytest.fixture
def mock_html_response():
    """Create a mock HTML response for web scraping tests."""
    html_content = """
    <html>
    <body>
        <table id="list">
            <tr>
                <td class="link"><a href="1981/">1981/</a></td>
                <td class="size">125.5 KiB</td>
            </tr>
            <tr>
                <td class="link"><a href="1982/">1982/</a></td>
                <td class="size">130.2 KiB</td>
            </tr>
            <tr>
                <td class="link"><a href="chirps-v2.0.1981.01.01.tif.gz">chirps-v2.0.1981.01.01.tif.gz</a></td>
                <td class="size">1024.0 KiB</td>
            </tr>
            <tr>
                <td class="link"><a href="chirps-v2.0.1981.01.02.tif.gz">chirps-v2.0.1981.01.02.tif.gz</a></td>
                <td class="size">1050.5 KiB</td>
            </tr>
        </table>
    </body>
    </html>
    """
    return html_content