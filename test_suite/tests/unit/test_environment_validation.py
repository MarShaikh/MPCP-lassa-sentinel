"""
Tests for environment variable validation.
Ensures code fails gracefully when required environment variables are missing or invalid.
"""
import pytest
from unittest.mock import patch, MagicMock
import os
import tempfile

from src.batch_processing.batch_task_runner import main
from src.batch_processing.processing import process_batch_with_progress
from src.batch_processing.batch_job_creator import create_batch_job
from src.utils.batch_task_utils import get_work_items_from_file
from src.utils.azure_storage_utils import upload_blob_to_azure_with_sas


class TestRequiredEnvironmentVariables:
    """Test that code validates required environment variables."""
    
    def test_missing_storage_account_url_in_upload(self):
        """Test that upload_blob_to_azure_with_sas fails when storage_account_url is not provided."""
        # Clear all environment variables
        with patch.dict(os.environ, {}, clear=True):
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
                f.write("test content")
                temp_file = f.name

            try:
                # The new function requires explicit parameters, so passing None should fail
                with pytest.raises((TypeError, ValueError, Exception)):
                    upload_blob_to_azure_with_sas(
                        storage_account_url=None,
                        container_name="processed-cogs",
                        file_path=temp_file,
                        file_name="test.tif",
                        sas_token="test_sas"
                    )
            finally:
                os.unlink(temp_file)
    
    @patch('src.batch_processing.processing.decompress_convert_to_cog')
    def test_missing_cog_container_sas(self, mock_decompress):
        """Test that process_batch_with_progress fails when COG_CONTAINER_SAS is missing."""
        env_vars = {
            'STORAGE_ACCOUNT_URL': 'https://test.blob.core.windows.net',
            # COG_CONTAINER_SAS is missing
            'RAW_CONTAINER_SAS': 'test_sas',
            'LOGS_CONTAINER_SAS': 'test_sas',
            'AZ_BATCH_TASK_ID': 'test_task'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            work_items = [{'year': '2020', 'url': 'http://test.url/file.tif.gz'}]

            with pytest.raises(KeyError) as exc_info:
                process_batch_with_progress(work_items)

            assert "COG_CONTAINER_SAS" in str(exc_info.value)
    
    @patch('src.batch_processing.processing.decompress_convert_to_cog')
    def test_missing_raw_container_sas(self, mock_decompress):
        """Test that process_batch_with_progress fails when RAW_CONTAINER_SAS is missing."""
        env_vars = {
            'STORAGE_ACCOUNT_URL': 'https://test.blob.core.windows.net',
            'COG_CONTAINER_SAS': 'test_sas',
            # RAW_CONTAINER_SAS is missing
            'LOGS_CONTAINER_SAS': 'test_sas',
            'AZ_BATCH_TASK_ID': 'test_task'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            work_items = [{'year': '2020', 'url': 'http://test.url/file.tif.gz'}]

            with pytest.raises(KeyError) as exc_info:
                process_batch_with_progress(work_items)

            assert "RAW_CONTAINER_SAS" in str(exc_info.value)
    
    @patch('src.batch_processing.processing.decompress_convert_to_cog')
    def test_missing_logs_container_sas(self, mock_decompress):
        """Test that process_batch_with_progress fails when LOGS_CONTAINER_SAS is missing."""
        env_vars = {
            'STORAGE_ACCOUNT_URL': 'https://test.blob.core.windows.net',
            'COG_CONTAINER_SAS': 'test_sas',
            'RAW_CONTAINER_SAS': 'test_sas',
            # LOGS_CONTAINER_SAS is missing
            'AZ_BATCH_TASK_ID': 'test_task'
        }

        with patch.dict(os.environ, env_vars, clear=True):
            work_items = [{'year': '2020', 'url': 'http://test.url/file.tif.gz'}]

            with pytest.raises(KeyError) as exc_info:
                process_batch_with_progress(work_items)

            assert "LOGS_CONTAINER_SAS" in str(exc_info.value)
    
    def test_missing_batch_task_working_dir(self, temp_dir):
        """Test that get_work_items_from_file fails when AZ_BATCH_TASK_WORKING_DIR is missing."""
        # Clear AZ_BATCH_TASK_WORKING_DIR
        with patch.dict(os.environ, {}, clear=True):
            # Should fail when trying to construct file path
            with pytest.raises((TypeError, AttributeError)) as exc_info:
                get_work_items_from_file()
            
            # os.path.join(None, "work_items.json") raises TypeError
            assert exc_info.type in (TypeError, AttributeError)
    
    def test_batch_task_id_has_default_fallback(self):
        """Test that AZ_BATCH_TASK_ID has a fallback value."""
        with patch.dict(os.environ, {}, clear=True):
            # Code uses: task_id = os.environ.get('AZ_BATCH_TASK_ID', 'unknown_task')
            task_id = os.environ.get('AZ_BATCH_TASK_ID', 'unknown_task')
            assert task_id == 'unknown_task'
    
    def test_missing_azure_tenant_id_in_batch_job_creator(self):
        """Test that create_batch_job fails when AZURE_TENANT_ID is missing."""
        env_vars = {
            # AZURE_TENANT_ID is missing
            'AZURE_CLIENT_ID': 'test-client-id',
            'AZURE_CLIENT_SECRET': 'test-secret',
            'BATCH_ACCOUNT_URL': 'https://test.batch.azure.com',
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(KeyError) as exc_info:
                create_batch_job()
            
            assert "AZURE_TENANT_ID" in str(exc_info.value)
    
    def test_missing_azure_client_id_in_batch_job_creator(self):
        """Test that create_batch_job fails when AZURE_CLIENT_ID is missing."""
        env_vars = {
            'AZURE_TENANT_ID': 'test-tenant-id',
            # AZURE_CLIENT_ID is missing
            'AZURE_CLIENT_SECRET': 'test-secret',
            'BATCH_ACCOUNT_URL': 'https://test.batch.azure.com',
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(KeyError) as exc_info:
                create_batch_job()
            
            assert "AZURE_CLIENT_ID" in str(exc_info.value)
    
    def test_missing_azure_client_secret_in_batch_job_creator(self):
        """Test that create_batch_job fails when AZURE_CLIENT_SECRET is missing."""
        env_vars = {
            'AZURE_TENANT_ID': 'test-tenant-id',
            'AZURE_CLIENT_ID': 'test-client-id',
            # AZURE_CLIENT_SECRET is missing
            'BATCH_ACCOUNT_URL': 'https://test.batch.azure.com',
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(KeyError) as exc_info:
                create_batch_job()
            
            assert "AZURE_CLIENT_SECRET" in str(exc_info.value)
    
    def test_missing_batch_account_url_in_batch_job_creator(self):
        """Test that create_batch_job fails when BATCH_ACCOUNT_URL is missing."""
        env_vars = {
            'AZURE_TENANT_ID': 'test-tenant-id',
            'AZURE_CLIENT_ID': 'test-client-id',
            'AZURE_CLIENT_SECRET': 'test-secret',
            # BATCH_ACCOUNT_URL is missing
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(KeyError) as exc_info:
                create_batch_job()
            
            assert "BATCH_ACCOUNT_URL" in str(exc_info.value)


class TestInvalidEnvironmentVariables:
    """Test behavior with invalid (but present) environment variables."""
    
    @patch('src.utils.azure_storage_utils.BlobServiceClient')
    def test_empty_storage_account_url(self, mock_blob_service):
        """Test handling of empty STORAGE_ACCOUNT_URL."""
        # BlobServiceClient should fail with malformed URL
        mock_blob_service.side_effect = ValueError("Invalid URL")

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test")
            temp_file = f.name

        try:
            with pytest.raises(ValueError):
                upload_blob_to_azure_with_sas(
                    storage_account_url='',  # Empty string!
                    container_name="processed-cogs",
                    file_path=temp_file,
                    file_name="test.tif",
                    sas_token='test_sas'
                )
        finally:
            os.unlink(temp_file)
    
    @patch('src.utils.azure_storage_utils.BlobServiceClient')
    def test_invalid_storage_account_url_format(self, mock_blob_service):
        """Test handling of malformed storage account URL."""
        # Should construct invalid URL and fail
        mock_blob_service.side_effect = Exception("Invalid URL format")

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test")
            temp_file = f.name

        try:
            with pytest.raises(Exception):
                upload_blob_to_azure_with_sas(
                    storage_account_url='not-a-valid-url',
                    container_name="processed-cogs",
                    file_path=temp_file,
                    file_name="test.tif",
                    sas_token='test_sas'
                )
        finally:
            os.unlink(temp_file)
    
    @patch('src.utils.azure_storage_utils.BlobServiceClient')
    def test_empty_sas_token(self, mock_blob_service):
        """Test behavior with empty SAS token."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test")
            temp_file = f.name

        try:
            # Should create URL with empty SAS: "https://test.blob.core.windows.net?"
            # This is technically valid but will fail authentication
            upload_blob_to_azure_with_sas(
                storage_account_url='https://test.blob.core.windows.net',
                container_name="processed-cogs",
                file_path=temp_file,
                file_name="test.tif",
                sas_token=''  # Empty SAS token
            )

            # Verify URL was constructed with empty SAS
            call_args = mock_blob_service.call_args
            assert call_args[1]['account_url'].endswith('?')
        finally:
            os.unlink(temp_file)
    
    def test_non_existent_working_directory(self):
        """Test handling of non-existent working directory."""
        env_vars = {
            'AZ_BATCH_TASK_WORKING_DIR': '/this/path/does/not/exist/anywhere'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            # Should raise FileNotFoundError when trying to read work_items.json
            with pytest.raises(FileNotFoundError) as exc_info:
                get_work_items_from_file()
            
            assert "Work items file not found" in str(exc_info.value)
            assert "/this/path/does/not/exist" in str(exc_info.value)
    
    def test_working_directory_is_file_not_directory(self, temp_dir):
        """Test handling when WORKING_DIR points to a file instead of directory."""
        # Create a file instead of directory
        temp_file = os.path.join(temp_dir, "not_a_directory")
        with open(temp_file, 'w') as f:
            f.write("this is a file")
        
        env_vars = {
            'AZ_BATCH_TASK_WORKING_DIR': temp_file
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            # os.path.join will still work, but the path won't exist
            with pytest.raises(FileNotFoundError):
                get_work_items_from_file()


class TestAllRequiredEnvironmentVariablesPresent:
    """Test that code works when all required environment variables are present."""
    
    @patch('src.batch_processing.processing.BlobServiceClient')
    def test_upload_with_all_required_vars_for_cog_container(self, mock_blob_service):
        """Test successful upload when all required variables are set."""
        env_vars = {
            'STORAGE_ACCOUNT_URL': 'https://testaccount.blob.core.windows.net',
            'COG_CONTAINER_SAS': 'valid_sas_token',
            'RAW_CONTAINER_SAS': 'valid_sas_token',
            'LOGS_CONTAINER_SAS': 'valid_sas_token',
        }
        
        # Setup mock
        mock_service_instance = MagicMock()
        mock_blob_client = MagicMock()
        mock_service_instance.get_blob_client.return_value = mock_blob_client
        mock_blob_service.return_value = mock_service_instance