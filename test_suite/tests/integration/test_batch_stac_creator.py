"""
Integration tests for batch_stac_job_creator module.

These tests use Azurite for real blob storage operations while mocking
the Azure Batch service (Azurite doesn't support Batch).

Setup:
    - Azurite must be running (handled by run_integration_tests.sh)
    - Tests create real containers and blobs in Azurite
    - Azure Batch operations are mocked
"""
import pytest
import os
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

from azure.storage.blob import BlobServiceClient

from src.stac_creation.batch_job_creator import (
    create_batch_job,
    get_cog_files_to_process,
    filter_existing_stac_items,
    create_and_submit_tasks,
    main
)
from src.utils.batch_task_utils import create_chunks


class TestBatchStacJobCreatorIntegration:
    """Integration tests for batch STAC job creator with Azurite."""
    
    @pytest.fixture
    def azurite_blob_service(self):
        """Create a BlobServiceClient connected to Azurite."""
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        yield blob_service_client
        
        # Cleanup: Delete all test containers
        for container in blob_service_client.list_containers():
            try:
                blob_service_client.delete_container(container.name)
            except:
                pass
    
    @pytest.fixture
    def setup_test_containers(self, azurite_blob_service):
        """Create test containers in Azurite."""
        containers = ['processed-cogs', 'stac-items', 'task-data', 'batch-logs']
        
        for container_name in containers:
            try:
                azurite_blob_service.create_container(container_name)
            except:
                pass
        
        yield azurite_blob_service
    
    @pytest.fixture
    def sample_cog_blobs(self, setup_test_containers):
        """Upload sample COG files to Azurite processed-cogs container."""
        blob_service = setup_test_containers
        container_client = blob_service.get_container_client('processed-cogs')
        
        cog_files = [
            '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif',
            '1981/nigeria-cog-chirps-v2.0.1981.01.02.tif',
            '1982/nigeria-cog-chirps-v2.0.1982.01.01.tif',
            '1982/nigeria-cog-chirps-v2.0.1982.01.02.tif',
        ]
        
        for blob_path in cog_files:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(b'fake cog data', overwrite=True)
        
        yield cog_files
    
    @pytest.fixture
    def sample_existing_stac_items(self, setup_test_containers):
        """Upload sample STAC items to Azurite stac-items container."""
        blob_service = setup_test_containers
        container_client = blob_service.get_container_client('stac-items')
        
        existing_stac = [
            '1981/nigeria-cog-chirps-v2.0.1981.01.01.json',
            '1982/nigeria-cog-chirps-v2.0.1982.01.01.json',
        ]
        
        for blob_path in existing_stac:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(b'{"type": "Feature"}', overwrite=True)
        
        yield existing_stac
    
    @pytest.fixture
    def mock_batch_env_vars(self):
        """Set up environment variables for batch operations."""
        env_vars = {
            'AZURE_TENANT_ID': 'test-tenant-id',
            'AZURE_CLIENT_ID': 'test-client-id',
            'AZURE_CLIENT_SECRET': 'test-client-secret',
            'BATCH_ACCOUNT_URL': 'https://testbatch.batch.azure.com',
            'STORAGE_ACCOUNT_URL': 'http://127.0.0.1:10000/devstoreaccount1',
            'BATCH_STORAGE_ACCOUNT_KEY': 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',  # Azurite default key
        }

        with patch.dict(os.environ, env_vars, clear=False):
            yield env_vars


class TestGetCogFilesToProcess(TestBatchStacJobCreatorIntegration):
    """Tests for get_cog_files_to_process function."""
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_lists_cog_files_from_container(
        self, mock_credential, setup_test_containers, sample_cog_blobs
    ):
        """Test that function lists all COG files from processed-cogs container."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.from_connection_string.return_value = real_blob_service

            work_items = get_cog_files_to_process()
        
        assert len(work_items) == 4
        assert all('year' in item for item in work_items)
        assert all('filename' in item for item in work_items)
        assert all('blob_path' in item for item in work_items)
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_extracts_year_and_filename_correctly(
        self, mock_credential, setup_test_containers, sample_cog_blobs
    ):
        """Test correct extraction of year and filename from blob paths."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.from_connection_string.return_value = real_blob_service

            work_items = get_cog_files_to_process()
        
        # Find specific work item to verify structure
        item_1981_01 = next(
            (item for item in work_items 
             if item['filename'] == 'nigeria-cog-chirps-v2.0.1981.01.01.tif'),
            None
        )
        
        assert item_1981_01 is not None
        assert item_1981_01['year'] == '1981'
        assert item_1981_01['blob_path'] == '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_handles_empty_container(
        self, mock_credential, setup_test_containers
    ):
        """Test function returns empty list when container is empty."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.from_connection_string.return_value = real_blob_service

            work_items = get_cog_files_to_process()
        
        assert work_items == []
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_ignores_non_tif_files(
        self, mock_credential, setup_test_containers
    ):
        """Test that function only includes .tif files."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client('processed-cogs')
        
        # Upload mixed file types
        test_files = [
            '1981/file1.tif',
            '1981/file2.json',
            '1981/file3.txt',
            '1981/file4.tif',
        ]
        
        for blob_path in test_files:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(b'test data', overwrite=True)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            work_items = get_cog_files_to_process()
        
        assert len(work_items) == 2
        assert all(item['filename'].endswith('.tif') for item in work_items)
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_ignores_malformed_paths(
        self, mock_credential, setup_test_containers
    ):
        """Test that function handles malformed paths (not in year/filename format)."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client('processed-cogs')
        
        # Upload files with various path structures
        test_files = [
            '1981/file1.tif',  # Valid
            'file2.tif',  # No year directory
            '1981/subdir/file3.tif',  # Too many path components
            '1982/file4.tif',  # Valid
        ]
        
        for blob_path in test_files:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(b'test data', overwrite=True)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            work_items = get_cog_files_to_process()
        
        # Should only get the two valid ones
        assert len(work_items) == 2
        assert all(len(item['blob_path'].split('/')) == 2 for item in work_items)


class TestFilterExistingStacItems(TestBatchStacJobCreatorIntegration):
    """Tests for filter_existing_stac_items function."""
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_filters_when_all_exist(
        self, mock_credential, setup_test_containers
    ):
        """Test filtering when all STAC items already exist."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        stac_container = blob_service.get_container_client('stac-items')
        
        # Create work items
        work_items = [
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
            },
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.02.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.02.tif'
            }
        ]
        
        # Upload corresponding STAC items
        for item in work_items:
            stac_blob_path = item['blob_path'].replace('.tif', '.json')
            blob_client = stac_container.get_blob_client(stac_blob_path)
            blob_client.upload_blob(b'{"type": "Feature"}', overwrite=True)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service

            filtered = filter_existing_stac_items(work_items)
        
        assert len(filtered) == 0
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_filters_when_none_exist(
        self, mock_credential, setup_test_containers
    ):
        """Test filtering when no STAC items exist."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        work_items = [
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
            },
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.02.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.02.tif'
            }
        ]
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service

            filtered = filter_existing_stac_items(work_items)
        
        assert len(filtered) == 2
        assert filtered == work_items
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_filters_partial_overlap(
        self, mock_credential, setup_test_containers
    ):
        """Test filtering with partial overlap of existing STAC items."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        stac_container = blob_service.get_container_client('stac-items')
        
        work_items = [
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
            },
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.02.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.02.tif'
            },
            {
                'year': '1982',
                'filename': 'nigeria-cog-chirps-v2.0.1982.01.01.tif',
                'blob_path': '1982/nigeria-cog-chirps-v2.0.1982.01.01.tif'
            }
        ]
        
        # Upload STAC item only for the first work item
        stac_blob_path = '1981/nigeria-cog-chirps-v2.0.1981.01.01.json'
        blob_client = stac_container.get_blob_client(stac_blob_path)
        blob_client.upload_blob(b'{"type": "Feature"}', overwrite=True)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service

            filtered = filter_existing_stac_items(work_items)
        
        assert len(filtered) == 2
        assert work_items[0] not in filtered
        assert work_items[1] in filtered
        assert work_items[2] in filtered
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_handles_empty_input(
        self, mock_credential, setup_test_containers
    ):
        """Test filtering with empty work items list."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            filtered = filter_existing_stac_items([])
        
        assert filtered == []
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_correct_stac_filename_mapping(
        self, mock_credential, setup_test_containers
    ):
        """Test that .tif extension is correctly mapped to .json for STAC items."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        stac_container = blob_service.get_container_client('stac-items')
        
        work_items = [
            {
                'year': '1981',
                'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
            }
        ]
        
        # Upload with .json extension
        stac_blob_path = '1981/nigeria-cog-chirps-v2.0.1981.01.01.json'
        blob_client = stac_container.get_blob_client(stac_blob_path)
        blob_client.upload_blob(b'{"type": "Feature"}', overwrite=True)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service

            filtered = filter_existing_stac_items(work_items)
        
        # Should be filtered out because .json version exists
        assert len(filtered) == 0


class TestCreateBatchJob(TestBatchStacJobCreatorIntegration):
    """Tests for create_batch_job function."""
    
    @patch('src.utils.azure_batch_utils.BatchServiceClient')
    @patch('src.utils.azure_batch_utils.ServicePrincipalCredentials')
    def test_create_batch_job_success(
        self, mock_credentials, mock_batch_service, mock_batch_env_vars
    ):
        """Test successful batch job creation."""
        mock_cred_instance = MagicMock()
        mock_credentials.return_value = mock_cred_instance
        
        mock_batch_client = MagicMock()
        mock_batch_service.return_value = mock_batch_client
        
        batch_client, job_id = create_batch_job()
        
        mock_credentials.assert_called_once_with(
            client_id='test-client-id',
            secret='test-client-secret',
            tenant='test-tenant-id',
            resource='https://batch.core.windows.net/'
        )
        
        mock_batch_service.assert_called_once_with(
            mock_cred_instance,
            batch_url='https://testbatch.batch.azure.com'
        )
        
        mock_batch_client.job.add.assert_called_once()
        
        assert job_id.startswith('stac-processing-')
        assert len(job_id.split('-')) >= 3
        
        assert batch_client == mock_batch_client
    
    @patch('src.utils.azure_batch_utils.BatchServiceClient')
    @patch('src.utils.azure_batch_utils.ServicePrincipalCredentials')
    def test_job_id_format_and_uniqueness(
        self, mock_credentials, mock_batch_service, mock_batch_env_vars
    ):
        """Test that job IDs are unique and have correct format."""
        import time
        
        mock_credentials.return_value = MagicMock()
        mock_batch_client = MagicMock()
        mock_batch_service.return_value = mock_batch_client
        
        # Create first job
        _, job_id_1 = create_batch_job()
        
        # Wait to ensure different timestamp
        time.sleep(1.1)
        
        # Create second job
        _, job_id_2 = create_batch_job()
        
        # IDs should be unique
        assert job_id_1 != job_id_2
        assert job_id_1.startswith('stac-processing-')
        assert job_id_2.startswith('stac-processing-')
    
    @patch('src.utils.azure_batch_utils.BatchServiceClient')
    @patch('src.utils.azure_batch_utils.ServicePrincipalCredentials')
    def test_uses_correct_pool(
        self, mock_credentials, mock_batch_service, mock_batch_env_vars
    ):
        """Test that job is created with correct pool information."""
        mock_credentials.return_value = MagicMock()
        mock_batch_client = MagicMock()
        mock_batch_service.return_value = mock_batch_client
        
        batch_client, job_id = create_batch_job()
        
        job_param = mock_batch_client.job.add.call_args[0][0]
        
        assert job_param.id == job_id
        assert job_param.pool_info is not None
        assert job_param.pool_info.pool_id == 'geospatial-processing-pool'


class TestCreateAndSubmitTasks(TestBatchStacJobCreatorIntegration):
    """Tests for create_and_submit_tasks function."""
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_creates_tasks_with_correct_work_items(
        self, mock_credential, setup_test_containers, mock_batch_env_vars
    ):
        """Test that tasks are created with correct work items."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        mock_batch_client = MagicMock()
        job_id = 'test-stac-job-123'
        
        work_items_chunks = [
            [
                {
                    'year': '1981',
                    'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                    'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
                }
            ],
            [
                {
                    'year': '1982',
                    'filename': 'nigeria-cog-chirps-v2.0.1982.01.01.tif',
                    'blob_path': '1982/nigeria-cog-chirps-v2.0.1982.01.01.tif'
                }
            ]
        ]
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        # Verify task_add_collection was called
        mock_batch_client.task.add_collection.assert_called_once()
        
        # Verify correct number of tasks
        call_args = mock_batch_client.task.add_collection.call_args
        tasks = call_args[0][1]  # Second argument is the task list
        
        assert len(tasks) == 2
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_uploads_work_items_to_task_data_container(
        self, mock_credential, setup_test_containers, mock_batch_env_vars
    ):
        """Test that work items JSON files are uploaded to task-data container."""
        mock_credential.return_value = MagicMock()

        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)

        mock_batch_client = MagicMock()
        job_id = 'test-stac-job-456'

        work_items_chunks = [
            [
                {
                    'year': '1981',
                    'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                    'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
                }
            ]
        ]

        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service

            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)

        # Verify blob was uploaded
        container_client = blob_service.get_container_client('task-data')
        blobs = list(container_client.list_blobs())

        assert len(blobs) == 1
        # The actual format is: job_id/stac_task000_work_items.json
        assert 'stac_task' in blobs[0].name
        assert 'work_items.json' in blobs[0].name
        assert job_id in blobs[0].name
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_task_command_line_uses_stac_runner(
        self, mock_credential, setup_test_containers, mock_batch_env_vars
    ):
        """Test that task command line runs batch_stac_task_runner.py."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        mock_batch_client = MagicMock()
        job_id = 'test-stac-job-789'
        
        work_items_chunks = [
            [
                {
                    'year': '1981',
                    'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                    'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
                }
            ]
        ]
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        # Get the tasks that were submitted
        call_args = mock_batch_client.task.add_collection.call_args
        tasks = call_args[0][1]
        
        # Verify command line includes stac runner
        assert 'batch_stac_task_runner.py' in tasks[0].command_line
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_sets_correct_environment_variables(
        self, mock_credential, setup_test_containers, mock_batch_env_vars
    ):
        """Test that task command sets correct environment variables."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        mock_batch_client = MagicMock()
        job_id = 'test-stac-job-101'
        
        work_items_chunks = [
            [
                {
                    'year': '1981',
                    'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                    'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
                }
            ]
        ]
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        # Get the tasks that were submitted
        call_args = mock_batch_client.task.add_collection.call_args
        tasks = call_args[0][1]
        command_line = tasks[0].command_line
        
        # Verify environment variables are set in command
        assert 'STORAGE_ACCOUNT_URL' in command_line
        assert 'COG_CONTAINER_SAS' in command_line
        assert 'STAC_CONTAINER_SAS' in command_line
        assert 'LOGS_CONTAINER_SAS' in command_line
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_generates_sas_tokens_with_correct_permissions(
        self, mock_credential, setup_test_containers, mock_batch_env_vars
    ):
        """Test that SAS tokens are generated (this is implicit in task creation)."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        mock_batch_client = MagicMock()
        job_id = 'test-stac-job-202'
        
        work_items_chunks = [
            [
                {
                    'year': '1981',
                    'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
                    'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
                }
            ]
        ]
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            # Should not raise any exceptions
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        # Verify tasks were submitted
        assert mock_batch_client.task.add_collection.called


class TestMainIntegration(TestBatchStacJobCreatorIntegration):
    """Integration tests for the main workflow."""
    
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_main_no_cog_files_found(
        self, mock_credential, setup_test_containers, mock_batch_env_vars
    ):
        """Test main function when no COG files are found."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            # Should exit gracefully
            main()
    
    @patch('src.utils.azure_batch_utils.create_batch_job_with_pool')
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_main_all_items_already_processed(
        self, mock_credential, mock_create_job,
        setup_test_containers, sample_cog_blobs, sample_existing_stac_items,
        mock_batch_env_vars
    ):
        """Test main function when all COG files already have STAC items."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        # Upload STAC items for all COGs
        stac_container = blob_service.get_container_client('stac-items')
        for cog_path in sample_cog_blobs:
            stac_path = cog_path.replace('.tif', '.json')
            blob_client = stac_container.get_blob_client(stac_path)
            blob_client.upload_blob(b'{"type": "Feature"}', overwrite=True)
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            main()
        
        # Batch job should not be created
        mock_create_job.assert_not_called()
    
    @patch('src.utils.azure_batch_utils.create_batch_job_with_pool')
    @patch('src.utils.azure_batch_utils.create_and_submit_tasks_with_config')
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_main_end_to_end_with_azurite(
        self, mock_credential, mock_submit_tasks, mock_create_job,
        setup_test_containers, sample_cog_blobs, sample_existing_stac_items,
        mock_batch_env_vars
    ):
        """Test main function end-to-end with Azurite for filtering."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        mock_batch_client = MagicMock()
        mock_create_job.return_value = (mock_batch_client, 'test-stac-job-303')
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            main()
        
        # Verify batch job was created
        mock_create_job.assert_called_once()

        # Verify tasks were submitted
        mock_submit_tasks.assert_called_once()

        # Verify filtered work items (should exclude the 2 existing STAC items)
        # After refactoring, function is called with keyword arguments
        work_items_chunks = mock_submit_tasks.call_args.kwargs['work_items_chunks']

        # Flatten chunks to count total items
        total_items = sum(len(chunk) for chunk in work_items_chunks)

        # Should have 4 COGs minus 2 existing STAC items = 2 items to process
        assert total_items == 2
    
    @patch('src.utils.azure_batch_utils.create_batch_job_with_pool')
    @patch('src.utils.azure_batch_utils.create_and_submit_tasks_with_config')
    @patch('src.utils.azure_storage_utils.DefaultAzureCredential')
    def test_main_creates_correct_number_of_tasks(
        self, mock_credential, mock_submit_tasks, mock_create_job,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that main creates correct number of tasks based on chunks."""
        mock_credential.return_value = MagicMock()
        
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        # Upload many COG files to test chunking
        cog_container = blob_service.get_container_client('processed-cogs')
        for i in range(150):  # More than chunk size of 100
            blob_path = f'1981/file{i:04d}.tif'
            blob_client = cog_container.get_blob_client(blob_path)
            blob_client.upload_blob(b'test data', overwrite=True)
        
        mock_batch_client = MagicMock()
        mock_create_job.return_value = (mock_batch_client, 'test-stac-job-404')
        
        with patch('src.utils.azure_storage_utils.BlobServiceClient') as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = blob_service
            
            main()
        
        # Verify tasks were submitted
        mock_submit_tasks.assert_called_once()

        # Verify number of chunks (150 items / 100 per chunk = 2 chunks)
        # After refactoring, function is called with keyword arguments
        work_items_chunks = mock_submit_tasks.call_args.kwargs['work_items_chunks']

        assert len(work_items_chunks) == 2