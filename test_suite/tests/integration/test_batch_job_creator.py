"""
Integration tests for batch_job_creator module.

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

from src.batch_processing.batch_job_creator import (
    create_batch_job,
    create_and_submit_tasks,
    filter_existing_work_items,
    main
)


class TestBatchJobCreatorIntegration:
    """Integration tests for batch job creator with Azurite."""
    
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
        containers = ['processed-cogs', 'task-data', 'raw-data', 'batch-logs']
        
        for container_name in containers:
            try:
                azurite_blob_service.create_container(container_name)
            except:
                pass
        
        yield azurite_blob_service
    
    @pytest.fixture
    def sample_existing_cogs(self, setup_test_containers):
        """Upload sample COG files to Azurite to simulate existing data."""
        blob_service = setup_test_containers
        container_client = blob_service.get_container_client('processed-cogs')
        
        existing_cogs = [
            '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif',
            '1981/nigeria-cog-chirps-v2.0.1981.01.02.tif',
            '1982/nigeria-cog-chirps-v2.0.1982.01.01.tif',
        ]
        
        for blob_path in existing_cogs:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(b'fake cog data', overwrite=True)
        
        yield existing_cogs
        
    @pytest.fixture
    def mock_batch_env_vars(self):
        """Set up environment variables for batch operations."""
        env_vars = {
            'STORAGE_ACCOUNT_URL': 'http://127.0.0.1:10000/devstoreaccount1',
            'AZURE_TENANT_ID': 'test-tenant-id',
            'AZURE_CLIENT_ID': 'test-client-id',
            'AZURE_CLIENT_SECRET': 'test-client-secret',
            'BATCH_ACCOUNT_URL': 'https://testbatch.batch.azure.com',
            'BATCH_STORAGE_ACCOUNT_KEY': 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
        }
        
        with patch.dict(os.environ, env_vars):
            yield env_vars


class TestFilterExistingWorkItems(TestBatchJobCreatorIntegration):
    """Test filtering work items based on existing COGs in Azurite."""
    
    @patch('src.batch_processing.batch_job_creator.DefaultAzureCredential')
    def test_filter_existing_work_items_with_azurite(
        self, mock_credential, sample_existing_cogs, mock_batch_env_vars
    ):
        """Test filtering uses real Azurite blob storage."""
        mock_credential.return_value = MagicMock()
        
        work_items = [
            {'year': '1981', 'url': 'https://data.chc.ucsb.edu/1981/chirps-v2.0.1981.01.01.tif.gz'},
            {'year': '1981', 'url': 'https://data.chc.ucsb.edu/1981/chirps-v2.0.1981.01.02.tif.gz'},
            {'year': '1981', 'url': 'https://data.chc.ucsb.edu/1981/chirps-v2.0.1981.01.03.tif.gz'},
            {'year': '1982', 'url': 'https://data.chc.ucsb.edu/1982/chirps-v2.0.1982.01.01.tif.gz'},
            {'year': '1982', 'url': 'https://data.chc.ucsb.edu/1982/chirps-v2.0.1982.01.02.tif.gz'},
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            filtered_items = filter_existing_work_items(work_items)
        
        assert len(filtered_items) == 2
        remaining_urls = [item['url'] for item in filtered_items]
        assert 'chirps-v2.0.1981.01.03.tif.gz' in remaining_urls[0]
        assert 'chirps-v2.0.1982.01.02.tif.gz' in remaining_urls[1]

class TestCreateAndSubmitTasks(TestBatchJobCreatorIntegration):
    """Test task creation and submission with real blob storage."""
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_task_work_items_uploaded_to_azurite(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that work items are uploaded to Azurite as JSON blobs."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'mock_container_sas'
        mock_blob_sas.return_value = 'mock_blob_sas'
        
        mock_batch_client = MagicMock()
        
        work_items_chunks = [
            [
                {'year': '1981', 'url': 'https://test.url/1981/file1.tif.gz'},
                {'year': '1981', 'url': 'https://test.url/1981/file2.tif.gz'},
            ],
            [
                {'year': '1981', 'url': 'https://test.url/1981/file3.tif.gz'},
            ]
        ]
        
        job_id = 'test-job-123'
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        # Verify blobs were uploaded to Azurite
        container_client = real_blob_service.get_container_client('task-data')
        blobs = list(container_client.list_blobs())
        
        assert len(blobs) == 2
        
        blob_names = [blob.name for blob in blobs]
        assert f'{job_id}/task000_work_items.json' in blob_names
        assert f'{job_id}/task001_work_items.json' in blob_names
        
        # Verify blob content
        blob_client = container_client.get_blob_client(f'{job_id}/task000_work_items.json')
        content = blob_client.download_blob().readall()
        work_items_data = json.loads(content)
        
        assert len(work_items_data) == 2
        assert work_items_data[0]['year'] == '1981'
        assert 'file1.tif.gz' in work_items_data[0]['url']
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_tasks_submitted_to_batch_client(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that tasks are submitted to Azure Batch with correct parameters."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'test_sas_token'
        mock_blob_sas.return_value = 'test_blob_sas'
        
        mock_batch_client = MagicMock()
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file.tif.gz'}]
        ]
        
        job_id = 'test-job-456'
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        mock_batch_client.task.add_collection.assert_called_once()
        
        call_args = mock_batch_client.task.add_collection.call_args
        submitted_job_id = call_args[0][0]
        submitted_tasks = call_args[0][1]
        
        assert submitted_job_id == job_id
        assert len(submitted_tasks) == 1
        
        task = submitted_tasks[0]
        assert task.id == 'task000'
        assert task.resource_files is not None
        assert len(task.resource_files) == 1
        
        resource_file = task.resource_files[0]
        assert 'work_items.json' in resource_file.file_path
        assert resource_file.http_url is not None
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_command_line_includes_environment_variables(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that task command line includes correct environment variables."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'sas_token_123'
        mock_blob_sas.return_value = 'blob_sas_456'
        
        mock_batch_client = MagicMock()
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file.tif.gz'}]
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)
        
        submitted_tasks = mock_batch_client.task.add_collection.call_args[0][1]
        task = submitted_tasks[0]
        command_line = task.command_line
        
        assert 'STORAGE_ACCOUNT_URL' in command_line
        assert 'COG_CONTAINER_SAS' in command_line
        assert 'RAW_CONTAINER_SAS' in command_line
        assert 'LOGS_CONTAINER_SAS' in command_line
        assert 'sas_token_123' in command_line
        
        assert 'git clone' in command_line
        assert 'python3.11' in command_line
        assert 'batch_task_runner.py' in command_line
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_multiple_task_chunks(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test handling multiple work item chunks creates multiple tasks."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'sas'
        mock_blob_sas.return_value = 'blob_sas'
        
        mock_batch_client = MagicMock()
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file1.tif.gz'}],
            [{'year': '1981', 'url': 'https://test.url/file2.tif.gz'}],
            [{'year': '1981', 'url': 'https://test.url/file3.tif.gz'}],
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)
        
        submitted_tasks = mock_batch_client.task.add_collection.call_args[0][1]
        assert len(submitted_tasks) == 3
        
        task_ids = [task.id for task in submitted_tasks]
        assert task_ids == ['task000', 'task001', 'task002']
        
        # Verify 3 blobs were uploaded
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client('task-data')
        blobs = list(container_client.list_blobs())
        assert len(blobs) == 3
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_blob_content_is_valid_json(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that uploaded blob content is valid JSON."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'sas'
        mock_blob_sas.return_value = 'blob_sas'
        
        mock_batch_client = MagicMock()
        
        work_items_chunks = [
            [
                {'year': '1981', 'url': 'https://test.url/file1.tif.gz'},
                {'year': '1982', 'url': 'https://test.url/file2.tif.gz'},
            ]
        ]
        
        job_id = 'test-json-validation'
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, job_id, work_items_chunks)
        
        # Download and verify JSON content
        container_client = real_blob_service.get_container_client('task-data')
        blob_client = container_client.get_blob_client(f'{job_id}/task000_work_items.json')
        content = blob_client.download_blob().readall()
        
        # Should not raise JSONDecodeError
        data = json.loads(content)
        assert isinstance(data, list)
        assert len(data) == 2
        assert all(isinstance(item, dict) for item in data)
        assert all('year' in item and 'url' in item for item in data)


class TestCreateBatchJob(TestBatchJobCreatorIntegration):
    """Test batch job creation (mocked since Azurite doesn't support Batch)."""
    
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
        
        assert job_id.startswith('chirps-processing-')
        assert len(job_id.split('-')) >= 3
        
        assert batch_client == mock_batch_client
    
    @patch('src.utils.azure_batch_utils.BatchServiceClient')
    @patch('src.utils.azure_batch_utils.ServicePrincipalCredentials')
    def test_create_batch_job_with_pool_info(
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
    
    @patch('src.utils.azure_batch_utils.BatchServiceClient')
    @patch('src.utils.azure_batch_utils.ServicePrincipalCredentials')
    def test_create_batch_job_unique_ids(
        self, mock_credentials, mock_batch_service, mock_batch_env_vars
    ):
        """Test that multiple job creations produce unique IDs."""
        import time

        mock_credentials.return_value = MagicMock()
        mock_batch_client = MagicMock()
        mock_batch_service.return_value = mock_batch_client

        # Create first job
        _, job_id_1 = create_batch_job()

        # Wait to ensure different timestamp
        time.sleep(1.1)  # Sleep just over 1 second to ensure different timestamp

        # Create second job
        _, job_id_2 = create_batch_job()

        # IDs should be unique
        assert job_id_1 != job_id_2


class TestMainIntegration(TestBatchJobCreatorIntegration):
    """Integration tests for the main workflow."""
    
    @patch('src.batch_processing.batch_job_creator.create_batch_job')
    @patch('src.batch_processing.batch_job_creator.create_and_submit_tasks')
    @patch('src.batch_processing.batch_job_creator.find_tiff_url')
    @patch('src.batch_processing.batch_job_creator.DefaultAzureCredential')
    def test_main_end_to_end_with_azurite(
        self, mock_credential, mock_find_tiff, mock_submit_tasks, mock_create_job,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test main function with Azurite for filtering and blob operations."""
        mock_credential.return_value = MagicMock()
        
        mock_find_tiff.side_effect = [
            ['https://test.url/1981/', 'https://test.url/1982/'],
            ['https://test.url/1981/chirps-v2.0.1981.01.01.tif.gz'],
            ['https://test.url/1982/chirps-v2.0.1982.01.01.tif.gz'],
        ]
        
        mock_batch_client = MagicMock()
        mock_create_job.return_value = (mock_batch_client, 'test-job-789')
        
        # Upload an existing COG to Azurite
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client('processed-cogs')
        blob_client = container_client.get_blob_client('1981/nigeria-cog-chirps-v2.0.1981.01.01.tif')
        blob_client.upload_blob(b'existing cog', overwrite=True)
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            with patch('src.batch_processing.batch_job_creator.create_chunks') as mock_chunks:
                mock_chunks.return_value = [
                    [{'year': '1982', 'url': 'https://test.url/1982/chirps-v2.0.1982.01.01.tif.gz'}]
                ]
                
                main()
        
        assert mock_find_tiff.call_count == 3
        mock_create_job.assert_called_once()
        mock_submit_tasks.assert_called_once()
        
        submit_call_args = mock_submit_tasks.call_args[0]
        work_items_chunks = submit_call_args[2]
        
        assert len(work_items_chunks) == 1
        assert work_items_chunks[0][0]['year'] == '1982'
    
    @patch('src.batch_processing.batch_job_creator.find_tiff_url')
    @patch('src.batch_processing.batch_job_creator.DefaultAzureCredential')
    def test_main_no_year_urls_found(
        self, mock_credential, mock_find_tiff, 
        setup_test_containers, mock_batch_env_vars
    ):
        """Test main function when no year URLs are found."""
        mock_credential.return_value = MagicMock()
        mock_find_tiff.return_value = []
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            main()
        
        assert mock_find_tiff.call_count == 1
    
    @patch('src.batch_processing.batch_job_creator.create_batch_job')
    @patch('src.batch_processing.batch_job_creator.find_tiff_url')
    @patch('src.batch_processing.batch_job_creator.DefaultAzureCredential')
    def test_main_all_items_already_processed(
        self, mock_credential, mock_find_tiff, mock_create_job,
        setup_test_containers, sample_existing_cogs, mock_batch_env_vars
    ):
        """Test main function when all work items already exist as COGs."""
        mock_credential.return_value = MagicMock()
        
        mock_find_tiff.side_effect = [
            ['https://test.url/1981/'],
            ['https://test.url/1981/chirps-v2.0.1981.01.01.tif.gz',
             'https://test.url/1981/chirps-v2.0.1981.01.02.tif.gz'],
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            main()
        
        mock_create_job.assert_not_called()
    
    @patch('src.batch_processing.batch_job_creator.create_batch_job')
    @patch('src.batch_processing.batch_job_creator.create_and_submit_tasks')
    @patch('src.batch_processing.batch_job_creator.find_tiff_url')
    @patch('src.batch_processing.batch_job_creator.DefaultAzureCredential')
    def test_main_no_work_items_generated(
        self, mock_credential, mock_find_tiff, mock_submit_tasks, mock_create_job,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test main when URLs are found but no work items generated."""
        mock_credential.return_value = MagicMock()
        
        # Return year URLs but empty file lists
        mock_find_tiff.side_effect = [
            ['https://test.url/1981/'],
            [],  # No files in 1981
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            main()
        
        mock_create_job.assert_not_called()
        mock_submit_tasks.assert_not_called()


class TestSASTokenGeneration(TestBatchJobCreatorIntegration):
    """Test SAS token generation with Azurite."""
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_container_sas_tokens_generated(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that SAS tokens are generated for each container."""
        mock_credential.return_value = MagicMock()
        mock_batch_client = MagicMock()
        
        sas_calls = []
        def track_sas_call(*args, **kwargs):
            sas_calls.append(kwargs.get('container_name'))
            return 'test_sas_token'
        
        mock_container_sas.side_effect = track_sas_call
        mock_blob_sas.return_value = 'blob_sas'
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file.tif.gz'}]
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)
        
        assert 'processed-cogs' in sas_calls
        assert 'raw-data' in sas_calls
        assert 'batch-logs' in sas_calls
        assert len(sas_calls) == 3
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_sas_token_expiry(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test that SAS tokens have appropriate expiry times."""
        mock_credential.return_value = MagicMock()
        mock_batch_client = MagicMock()
        
        captured_expiry = []
        def capture_expiry(*args, **kwargs):
            if 'expiry' in kwargs:
                captured_expiry.append(kwargs['expiry'])
            return 'test_sas_token'
        
        mock_container_sas.side_effect = capture_expiry
        mock_blob_sas.side_effect = capture_expiry
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file.tif.gz'}]
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)
        
        # Should have expiry times for container and blob SAS tokens
        assert len(captured_expiry) > 0
        
        # All expiry times should be in the future
        now = datetime.now(timezone.utc)
        for expiry in captured_expiry:
            assert expiry > now


class TestErrorHandling(TestBatchJobCreatorIntegration):
    """Test error handling in integration scenarios."""
    
    @patch('src.batch_processing.batch_job_creator.DefaultAzureCredential')
    def test_filter_handles_missing_container(
        self, mock_credential, azurite_blob_service, mock_batch_env_vars
    ):
        """Test filtering gracefully handles missing processed-cogs container."""
        mock_credential.return_value = MagicMock()
        
        work_items = [
            {'year': '1981', 'url': 'https://test.url/file.tif.gz'}
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            with pytest.raises(Exception):
                filter_existing_work_items(work_items)
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_task_submission_with_batch_api_failure(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test handling of batch API failure during task submission."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'sas'
        mock_blob_sas.return_value = 'blob_sas'
        
        mock_batch_client = MagicMock()
        mock_batch_client.task.add_collection.side_effect = Exception("Batch API error")
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file.tif.gz'}]
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            with pytest.raises(Exception, match="Batch API error"):
                create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_partial_blob_upload_failure(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        setup_test_containers, mock_batch_env_vars
    ):
        """Test behavior when some but not all blob uploads fail."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'sas'
        mock_blob_sas.return_value = 'blob_sas'
        mock_batch_client = MagicMock()

        # Create multiple chunks
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file1.tif.gz'}],
            [{'year': '1981', 'url': 'https://test.url/file2.tif.gz'}],
        ]

        # Make the second upload fail
        upload_count = [0]
        def failing_upload(*args, **kwargs):
            upload_count[0] += 1
            if upload_count[0] == 2:
                raise Exception("Upload failed for second blob")

        # Need to patch BlobServiceClient where it's created (in utils module)
        with patch('src.utils.azure_batch_utils.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)

            # Wrap upload_blob to inject failure
            original_get_blob_client = real_blob_service.get_blob_client
            def get_blob_client_wrapper(*args, **kwargs):
                blob_client = original_get_blob_client(*args, **kwargs)
                original_upload = blob_client.upload_blob
                blob_client.upload_blob = lambda *a, **k: (
                    failing_upload(*a, **k) or original_upload(*a, **k)
                )
                return blob_client

            real_blob_service.get_blob_client = get_blob_client_wrapper
            mock_blob_service.return_value = real_blob_service
            mock_blob_service.from_connection_string = lambda cs: real_blob_service

            # Should raise exception on second upload
            with pytest.raises(Exception, match="Upload failed for second blob"):
                create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)


class TestTaskDataContainerCreation(TestBatchJobCreatorIntegration):
    """Test automatic creation of task-data container."""
    
    @patch('src.utils.azure_batch_utils.DefaultAzureCredential')
    @patch('src.utils.azure_batch_utils.generate_container_sas')
    @patch('src.utils.azure_batch_utils.generate_blob_sas')
    def test_task_data_container_must_exist(
        self, mock_blob_sas, mock_container_sas, mock_credential,
        azurite_blob_service, mock_batch_env_vars
    ):
        """Test that task-data container must exist before task submission."""
        mock_credential.return_value = MagicMock()
        mock_container_sas.return_value = 'sas'
        mock_blob_sas.return_value = 'blob_sas'
        mock_batch_client = MagicMock()
        
        # Ensure task-data doesn't exist
        try:
            azurite_blob_service.delete_container('task-data')
        except:
            pass
        
        work_items_chunks = [
            [{'year': '1981', 'url': 'https://test.url/file.tif.gz'}]
        ]
        
        with patch('src.batch_processing.batch_job_creator.BlobServiceClient') as mock_blob_service:
            connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            real_blob_service = BlobServiceClient.from_connection_string(connection_string)
            mock_blob_service.return_value = real_blob_service
            
            # Should raise error when container doesn't exist
            from azure.core.exceptions import ResourceNotFoundError
            with pytest.raises(ResourceNotFoundError, match="container does not exist"):
                create_and_submit_tasks(mock_batch_client, 'test-job', work_items_chunks)