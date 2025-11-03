"""
Integration tests for batch_stac_task_runner module.

These tests use Azurite for real blob storage operations while mocking
STAC conversion functions to avoid requiring actual COG files.

Setup:
    - Azurite must be running (handled by run_integration_tests.sh)
    - Tests create real containers and blobs in Azurite
    - STAC conversion functions (process_cog_to_stac) are mocked
"""
import pytest
import os
import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, call, mock_open
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient

from src.batch_stac_task_runner import (
    get_work_items_from_file,
    setup_working_directories,
    process_single_cog,
    process_batch_with_progress,
    update_progress_file,
    main
)


class TestGetWorkItemsFromFile:
    """Tests for get_work_items_from_file function."""
    
    def test_read_valid_work_items(self, mock_batch_stac_env, stac_work_items_json_file, 
                                   sample_stac_work_items):
        """Test successfully reading valid work items JSON file."""
        result = get_work_items_from_file()
        
        assert isinstance(result, list)
        assert len(result) == len(sample_stac_work_items)
        assert result[0]['year'] == '1981'
        assert 'filename' in result[0]
        assert 'blob_path' in result[0]
        assert result == sample_stac_work_items
    
    def test_read_empty_work_items(self, mock_batch_stac_env, empty_stac_work_items_file):
        """Test reading empty work items list."""
        result = get_work_items_from_file()
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_file_not_found(self, temp_dir):
        """Test error when work_items.json doesn't exist."""
        env_vars = {
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            with pytest.raises(FileNotFoundError) as exc_info:
                get_work_items_from_file()
            
            assert "Work items file not found" in str(exc_info.value)
            assert temp_dir in str(exc_info.value)
    
    def test_malformed_json(self, mock_batch_stac_env, malformed_stac_json_file):
        """Test error handling for malformed JSON."""
        with pytest.raises(ValueError) as exc_info:
            get_work_items_from_file()
        
        assert "Failed to read or parse work_items.json" in str(exc_info.value)
    
    def test_reads_from_correct_directory(self, temp_dir, sample_stac_work_items):
        """Test that function reads from AZ_BATCH_TASK_WORKING_DIR."""
        subdir = os.path.join(temp_dir, "batch_subdir")
        os.makedirs(subdir, exist_ok=True)
        
        file_path = os.path.join(subdir, "work_items.json")
        with open(file_path, 'w') as f:
            json.dump(sample_stac_work_items, f)
        
        env_vars = {
            'AZ_BATCH_TASK_WORKING_DIR': subdir
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            result = get_work_items_from_file()
            assert len(result) == 3
            assert result[0]['year'] == '1981'


class TestSetupWorkingDirectories:
    """Tests for setup_working_directories function."""
    
    def test_creates_all_directories(self):
        """Test that all required directories are created."""
        base_dir = setup_working_directories()
        
        # Check that directories exist
        assert os.path.exists(base_dir)
        assert os.path.exists(os.path.join(base_dir, "stac-items"))
        assert os.path.exists("/tmp/batch-logs")
        
        # Check that they are directories
        assert os.path.isdir(base_dir)
        assert os.path.isdir(os.path.join(base_dir, "stac-items"))
        assert os.path.isdir("/tmp/batch-logs")
        
        # Verify base_dir is returned
        assert base_dir == "/tmp/stac_processing"
    
    def test_handles_existing_directories(self):
        """Test that function doesn't fail if directories already exist."""
        # Create directories first
        base_dir1 = setup_working_directories()
        
        # Call again - should not raise error
        base_dir2 = setup_working_directories()
        
        # Verify they still exist and are the same
        assert base_dir1 == base_dir2
        assert os.path.exists(os.path.join(base_dir2, "stac-items"))
        assert os.path.exists("/tmp/batch-logs")


class TestProcessSingleCog:
    """Tests for process_single_cog function."""
    
    @patch('src.batch_stac_task_runner.save_stac_item_to_blob')
    @patch('src.batch_stac_task_runner.process_cog_to_stac')
    def test_process_single_cog_success(self, mock_process_stac, mock_save_stac, 
                                       mock_stac_azure_env):
        """Test successful processing of a single COG to STAC."""
        # Mock the STAC conversion
        mock_stac_dict = {
            'id': 'nigeria-cog-chirps-v2.0.1981.01.01',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'properties': {'product_type': 'chirps'}
        }
        mock_process_stac.return_value = mock_stac_dict
        
        # Mock container and service clients
        mock_cog_container = MagicMock()
        mock_blob_client = MagicMock()
        mock_cog_container.get_blob_client.return_value = mock_blob_client
        mock_stac_service = MagicMock()
        
        # Work item
        work_item = {
            'year': '1981',
            'filename': 'nigeria-cog-chirps-v2.0.1981.01.01.tif',
            'blob_path': '1981/nigeria-cog-chirps-v2.0.1981.01.01.tif'
        }
        
        # Execute
        result = process_single_cog(work_item, mock_cog_container, mock_stac_service)
        
        # Verify result
        assert result['success'] is True
        assert result['blob_path'] == work_item['blob_path']
        assert result['stac_path'] == '1981/nigeria-cog-chirps-v2.0.1981.01.01.json'
        
        # Verify process_cog_to_stac was called correctly
        mock_process_stac.assert_called_once_with(
            blob_client=mock_blob_client,
            filename=work_item['filename'],
            blob_domain=os.environ["STORAGE_ACCOUNT_URL"],
            container_name="processed-cogs",
            year='1981'
        )
        
        # Verify save_stac_item_to_blob was called correctly
        mock_save_stac.assert_called_once_with(
            stac_item_dict=mock_stac_dict,
            blob_service_client=mock_stac_service,
            container_name="stac-items",
            blob_path='1981/nigeria-cog-chirps-v2.0.1981.01.01.json'
        )
    
    @patch('src.batch_stac_task_runner.save_stac_item_to_blob')
    @patch('src.batch_stac_task_runner.process_cog_to_stac')
    def test_process_single_cog_conversion_failure(self, mock_process_stac, 
                                                   mock_save_stac, mock_stac_azure_env):
        """Test handling of STAC conversion failure."""
        # Mock conversion failure
        mock_process_stac.side_effect = Exception("Failed to process COG")
        
        mock_cog_container = MagicMock()
        mock_stac_service = MagicMock()
        
        work_item = {
            'year': '1981',
            'filename': 'invalid.tif',
            'blob_path': '1981/invalid.tif'
        }
        
        # Execute
        result = process_single_cog(work_item, mock_cog_container, mock_stac_service)
        
        # Verify failure result
        assert result['success'] is False
        assert result['blob_path'] == work_item['blob_path']
        assert 'error' in result
        assert 'Failed to process COG' in result['error']
        assert 'traceback' in result
        
        # Verify save was not called
        mock_save_stac.assert_not_called()
    
    @patch('src.batch_stac_task_runner.save_stac_item_to_blob')
    @patch('src.batch_stac_task_runner.process_cog_to_stac')
    def test_process_single_cog_save_failure(self, mock_process_stac, 
                                            mock_save_stac, mock_stac_azure_env):
        """Test handling of blob save failure."""
        # Mock successful conversion but failed save
        mock_stac_dict = {'id': 'test-item'}
        mock_process_stac.return_value = mock_stac_dict
        mock_save_stac.side_effect = Exception("Failed to upload blob")
        
        mock_cog_container = MagicMock()
        mock_stac_service = MagicMock()
        
        work_item = {
            'year': '1981',
            'filename': 'test.tif',
            'blob_path': '1981/test.tif'
        }
        
        # Execute
        result = process_single_cog(work_item, mock_cog_container, mock_stac_service)
        
        # Verify failure result
        assert result['success'] is False
        assert 'Failed to upload blob' in result['error']


class TestUpdateProgressFile:
    """Tests for update_progress_file function with Azurite integration."""
    
    @pytest.fixture
    def azurite_setup(self):
        """Setup Azurite connection and create logs container."""
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        # Create logs container
        try:
            blob_service.create_container('batch-logs')
        except:
            pass
        
        yield blob_service
        
        # Cleanup
        try:
            blob_service.delete_container('batch-logs')
        except:
            pass
    
    def test_update_progress_file_uploads_to_azurite(self, azurite_setup, 
                                                     mock_stac_azure_env):
        """Test that progress file is created and uploaded to Azurite."""
        task_id = 'stac_test_task_001'
        completed = [
            {'success': True, 'blob_path': '1981/file1.tif', 'stac_path': '1981/file1.json'},
            {'success': True, 'blob_path': '1981/file2.tif', 'stac_path': '1981/file2.json'}
        ]
        failed = [
            {'success': False, 'blob_path': '1981/file3.tif', 'error': 'Test error'}
        ]
        total = 10
        
        storage_account_url = os.environ['STORAGE_ACCOUNT_URL']
        logs_sas = 'test_sas_token'
        
        # Mock BlobServiceClient to use real Azurite
        with patch('src.batch_stac_task_runner.BlobServiceClient') as mock_bs:
            real_blob_service = azurite_setup
            mock_bs.return_value = real_blob_service
            
            # Execute
            update_progress_file(task_id, completed, failed, total, 
                               storage_account_url, logs_sas)
        
        # Verify blob was uploaded to Azurite
        blob_client = real_blob_service.get_blob_client(
            container='batch-logs',
            blob=f'stac_{task_id}_progress.json'
        )
        
        # Download and verify content
        blob_data = blob_client.download_blob().readall()
        progress_data = json.loads(blob_data)
        
        assert progress_data['task_id'] == task_id
        assert progress_data['total_items'] == total
        assert progress_data['completed_count'] == 2
        assert progress_data['failed_count'] == 1
        assert progress_data['progress_percentage'] == 20.0
        assert len(progress_data['completed_items']) == 2
        assert len(progress_data['failed_items']) == 1
    
    def test_update_progress_file_handles_empty_lists(self, azurite_setup, 
                                                      mock_stac_azure_env):
        """Test progress file with empty completed and failed lists."""
        task_id = 'stac_test_empty'
        
        with patch('src.batch_stac_task_runner.BlobServiceClient') as mock_bs:
            real_blob_service = azurite_setup
            mock_bs.return_value = real_blob_service
            
            update_progress_file(task_id, [], [], 0, 
                               os.environ['STORAGE_ACCOUNT_URL'], 'sas')
        
        blob_client = real_blob_service.get_blob_client(
            container='batch-logs',
            blob=f'stac_{task_id}_progress.json'
        )
        
        blob_data = blob_client.download_blob().readall()
        progress_data = json.loads(blob_data)
        
        assert progress_data['completed_count'] == 0
        assert progress_data['failed_count'] == 0
        assert progress_data['progress_percentage'] == 0


class TestProcessBatchWithProgress:
    """Tests for process_batch_with_progress orchestration with Azurite."""
    
    @pytest.fixture
    def azurite_containers(self):
        """Setup Azurite with required containers."""
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
        containers = ['processed-cogs', 'stac-items', 'batch-logs']
        for container in containers:
            try:
                blob_service.create_container(container)
            except:
                pass
        
        # Upload sample COG metadata
        cog_container = blob_service.get_container_client('processed-cogs')
        for i in range(3):
            blob_client = cog_container.get_blob_client(f'1981/file{i}.tif')
            blob_client.upload_blob(b'fake cog data', overwrite=True)
        
        yield blob_service
        
        # Cleanup
        for container in containers:
            try:
                blob_service.delete_container(container)
            except:
                pass
    
    @patch('src.batch_stac_task_runner.update_progress_file')
    @patch('src.batch_stac_task_runner.save_stac_item_to_blob')
    @patch('src.batch_stac_task_runner.process_cog_to_stac')
    @patch('src.batch_stac_task_runner.BlobServiceClient')
    def test_process_batch_success(self, mock_blob_service_class, mock_process_stac, 
                                   mock_save_stac, mock_update_progress, azurite_containers, 
                                   mock_stac_azure_env):
        """Test successful batch processing with progress tracking."""
        
        # Mock BlobServiceClient to return real Azurite connection
        mock_blob_service_class.return_value = azurite_containers
        
        progress_calls = []
        def track_progress(*args, **kwargs):
            progress_calls.append((args, kwargs))
        mock_update_progress.side_effect = track_progress
        
        # Mock STAC conversion to succeed
        def create_mock_stac(blob_client, filename, **kwargs):
            return {
                'id': filename.replace('.tif', ''),
                'type': 'Feature',
                'properties': {}
            }
        
        mock_process_stac.side_effect = create_mock_stac
        
        # Work items
        work_items = [
            {'year': '1981', 'filename': 'file0.tif', 'blob_path': '1981/file0.tif'},
            {'year': '1981', 'filename': 'file1.tif', 'blob_path': '1981/file1.tif'},
            {'year': '1981', 'filename': 'file2.tif', 'blob_path': '1981/file2.tif'}
        ]
        
        # Execute
        process_batch_with_progress(work_items)
        
        # Verify all items were processed
        assert mock_process_stac.call_count == 3
        assert mock_save_stac.call_count == 3
        
        assert mock_update_progress.called
        assert len(progress_calls) == 1
    
    @patch('src.batch_stac_task_runner.update_progress_file')
    @patch('src.batch_stac_task_runner.save_stac_item_to_blob')
    @patch('src.batch_stac_task_runner.process_cog_to_stac')
    @patch('src.batch_stac_task_runner.BlobServiceClient')
    def test_process_batch_with_failures(self, mock_blob_service_class, mock_process_stac,
                                         mock_save_stac, mock_update_progress, azurite_containers,
                                         mock_stac_azure_env):
        """Test batch processing with some failures."""

        # Mock BlobServiceClient to return real Azurite connection
        mock_blob_service_class.return_value = azurite_containers

        # Mock first to fail, second to succeed, third to fail
        mock_process_stac.side_effect = [
            Exception("Failed to process"),
            {'id': 'success', 'type': 'Feature'},
            Exception("Another failure")
        ]

        work_items = [
            {'year': '1981', 'filename': 'fail1.tif', 'blob_path': '1981/fail1.tif'},
            {'year': '1981', 'filename': 'success.tif', 'blob_path': '1981/success.tif'},
            {'year': '1981', 'filename': 'fail2.tif', 'blob_path': '1981/fail2.tif'}
        ]

        # Execute
        process_batch_with_progress(work_items)

        # Verify all were attempted
        assert mock_process_stac.call_count == 3

        # Only one should have been saved
        assert mock_save_stac.call_count == 1

        # Verify the progress call had the correct counts
        mock_update_progress.assert_called_once()

        call_kwargs = mock_update_progress.call_args.kwargs

        assert len(call_kwargs['completed']) == 1  # One success
        assert len(call_kwargs['failed']) == 2     # Two failures
    
    @patch('src.batch_stac_task_runner.update_progress_file')
    @patch('src.batch_stac_task_runner.save_stac_item_to_blob')
    @patch('src.batch_stac_task_runner.process_cog_to_stac')
    @patch('src.batch_stac_task_runner.BlobServiceClient')
    def test_process_batch_progress_updates_every_10_items(self, mock_blob_service_class,
                                                          mock_process_stac, mock_save_stac,
                                                          mock_update_progress, azurite_containers,
                                                          mock_stac_azure_env):
        """Test that progress is updated every 10 items."""
        # Mock BlobServiceClient to return real Azurite connection
        mock_blob_service_class.return_value = azurite_containers

        # Mock successful processing
        mock_process_stac.return_value = {'id': 'test', 'type': 'Feature'}

        # Create 25 work items
        work_items = [
            {'year': '1981', 'filename': f'file{i}.tif', 'blob_path': f'1981/file{i}.tif'}
            for i in range(25)
        ]

        # Execute
        process_batch_with_progress(work_items)

        # Should update at items 10, 20, and 25 (end) = 3 updates
        assert mock_update_progress.call_count == 3

        # mock_update_progress.call_args_list contains a list of all calls made
        completed_counts = [len(call.kwargs['completed']) for call in mock_update_progress.call_args_list]
        assert completed_counts == [10, 20, 25]


class TestMainFunction:
    """Integration tests for main function."""
    
    @patch('src.batch_stac_task_runner.process_batch_with_progress')
    def test_main_success(self, mock_process_batch, mock_batch_stac_env, 
                         stac_work_items_json_file, sample_stac_work_items):
        """Test successful execution of main function."""
        # Run main
        main()
        
        # Verify process_batch_with_progress was called once
        mock_process_batch.assert_called_once()
        
        # Verify it was called with correct work items
        call_args = mock_process_batch.call_args[0][0]
        assert call_args == sample_stac_work_items
        assert len(call_args) == 3
    
    @patch('src.batch_stac_task_runner.process_batch_with_progress')
    def test_main_with_empty_work_items(self, mock_process_batch, mock_batch_stac_env, 
                                       empty_stac_work_items_file):
        """Test main function with empty work items list."""
        main()
        
        # Should still call process_batch_with_progress with empty list
        mock_process_batch.assert_called_once_with([])
    
    @patch('src.batch_stac_task_runner.process_batch_with_progress')
    def test_main_exits_on_file_not_found(self, mock_process_batch, temp_dir):
        """Test main exits with error code when work items file not found."""
        env_vars = {
            'AZ_BATCH_TASK_ID': 'test_stac_task_001',
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir,
            'STORAGE_ACCOUNT_URL': 'http://test.url',
            'COG_CONTAINER_SAS': 'sas1',
            'STAC_CONTAINER_SAS': 'sas2',
            'LOGS_CONTAINER_SAS': 'sas3'
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            # Should exit with code 1
            assert exc_info.value.code == 1
        
        # process_batch_with_progress should not be called
        mock_process_batch.assert_not_called()
    
    @patch('src.batch_stac_task_runner.process_batch_with_progress')
    def test_main_exits_on_json_error(self, mock_process_batch, mock_batch_stac_env, 
                                     malformed_stac_json_file):
        """Test main exits with error code on JSON parsing error."""
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 1
        mock_process_batch.assert_not_called()
    
    @patch('src.batch_stac_task_runner.process_batch_with_progress')
    def test_main_exits_on_processing_error(self, mock_process_batch, mock_batch_stac_env, 
                                           stac_work_items_json_file):
        """Test main exits with error code when processing fails."""
        # Mock processing to raise exception
        mock_process_batch.side_effect = Exception("Processing failed")
        
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 1
    
    @patch('src.batch_stac_task_runner.process_batch_with_progress')
    def test_full_stac_task_execution_flow(self, mock_process_batch, temp_dir):
        """Test complete STAC task flow from file read to processing."""
        # Create work items file
        work_items = [
            {
                'year': '1981', 
                'filename': 'file1.tif',
                'blob_path': '1981/file1.tif'
            },
            {
                'year': '1981',
                'filename': 'file2.tif', 
                'blob_path': '1981/file2.tif'
            }
        ]
        
        file_path = os.path.join(temp_dir, "work_items.json")
        with open(file_path, 'w') as f:
            json.dump(work_items, f)
        
        # Set environment
        env_vars = {
            'AZ_BATCH_TASK_ID': 'stac_integration_test',
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir,
            'STORAGE_ACCOUNT_URL': 'http://test.url',
            'COG_CONTAINER_SAS': 'sas1',
            'STAC_CONTAINER_SAS': 'sas2',
            'LOGS_CONTAINER_SAS': 'sas3'
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            # Execute main
            main()
            
            # Verify process_batch_with_progress was called correctly
            mock_process_batch.assert_called_once()
            call_args = mock_process_batch.call_args[0][0]
            
            assert len(call_args) == 2
            assert call_args[0]['filename'] == 'file1.tif'
            assert call_args[1]['blob_path'] == '1981/file2.tif'