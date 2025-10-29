"""
Integration tests for batch_task_runner module.

These tests use mocked Azure Batch operations and process_batch_with_progress.
File system operations are tested with real temporary directories.

Setup:
    - Tests use temp directories for file operations
    - Azure Batch operations are mocked
    - process_batch_with_progress is mocked
"""
import pytest
import os
import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from src.batch_processing.batch_task_runner import (
    get_work_items_from_file,
    setup_working_directories,
    main
)


class TestGetWorkItemsFromFile:
    """Tests for get_work_items_from_file function."""
    
    def test_read_valid_work_items(self, mock_batch_task_env, work_items_json_file, sample_task_work_items):
        """Test successfully reading valid work items JSON file."""
        result = get_work_items_from_file()
        
        assert isinstance(result, list)
        assert len(result) == len(sample_task_work_items)
        assert result[0]['year'] == '1981'
        assert 'url' in result[0]
        assert result == sample_task_work_items
    
    def test_read_empty_work_items(self, mock_batch_task_env, empty_work_items_file):
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
    
    def test_malformed_json(self, mock_batch_task_env, malformed_json_file):
        """Test error handling for malformed JSON."""
        with pytest.raises(ValueError) as exc_info:
            get_work_items_from_file()
        
        assert "Failed to read or parse work_items.json" in str(exc_info.value)
    
    def test_reads_from_correct_directory(self, temp_dir, sample_task_work_items):
        """Test that function reads from AZ_BATCH_TASK_WORKING_DIR."""
        # Create file in a subdirectory
        subdir = os.path.join(temp_dir, "batch_subdir")
        os.makedirs(subdir, exist_ok=True)
        
        file_path = os.path.join(subdir, "work_items.json")
        with open(file_path, 'w') as f:
            json.dump(sample_task_work_items, f)
        
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
        setup_working_directories()
        
        # Check that directories exist
        assert os.path.exists("/tmp/processing")
        assert os.path.exists("/tmp/processing/raw-data")
        assert os.path.exists("/tmp/processing/processed-cogs")
        assert os.path.exists("/tmp/batch-logs")
        
        # Check that they are directories
        assert os.path.isdir("/tmp/processing")
        assert os.path.isdir("/tmp/processing/raw-data")
        assert os.path.isdir("/tmp/processing/processed-cogs")
        assert os.path.isdir("/tmp/batch-logs")
    
    def test_handles_existing_directories(self):
        """Test that function doesn't fail if directories already exist."""
        # Create directories first
        setup_working_directories()
        
        # Call again - should not raise error
        setup_working_directories()
        
        # Verify they still exist
        assert os.path.exists("/tmp/processing/raw-data")
        assert os.path.exists("/tmp/processing/processed-cogs")
    
    def test_directory_structure(self):
        """Test the directory structure is correct."""
        setup_working_directories()
        
        base_dir = "/tmp/processing"
        raw_data_dir = os.path.join(base_dir, "raw-data")
        cogs_dir = os.path.join(base_dir, "processed-cogs")
        
        # Verify parent-child relationships
        assert os.path.dirname(raw_data_dir) == base_dir
        assert os.path.dirname(cogs_dir) == base_dir


class TestMainFunction:
    """Integration tests for main function."""
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_main_success(self, mock_process_batch, mock_batch_task_env, 
                         work_items_json_file, sample_task_work_items):
        """Test successful execution of main function."""
        # Run main
        main()
        
        # Verify process_batch_with_progress was called once
        mock_process_batch.assert_called_once()
        
        # Verify it was called with correct work items
        call_args = mock_process_batch.call_args[0][0]
        assert call_args == sample_task_work_items
        assert len(call_args) == 3
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_main_with_empty_work_items(self, mock_process_batch, 
                                       mock_batch_task_env, empty_work_items_file):
        """Test main function with empty work items list."""
        main()
        
        # Should still call process_batch_with_progress with empty list
        mock_process_batch.assert_called_once_with([])
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_main_exits_on_file_not_found(self, mock_process_batch, temp_dir):
        """Test main exits with error code when work items file not found."""
        env_vars = {
            'AZ_BATCH_TASK_ID': 'test_task_001',
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            # Should exit with code 1
            assert exc_info.value.code == 1
        
        # process_batch_with_progress should not be called
        mock_process_batch.assert_not_called()
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_main_exits_on_json_error(self, mock_process_batch, 
                                     mock_batch_task_env, malformed_json_file):
        """Test main exits with error code on JSON parsing error."""
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 1
        mock_process_batch.assert_not_called()
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_main_exits_on_processing_error(self, mock_process_batch, 
                                           mock_batch_task_env, work_items_json_file):
        """Test main exits with error code when processing fails."""
        # Make process_batch_with_progress raise an exception
        mock_process_batch.side_effect = Exception("Processing failed")
        
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 1
        mock_process_batch.assert_called_once()
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    @patch('builtins.print')
    def test_main_prints_task_id(self, mock_print, mock_process_batch, 
                                 mock_batch_task_env, work_items_json_file):
        """Test that main function prints task ID."""
        main()
        
        # Check that task ID was printed
        print_calls = [str(call) for call in mock_print.call_args_list]
        task_id_printed = any('test_task_001' in str(call) for call in print_calls)
        assert task_id_printed
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    @patch('builtins.print')
    def test_main_prints_work_items_count(self, mock_print, mock_process_batch,
                                         mock_batch_task_env, work_items_json_file):
        """Test that main function prints number of work items."""
        main()
        
        # Check that work items count was printed
        print_calls = [str(call) for call in mock_print.call_args_list]
        count_printed = any('3' in str(call) and 'files' in str(call).lower() 
                          for call in print_calls)
        assert count_printed
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_main_uses_default_task_id(self, mock_process_batch, temp_dir, 
                                      work_items_json_file):
        """Test main uses 'unknown_task' when AZ_BATCH_TASK_ID not set."""
        env_vars = {
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('builtins.print') as mock_print:
                main()
                
                # Check that 'unknown_task' was used
                print_calls = [str(call) for call in mock_print.call_args_list]
                unknown_task_used = any('unknown_task' in str(call) 
                                       for call in print_calls)
                assert unknown_task_used


class TestEndToEndIntegration:
    """End-to-end integration tests for the task runner."""
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_full_task_execution_flow(self, mock_process_batch, temp_dir):
        """Test complete flow from file read to processing."""
        # Create work items file
        work_items = [
            {'year': '1981', 'url': 'https://test.url/1981.01.01.tif.gz'},
            {'year': '1981', 'url': 'https://test.url/1981.01.02.tif.gz'}
        ]
        
        file_path = os.path.join(temp_dir, "work_items.json")
        with open(file_path, 'w') as f:
            json.dump(work_items, f)
        
        # Set environment
        env_vars = {
            'AZ_BATCH_TASK_ID': 'integration_test_task',
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            # Execute main
            main()
            
            # Verify process_batch_with_progress was called correctly
            mock_process_batch.assert_called_once()
            call_args = mock_process_batch.call_args[0][0]
            
            assert len(call_args) == 2
            assert call_args[0]['year'] == '1981'
            assert call_args[1]['url'] == 'https://test.url/1981.01.02.tif.gz'
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_handles_large_work_items_list(self, mock_process_batch, temp_dir):
        """Test handling large number of work items."""
        # Create 100 work items
        work_items = [
            {'year': '1981', 'url': f'https://test.url/1981.01.{i:02d}.tif.gz'}
            for i in range(1, 101)
        ]
        
        file_path = os.path.join(temp_dir, "work_items.json")
        with open(file_path, 'w') as f:
            json.dump(work_items, f)
        
        env_vars = {
            'AZ_BATCH_TASK_ID': 'large_batch_test',
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            main()
            
            call_args = mock_process_batch.call_args[0][0]
            assert len(call_args) == 100
            assert call_args[0]['url'].endswith('01.tif.gz')
            assert call_args[99]['url'].endswith('00.tif.gz')
    
    @patch('src.batch_processing.batch_task_runner.process_batch_with_progress')
    def test_work_items_with_different_years(self, mock_process_batch, temp_dir):
        """Test processing work items from multiple years."""
        work_items = [
            {'year': '1981', 'url': 'https://test.url/1981.01.01.tif.gz'},
            {'year': '1982', 'url': 'https://test.url/1982.01.01.tif.gz'},
            {'year': '1983', 'url': 'https://test.url/1983.01.01.tif.gz'}
        ]
        
        file_path = os.path.join(temp_dir, "work_items.json")
        with open(file_path, 'w') as f:
            json.dump(work_items, f)
        
        env_vars = {
            'AZ_BATCH_TASK_ID': 'multi_year_test',
            'AZ_BATCH_TASK_WORKING_DIR': temp_dir
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            main()
            
            call_args = mock_process_batch.call_args[0][0]
            years = [item['year'] for item in call_args]
            assert years == ['1981', '1982', '1983']