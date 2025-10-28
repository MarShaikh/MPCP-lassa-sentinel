"""
Unit tests for processing module.
"""
import pytest
import os
import json
import gzip
import tempfile
from unittest.mock import Mock, patch, MagicMock, mock_open

from src.batch_processing.processing import (
    create_chunks,
    unzip_file,
    clip_to_cog,
    upload_blob_to_azure,
    cleanup_local_files,
    update_progress_file,
    process_batch_with_progress
)


class TestCreateChunks:
    """Tests for create_chunks function."""
    
    def test_create_chunks_default_size(self, sample_work_items):
        """Test chunking with default chunk size."""
        # Create 1000 work items
        work_items = sample_work_items * 100  # 1000 items
        
        chunks = create_chunks(work_items, chunk_size=550)
        
        assert len(chunks) == 2  # Should create 2 chunks
        assert len(chunks[0]) == 550
        assert len(chunks[1]) == 450
        assert chunks[0][0] == sample_work_items[0]
    
    def test_create_chunks_custom_size(self):
        """Test chunking with custom size."""
        work_items = [{'id': i} for i in range(25)]
        
        chunks = create_chunks(work_items, chunk_size=10)
        
        assert len(chunks) == 3
        assert len(chunks[0]) == 10
        assert len(chunks[1]) == 10
        assert len(chunks[2]) == 5
    
    def test_create_chunks_empty_list(self):
        """Test with empty work items list."""
        chunks = create_chunks([], chunk_size=100)
        assert chunks == []
    
    def test_create_chunks_smaller_than_chunk_size(self):
        """Test when total items less than chunk size."""
        work_items = [{'id': i} for i in range(50)]
        
        chunks = create_chunks(work_items, chunk_size=100)
        
        assert len(chunks) == 1
        assert len(chunks[0]) == 50


class TestUnzipFile:
    """Tests for unzip_file function."""
    
    @patch('src.batch_processing.processing.requests.get')
    def test_unzip_gzipped_file(self, mock_get, sample_chirps_compressed):
        """Test decompressing a gzipped file."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = sample_chirps_compressed
        mock_get.return_value = mock_response
        
        result = unzip_file("http://test.url/file.tif.gz")
        
        assert result == gzip.decompress(sample_chirps_compressed)
        mock_get.assert_called_once_with("http://test.url/file.tif.gz")
    
    @patch('src.batch_processing.processing.requests.get')
    def test_unzip_non_gzipped_file(self, mock_get, sample_chirps_data):
        """Test handling non-gzipped file."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = sample_chirps_data
        mock_get.return_value = mock_response
        
        result = unzip_file("http://test.url/file.tif")
        
        assert result == sample_chirps_data
    
    @patch('src.batch_processing.processing.requests.get')
    def test_unzip_file_http_error(self, mock_get):
        """Test handling HTTP error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Should raise Exception with status code message
        with pytest.raises(Exception, match="Failed to download file.*Status code: 404"):
            unzip_file("http://test.url/missing.tif.gz")


class TestClipToCog:
    """Tests for clip_to_cog function."""
    
    @patch('src.batch_processing.processing.CRS')
    @patch('src.batch_processing.processing.transform_bounds')
    @patch('src.batch_processing.processing.from_bounds')
    @patch('src.batch_processing.processing.rasterio.open')
    def test_clip_to_cog_success(self, mock_rasterio_open, mock_from_bounds, 
                                 mock_transform_bounds, mock_CRS, temp_dir):
        """Test successful COG creation."""
        # Setup mock rasterio dataset for reading
        mock_src = MagicMock()
        mock_src.crs = MagicMock()
        mock_src.res = (0.05, 0.05)
        mock_src.transform = MagicMock()
        mock_src.profile = {
            'driver': 'GTiff',
            'dtype': 'float32',
            'nodata': -9999.0,
            'width': 1000,
            'height': 1000,
            'count': 1
        }
        mock_src.dtypes = ['float32']
        mock_src.nodata = -9999.0
        
        # Mock window
        mock_window = MagicMock()
        mock_window.height = 100
        mock_window.width = 100
        mock_src.read.return_value = MagicMock()  # Mock data array
        mock_src.window_transform.return_value = MagicMock()
        
        # Mock CRS operations
        mock_crs_obj = MagicMock()
        mock_CRS.from_string.return_value = mock_crs_obj
        
        # Mock transform_bounds to return 4 bbox coordinates
        mock_transform_bounds.return_value = (2.316388, 3.837669, 15.126447, 14.153350)
        
        # Mock from_bounds to return our mock window
        mock_from_bounds.return_value = mock_window
        
        # Setup mock destination for writing
        mock_dst = MagicMock()
        
        # Configure context managers for both read and write
        mock_src_context = MagicMock()
        mock_src_context.__enter__.return_value = mock_src
        mock_src_context.__exit__.return_value = None
        
        mock_dst_context = MagicMock()
        mock_dst_context.__enter__.return_value = mock_dst
        mock_dst_context.__exit__.return_value = None
        
        # Make open return different contexts for read vs write
        mock_rasterio_open.side_effect = [mock_src_context, mock_dst_context]
        
        input_path = os.path.join(temp_dir, "input.tif")
        output_path = os.path.join(temp_dir, "output.tif")
        bbox = [2.316388, 3.837669, 15.126447, 14.153350]
        
        # Should not raise an exception
        clip_to_cog(input_path, output_path, bbox, "EPSG:4326")
        
        assert mock_rasterio_open.call_count == 2  # Once for read, once for write
        mock_dst.write.assert_called_once()
        mock_dst.build_overviews.assert_called_once()
    
    @patch('src.batch_processing.processing.rasterio.open')
    def test_clip_to_cog_with_exception(self, mock_rasterio_open):
        """Test exception handling in clip_to_cog."""
        mock_rasterio_open.side_effect = Exception("Rasterio error")
        
        # Current implementation prints but doesn't raise
        # This documents current behavior
        clip_to_cog("input.tif", "output.tif", [0, 0, 1, 1], "EPSG:4326")
        # Should complete without raising


class TestUploadBlobToAzure:
    """Tests for upload_blob_to_azure function."""
    
    @patch('src.batch_processing.processing.BlobServiceClient')
    def test_upload_blob_success(self, mock_blob_service, mock_azure_storage_env):
        """Test successful blob upload."""
        # Setup mocks
        mock_blob_client = MagicMock()
        mock_service_instance = MagicMock()
        mock_service_instance.get_blob_client.return_value = mock_blob_client
        mock_blob_service.return_value = mock_service_instance
        
        # Create a test file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content")
            temp_file = f.name
        
        try:
            upload_blob_to_azure("processed-cogs", temp_file, "test.tif")
            
            # Verify blob client was created correctly
            mock_service_instance.get_blob_client.assert_called_once_with(
                container="processed-cogs",
                blob="test.tif"
            )
            
            # Verify upload was called
            mock_blob_client.upload_blob.assert_called_once()
            
        finally:
            os.unlink(temp_file)
    
    @patch('src.batch_processing.processing.BlobServiceClient')
    def test_upload_blob_different_containers(self, mock_blob_service, mock_azure_storage_env):
        """Test upload with different container SAS tokens."""
        mock_service_instance = MagicMock()
        mock_blob_service.return_value = mock_service_instance
        
        containers = ["processed-cogs", "raw-data", "batch-logs"]
        
        for container in containers:
            with tempfile.NamedTemporaryFile() as f:
                upload_blob_to_azure(container, f.name, "test.file")
            
            # Check that correct SAS token was used
            if container == "processed-cogs":
                expected_sas = "mock_cog_sas_token"
            elif container == "raw-data":
                expected_sas = "mock_raw_sas_token"
            else:
                expected_sas = "mock_logs_sas_token"
            
            expected_url = f"{mock_azure_storage_env['STORAGE_ACCOUNT_URL']}?{expected_sas}"
            mock_blob_service.assert_called_with(account_url=expected_url)
    
    def test_upload_blob_unknown_container(self, mock_azure_storage_env):
        """Test upload with unknown container raises error."""
        with pytest.raises(ValueError, match="Unknown container"):
            upload_blob_to_azure("unknown-container", "file.txt", "blob.txt")


class TestCleanupLocalFiles:
    """Tests for cleanup_local_files function."""
    
    def test_cleanup_single_file(self, temp_dir):
        """Test cleaning up a single file."""
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        assert os.path.exists(test_file)
        cleanup_local_files(test_file)
        assert not os.path.exists(test_file)
    
    def test_cleanup_multiple_files(self, temp_dir):
        """Test cleaning up multiple files as tuples."""
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "file2.txt")
        
        for f in [file1, file2]:
            with open(f, 'w') as fh:
                fh.write("test")
        
        cleanup_local_files([(file1, file2)])
        
        assert not os.path.exists(file1)
        assert not os.path.exists(file2)
    
    def test_cleanup_nonexistent_file(self):
        """Test cleanup handles missing files gracefully."""
        # Should not raise an exception
        cleanup_local_files("/nonexistent/file.txt")
        cleanup_local_files([("/nonexistent/file1.txt", "/nonexistent/file2.txt")])


class TestUpdateProgressFile:
    """Tests for update_progress_file function."""
    
    @patch('src.batch_processing.processing.upload_blob_to_azure')
    @patch('src.batch_processing.processing.cleanup_local_files')
    @patch('src.batch_processing.processing.datetime')
    def test_update_progress_file(self, mock_datetime, mock_cleanup, mock_upload):
        """Test progress file creation and upload."""
        mock_datetime.now.return_value.isoformat.return_value = "2024-01-01T12:00:00"
        
        failed_files = [{"item": {"url": "test.tif"}, "Error": "Test error"}]
        
        with patch('builtins.open', mock_open()) as mock_file:
            update_progress_file("task001", 10, failed_files)
        
        # Verify JSON structure written
        written_content = ''.join(
            call.args[0] for call in mock_file().write.call_args_list
        )
        progress_data = json.loads(written_content)
        
        assert progress_data["iso_timestamp"] == "2024-01-01T12:00:00"
        assert progress_data["batch_number"] == "task001"
        assert progress_data["completed"] == 10
        assert progress_data["failed_files"] == failed_files
        
        # Verify upload was called
        mock_upload.assert_called_once()
        assert mock_upload.call_args[1]["container_name"] == "batch-logs"
        assert mock_upload.call_args[1]["file_name"] == "task001.json"
        
        # Verify cleanup was called
        mock_cleanup.assert_called_once()


class TestProcessBatchWithProgress:
    """Tests for process_batch_with_progress orchestration."""
    
    @patch('src.batch_processing.processing.upload_blob_to_azure')
    @patch('src.batch_processing.processing.decompress_convert_to_cog')
    @patch('src.batch_processing.processing.update_progress_file')
    @patch('src.batch_processing.processing.cleanup_local_files')
    def test_process_batch_success(
        self, mock_cleanup, mock_update, mock_decompress, mock_upload, 
        sample_work_items
    ):
        """Test successful batch processing."""
        # Setup decompress mock to return file paths
        mock_decompress.return_value = (
            "/tmp/cog.tif", "cog.tif", "/tmp/raw.tif", "raw.tif"
        )
        
        # Process a small batch
        work_items = sample_work_items[:3]
        process_batch_with_progress(work_items)
        
        # Verify decompress was called for each item
        assert mock_decompress.call_count == 3
        
        # Verify uploads (2 per item: COG and raw)
        assert mock_upload.call_count == 6
        
        # Verify progress updates
        assert mock_update.called
        
        # Verify cleanup
        assert mock_cleanup.called
    
    @patch('src.batch_processing.processing.upload_blob_to_azure')
    @patch('src.batch_processing.processing.decompress_convert_to_cog')
    @patch('src.batch_processing.processing.update_progress_file')
    def test_process_batch_with_failures(
        self, mock_update, mock_decompress, mock_upload,
        sample_work_items, mock_azure_storage_env  # Add this fixture
    ):
        """Test batch processing with some failures."""
        # Make first call fail, second succeed
        mock_decompress.side_effect = [
            Exception("Processing failed"),
            ("/tmp/cog.tif", "cog.tif", "/tmp/raw.tif", "raw.tif")
        ]
        
        work_items = sample_work_items[:2]
        process_batch_with_progress(work_items)
        
        # Verify progress update includes failure
        progress_call = mock_update.call_args
        assert progress_call[0][0] == "test_task_001"  # task_id from fixture
        assert progress_call[0][1] == 1  # completed count
        assert len(progress_call[0][2]) == 1  # failed count
        assert "Processing failed" in progress_call[0][2][0]["Error"]