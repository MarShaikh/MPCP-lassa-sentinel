"""
Unit tests for stac_conversion module.

This module tests the conversion of COG files to STAC items, including:
- Metadata extraction from filenames
- STAC item creation using rio-stac
- Enhancement of STAC items with custom metadata
- Saving STAC items to Azure Blob Storage
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from src.stac_creation.stac_conversion import (
    extract_metadata_from_filename_chirps,
    create_stac_item_from_cog_chirps,
    enhance_stac_item_with_metadata_chirps,
    process_cog_to_stac,
    save_stac_item_to_blob
)


class TestExtractMetadataFromFilename:
    """Tests for extract_metadata_from_filename_chirps function."""
    
    def test_extract_metadata_standard_filename(self):
        """Test extracting metadata from a standard CHIRPS filename."""
        filename = "nigeria-cog-chirps-v2.0.1981.01.01"
        
        result = extract_metadata_from_filename_chirps(filename)
        
        assert result is not None
        assert result['product_type'] == 'chirps'
        assert result['version'] == 'v2.0'
        assert result['date'].year == 1981
        assert result['date'].month == 1
        assert result['date'].day == 1
        assert result['date'].tzinfo == timezone.utc
        assert 'creation_date' in result
        assert isinstance(result['creation_date'], datetime)
    
    def test_extract_metadata_different_dates(self):
        """Test metadata extraction with various dates."""
        test_cases = [
            ("nigeria-cog-chirps-v2.0.1985.12.31", 1985, 12, 31),
            ("nigeria-cog-chirps-v2.0.2020.06.15", 2020, 6, 15),
            ("nigeria-cog-chirps-v2.0.1981.01.01", 1981, 1, 1),
        ]
        
        for filename, expected_year, expected_month, expected_day in test_cases:
            result = extract_metadata_from_filename_chirps(filename)
            
            assert result['date'].year == expected_year
            assert result['date'].month == expected_month
            assert result['date'].day == expected_day
    
    def test_extract_metadata_invalid_filename(self):
        """Test handling of invalid filename format."""
        invalid_filenames = [
            "invalid-filename.tif",
            "chirps-only-v2.0",
            "nigeria-cog-chirps",  # Missing date
            "random-text",
        ]
        
        for filename in invalid_filenames:
            result = extract_metadata_from_filename_chirps(filename)
            assert result is None
    
    def test_extract_metadata_version_extraction(self):
        """Test that version is correctly extracted."""
        filename = "nigeria-cog-chirps-v2.0.1981.01.01"
        
        result = extract_metadata_from_filename_chirps(filename)
        
        assert result['version'] == 'v2.0'
        assert result['product_type'] == 'chirps'


class TestCreateStacItemFromCog:
    """Tests for create_stac_item_from_cog_chirps function."""
    
    @patch('src.stac_creation.stac_conversion.create_stac_item')
    @patch('src.stac_creation.stac_conversion.MemoryFile')
    def test_create_stac_item_success(self, mock_memory_file, mock_create_stac_item):
        """Test successful STAC item creation from COG."""
        # Mock blob client
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b'fake_tiff_data'
        
        # Mock blob properties
        mock_properties = MagicMock()
        mock_properties.size = 1024000
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        # Mock rasterio dataset inside MemoryFile
        mock_dataset = MagicMock()
        mock_dataset.crs = MagicMock()
        mock_dataset.transform = MagicMock()
        mock_dataset.bounds = (2.316388, 3.837669, 15.126447, 14.153350)
        mock_dataset.nodata = -9999.0
        mock_dataset.dtypes = ['float32']
        mock_dataset.res = (0.05, 0.05)
        
        # Mock MemoryFile context managers
        mock_memfile_instance = MagicMock()
        mock_memfile_instance.__enter__.return_value = mock_memfile_instance
        mock_memfile_instance.__exit__.return_value = None
        mock_memfile_instance.open.return_value.__enter__.return_value = mock_dataset
        mock_memfile_instance.open.return_value.__exit__.return_value = None
        
        mock_memory_file.return_value = mock_memfile_instance
        
        # Mock rio-stac's create_stac_item
        mock_stac_item = MagicMock()
        mock_stac_item.id = 'test-item'
        mock_stac_item.geometry = {'type': 'Polygon', 'coordinates': [[]]}
        mock_stac_item.bbox = [2.316388, 3.837669, 15.126447, 14.153350]
        mock_stac_item.properties = {'datetime': None}
        mock_stac_item.assets = {}
        mock_create_stac_item.return_value = mock_stac_item
        
        # Test parameters
        filename = "nigeria-cog-chirps-v2.0.1981.01.01.tif"
        blob_domain = "https://testaccount.blob.core.windows.net"
        container_name = "processed-cogs"
        year = "1981"
        
        # Execute
        result = create_stac_item_from_cog_chirps(
            mock_blob_client, filename, blob_domain, container_name, year
        )
        
        # Verify blob was downloaded
        mock_blob_client.download_blob.assert_called_once()
        mock_blob_client.get_blob_properties.assert_called_once()
        
        # Verify MemoryFile was used
        mock_memory_file.assert_called_once_with(b'fake_tiff_data')
        
        # Verify create_stac_item was called with correct parameters
        mock_create_stac_item.assert_called_once()
        call_kwargs = mock_create_stac_item.call_args[1]
        
        assert call_kwargs['source'] == mock_dataset
        assert call_kwargs['id'] == 'nigeria-cog-chirps-v2.0.1981.01.01'
        assert call_kwargs['asset_name'] == 'data'
        assert blob_domain in call_kwargs['asset_href']
        assert call_kwargs['with_proj'] is True
        assert call_kwargs['with_raster'] is True
        
        # Verify return
        assert result == mock_stac_item
    
    @patch('src.stac_creation.stac_conversion.create_stac_item')
    @patch('src.stac_creation.stac_conversion.MemoryFile')
    def test_create_stac_item_with_properties(self, mock_memory_file, mock_create_stac_item):
        """Test that STAC item is created with correct properties."""
        # Setup mocks
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b'data'
        
        mock_properties = MagicMock()
        mock_properties.size = 2048000
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        mock_dataset = MagicMock()
        mock_dataset.nodata = -3.4e+38
        mock_dataset.dtypes = ['float64']
        mock_dataset.res = (0.05, 0.05)
        
        mock_memfile = MagicMock()
        mock_memfile.__enter__.return_value = mock_memfile
        mock_memfile.__exit__.return_value = None
        mock_memfile.open.return_value.__enter__.return_value = mock_dataset
        mock_memfile.open.return_value.__exit__.return_value = None
        
        mock_memory_file.return_value = mock_memfile
        mock_create_stac_item.return_value = MagicMock()
        
        # Execute
        filename = "nigeria-cog-chirps-v2.0.2020.06.15.tif"
        create_stac_item_from_cog_chirps(
            mock_blob_client, filename, "https://test.url", "cogs", "2020"
        )
        
        # Verify properties passed to create_stac_item
        call_kwargs = mock_create_stac_item.call_args[1]
        properties = call_kwargs['properties']
        
        assert properties['datetime'] is None
        assert 'raster:bands' in properties
        assert properties['raster:bands'][0]['nodata'] == -3.4e+38
        assert properties['raster:bands'][0]['data_type'] == 'float64'
        assert properties['raster:bands'][0]['spatial_resolution'] == 0.05
        assert properties['file:size'] == 2048000
    
    @patch('src.stac_creation.stac_conversion.MemoryFile')
    def test_create_stac_item_blob_download_failure(self, mock_memory_file):
        """Test handling of blob download failure."""
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.side_effect = Exception("Blob not found")
        
        with pytest.raises(Exception, match="Blob not found"):
            create_stac_item_from_cog_chirps(
                mock_blob_client, "test.tif", "https://test.url", "cogs", "1981"
            )
    
    @patch('src.stac_creation.stac_conversion.create_stac_item') 
    @patch('src.stac_creation.stac_conversion.MemoryFile')        
    def test_create_stac_item_rasterio_failure(self, mock_memory_file, mock_create_stac_item):
        """Test handling of rasterio processing failure."""
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b'data'
        mock_blob_client.get_blob_properties.return_value.size = 1000

        # Make MemoryFile raise an exception
        mock_memory_file.side_effect = Exception("Invalid TIFF data")

        with pytest.raises(Exception, match="Failed to create STAC item"):
            create_stac_item_from_cog_chirps(
                mock_blob_client, "test.tif", "https://test.url", "cogs", "1981"
            )


class TestEnhanceStacItemWithMetadata:
    """Tests for enhance_stac_item_with_metadata_chirps function."""
    
    def test_enhance_stac_item_basic(self):
        """Test enhancing STAC item with metadata."""
        # Create a mock STAC item
        mock_stac_item = MagicMock()
        mock_stac_item.datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_stac_item.common_metadata = MagicMock()
        mock_stac_item.properties = {}
        
        # Create metadata
        metadata = {
            'product_type': 'chirps',
            'version': 'v2.0',
            'date': datetime(1981, 1, 1, tzinfo=timezone.utc),
            'creation_date': datetime(2024, 1, 1, tzinfo=timezone.utc)
        }
        
        # Execute
        result = enhance_stac_item_with_metadata_chirps(mock_stac_item, metadata)
        
        # Verify datetime is set to None
        assert result.datetime is None
        
        # Verify start and end datetime are set
        assert mock_stac_item.common_metadata.start_datetime == metadata['date']
        assert mock_stac_item.common_metadata.end_datetime == metadata['date']
        assert mock_stac_item.common_metadata.created == metadata['creation_date']
        
        # Verify properties are updated
        assert result.properties['product_type'] == 'chirps'
        assert result.properties['version'] == 'v2.0'
    
    def test_enhance_stac_item_preserves_existing_properties(self):
        """Test that enhancement preserves existing properties."""
        mock_stac_item = MagicMock()
        mock_stac_item.datetime = datetime.now(timezone.utc)
        mock_stac_item.common_metadata = MagicMock()
        mock_stac_item.properties = {
            'existing_property': 'should_remain',
            'another_property': 123
        }
        
        metadata = {
            'product_type': 'chirps',
            'version': 'v2.0',
            'date': datetime(1981, 1, 1, tzinfo=timezone.utc),
            'creation_date': datetime(2024, 1, 1, tzinfo=timezone.utc)
        }
        
        result = enhance_stac_item_with_metadata_chirps(mock_stac_item, metadata)
        
        # Verify existing properties remain
        assert result.properties['existing_property'] == 'should_remain'
        assert result.properties['another_property'] == 123
        
        # Verify new properties are added
        assert result.properties['product_type'] == 'chirps'
        assert result.properties['version'] == 'v2.0'
    
    def test_enhance_stac_item_different_dates(self):
        """Test enhancement with various dates."""
        mock_stac_item = MagicMock()
        mock_stac_item.common_metadata = MagicMock()
        mock_stac_item.properties = {}
        
        test_date = datetime(2020, 6, 15, tzinfo=timezone.utc)
        creation_date = datetime(2024, 10, 28, tzinfo=timezone.utc)
        
        metadata = {
            'product_type': 'chirps',
            'version': 'v2.1',
            'date': test_date,
            'creation_date': creation_date
        }
        
        result = enhance_stac_item_with_metadata_chirps(mock_stac_item, metadata)
        
        assert mock_stac_item.common_metadata.start_datetime == test_date
        assert mock_stac_item.common_metadata.end_datetime == test_date
        assert mock_stac_item.common_metadata.created == creation_date


class TestProcessCogToStac:
    """Tests for process_cog_to_stac orchestration function."""
    
    @patch('src.stac_creation.stac_conversion.enhance_stac_item_with_metadata_chirps')
    @patch('src.stac_creation.stac_conversion.create_stac_item_from_cog_chirps')
    @patch('src.stac_creation.stac_conversion.extract_metadata_from_filename_chirps')
    def test_process_cog_to_stac_success(self, mock_extract, mock_create, mock_enhance):
        """Test complete COG to STAC conversion process."""
        # Setup mocks
        mock_blob_client = MagicMock()
        
        # Mock metadata extraction
        mock_metadata = {
            'product_type': 'chirps',
            'version': 'v2.0',
            'date': datetime(1981, 1, 1, tzinfo=timezone.utc),
            'creation_date': datetime.now(timezone.utc)
        }
        mock_extract.return_value = mock_metadata
        
        # Mock STAC item creation
        mock_stac_item = MagicMock()
        mock_stac_item.to_dict.return_value = {
            'id': 'test-item',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'geometry': {},
            'properties': {}
        }
        mock_create.return_value = mock_stac_item
        
        # Mock enhancement
        mock_enhanced_item = MagicMock()
        mock_enhanced_item.to_dict.return_value = {
            'id': 'test-item',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'geometry': {},
            'properties': {
                'product_type': 'chirps',
                'version': 'v2.0'
            }
        }
        mock_enhance.return_value = mock_enhanced_item
        
        # Execute
        filename = "nigeria-cog-chirps-v2.0.1981.01.01.tif"
        result = process_cog_to_stac(
            mock_blob_client, filename, "https://test.url", "cogs", "1981"
        )
        
        # Verify workflow
        mock_extract.assert_called_once_with("nigeria-cog-chirps-v2.0.1981.01.01")
        mock_create.assert_called_once_with(
            mock_blob_client, filename, "https://test.url", "cogs", "1981"
        )
        mock_enhance.assert_called_once_with(mock_stac_item, mock_metadata)
        
        # Verify result is dictionary
        assert isinstance(result, dict)
        assert result['id'] == 'test-item'
        assert result['properties']['product_type'] == 'chirps'
    
    @patch('src.stac_creation.stac_conversion.extract_metadata_from_filename_chirps')
    def test_process_cog_to_stac_metadata_extraction_failure(self, mock_extract):
        """Test handling of metadata extraction failure."""
        mock_blob_client = MagicMock()
        mock_extract.return_value = None  # Simulate extraction failure
        
        filename = "invalid-filename.tif"
        
        with pytest.raises(ValueError, match="Could not extract metadata"):
            process_cog_to_stac(
                mock_blob_client, filename, "https://test.url", "cogs", "1981"
            )
    
    @patch('src.stac_creation.stac_conversion.create_stac_item_from_cog_chirps')
    @patch('src.stac_creation.stac_conversion.extract_metadata_from_filename_chirps')
    def test_process_cog_to_stac_creation_failure(self, mock_extract, mock_create):
        """Test handling of STAC item creation failure."""
        mock_blob_client = MagicMock()
        
        mock_extract.return_value = {
            'product_type': 'chirps',
            'version': 'v2.0',
            'date': datetime(1981, 1, 1, tzinfo=timezone.utc),
            'creation_date': datetime.now(timezone.utc)
        }
        
        mock_create.side_effect = Exception("Failed to create STAC item")
        
        filename = "nigeria-cog-chirps-v2.0.1981.01.01.tif"
        
        with pytest.raises(Exception, match="Failed to create STAC item"):
            process_cog_to_stac(
                mock_blob_client, filename, "https://test.url", "cogs", "1981"
            )


class TestSaveStacItemToBlob:
    """Tests for save_stac_item_to_blob function."""
    
    @patch('src.stac_creation.stac_conversion.json.dumps')
    def test_save_stac_item_success(self, mock_json_dumps):
        """Test successful saving of STAC item to blob."""
        # Mock blob service client
        mock_blob_service = MagicMock()
        mock_blob_client = MagicMock()
        mock_blob_service.get_blob_client.return_value = mock_blob_client
        
        # Mock JSON dumps
        mock_json_dumps.return_value = '{"id": "test-item"}'
        
        # STAC item dict
        stac_item_dict = {
            'id': 'test-item',
            'type': 'Feature',
            'stac_version': '1.0.0'
        }
        
        # Execute
        save_stac_item_to_blob(
            stac_item_dict,
            mock_blob_service,
            "stac-items",
            "1981/test-item.json"
        )
        
        # Verify blob client was created correctly
        mock_blob_service.get_blob_client.assert_called_once_with(
            container="stac-items",
            blob="1981/test-item.json"
        )
        
        # Verify JSON was dumped
        mock_json_dumps.assert_called_once_with(stac_item_dict, indent=2)
        
        # Verify upload was called
        mock_blob_client.upload_blob.assert_called_once()
        upload_args = mock_blob_client.upload_blob.call_args[0]
        assert upload_args[0] == '{"id": "test-item"}'.encode('utf-8')
        assert mock_blob_client.upload_blob.call_args[1]['overwrite'] is True
    
    def test_save_stac_item_different_containers(self):
        """Test saving to different containers."""
        mock_blob_service = MagicMock()
        mock_blob_client = MagicMock()
        mock_blob_service.get_blob_client.return_value = mock_blob_client
        
        stac_item_dict = {'id': 'test'}
        
        containers = ["stac-items", "stac-archive", "test-stac"]
        
        for container in containers:
            save_stac_item_to_blob(
                stac_item_dict,
                mock_blob_service,
                container,
                "test.json"
            )
            
            # Verify correct container was used
            call_kwargs = mock_blob_service.get_blob_client.call_args[1]
            assert call_kwargs['container'] == container
    
    def test_save_stac_item_nested_paths(self):
        """Test saving with nested blob paths."""
        mock_blob_service = MagicMock()
        mock_blob_client = MagicMock()
        mock_blob_service.get_blob_client.return_value = mock_blob_client
        
        stac_item_dict = {'id': 'test'}
        
        paths = [
            "1981/01/item.json",
            "2020/06/15/item.json",
            "year/month/day/hour/item.json"
        ]
        
        for path in paths:
            save_stac_item_to_blob(
                stac_item_dict,
                mock_blob_service,
                "stac-items",
                path
            )
            
            call_kwargs = mock_blob_service.get_blob_client.call_args[1]
            assert call_kwargs['blob'] == path
    
    def test_save_stac_item_upload_failure(self):
        """Test handling of upload failure."""
        mock_blob_service = MagicMock()
        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob.side_effect = Exception("Upload failed")
        mock_blob_service.get_blob_client.return_value = mock_blob_client
        
        stac_item_dict = {'id': 'test'}
        
        with pytest.raises(Exception, match="Upload failed"):
            save_stac_item_to_blob(
                stac_item_dict,
                mock_blob_service,
                "stac-items",
                "test.json"
            )
    
    def test_save_stac_item_complex_dict(self):
        """Test saving a complex STAC item with all properties."""
        mock_blob_service = MagicMock()
        mock_blob_client = MagicMock()
        mock_blob_service.get_blob_client.return_value = mock_blob_client
        
        # Complex STAC item with all typical fields
        stac_item_dict = {
            'id': 'nigeria-cog-chirps-v2.0.1981.01.01',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[[2.3, 3.8], [15.1, 3.8], [15.1, 14.1], [2.3, 14.1], [2.3, 3.8]]]
            },
            'bbox': [2.316388, 3.837669, 15.126447, 14.153350],
            'properties': {
                'datetime': None,
                'start_datetime': '1981-01-01T00:00:00Z',
                'end_datetime': '1981-01-01T00:00:00Z',
                'product_type': 'chirps',
                'version': 'v2.0',
                'created': '2024-10-28T00:00:00Z'
            },
            'assets': {
                'data': {
                    'href': 'https://account.blob.core.windows.net/cogs/1981/file.tif',
                    'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
                    'roles': ['data']
                }
            },
            'links': []
        }
        
        save_stac_item_to_blob(
            stac_item_dict,
            mock_blob_service,
            "stac-items",
            "1981/nigeria-cog-chirps-v2.0.1981.01.01.json"
        )
        
        # Verify upload was called (indicates no serialization errors)
        mock_blob_client.upload_blob.assert_called_once()


# Integration-style tests that test multiple functions together
class TestStacConversionIntegration:
    """Integration tests for the full STAC conversion workflow."""
    
    @patch('src.stac_creation.stac_conversion.enhance_stac_item_with_metadata_chirps')
    @patch('src.stac_creation.stac_conversion.create_stac_item_from_cog_chirps')
    def test_full_workflow_with_real_filename_parsing(self, mock_create, mock_enhance):
        """Test the full workflow with real filename parsing (no mock on extract)."""
        mock_blob_client = MagicMock()
        
        # Use real extract_metadata_from_filename_chirps (not mocked)
        # Mock only the STAC item creation and enhancement
        mock_stac_item = MagicMock()
        mock_stac_item.to_dict.return_value = {
            'id': 'test-item',
            'properties': {}
        }
        mock_create.return_value = mock_stac_item
        
        mock_enhanced = MagicMock()
        mock_enhanced.to_dict.return_value = {
            'id': 'test-item',
            'properties': {
                'product_type': 'chirps',
                'version': 'v2.0'
            }
        }
        mock_enhance.return_value = mock_enhanced
        
        # Execute with a real filename
        filename = "nigeria-cog-chirps-v2.0.1981.01.01.tif"
        result = process_cog_to_stac(
            mock_blob_client, filename, "https://test.url", "cogs", "1981"
        )
        
        # Verify that enhance was called with correctly extracted metadata
        enhance_call_args = mock_enhance.call_args[0]
        metadata = enhance_call_args[1]
        
        assert metadata['product_type'] == 'chirps'
        assert metadata['version'] == 'v2.0'
        assert metadata['date'].year == 1981
        assert metadata['date'].month == 1
        assert metadata['date'].day == 1
    
    @patch('src.stac_creation.stac_conversion.save_stac_item_to_blob')
    @patch('src.stac_creation.stac_conversion.process_cog_to_stac')
    def test_process_and_save_workflow(self, mock_process, mock_save):
        """Test combining process_cog_to_stac with save_stac_item_to_blob."""
        mock_blob_client = MagicMock()
        mock_blob_service = MagicMock()
        
        # Mock process result
        stac_dict = {
            'id': 'nigeria-cog-chirps-v2.0.1981.01.01',
            'type': 'Feature',
            'properties': {'product_type': 'chirps'}
        }
        mock_process.return_value = stac_dict
        
        # Execute workflow
        filename = "nigeria-cog-chirps-v2.0.1981.01.01.tif"
        year = "1981"
        
        # Process
        result = mock_process(
            mock_blob_client, filename, "https://test.url", "cogs", year
        )
        
        # Save
        stac_filename = filename.replace('.tif', '.json')
        blob_path = f"{year}/{stac_filename}"
        
        mock_save(result, mock_blob_service, "stac-items", blob_path)
        
        # Verify workflow
        mock_process.assert_called_once()
        mock_save.assert_called_once_with(
            stac_dict, mock_blob_service, "stac-items", "1981/nigeria-cog-chirps-v2.0.1981.01.01.json"
        )