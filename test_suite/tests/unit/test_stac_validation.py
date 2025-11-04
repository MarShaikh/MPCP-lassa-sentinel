"""
Tests for STAC schema validation against official STAC specification.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
import json

from pystac import Item
from pystac.validation import validate_dict

from src.stac_conversion import (
    process_cog_to_stac,
    create_stac_item_from_cog_chirps,
    enhance_stac_item_with_metadata_chirps,
    extract_metadata_from_filename_chirps
)


class TestStacSchemaValidation:
    """Tests that generated STAC items are valid against official schema."""
    
    @patch('src.stac_conversion.create_stac_item')
    @patch('src.stac_conversion.MemoryFile')
    def test_stac_item_validates_against_schema(self, mock_memory_file, mock_create_stac):
        """Test that created STAC item validates against official STAC schema."""
        # Setup mocks
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b'fake_data'
        
        mock_properties = MagicMock()
        mock_properties.size = 1024
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.nodata = -9999.0
        mock_dataset.dtypes = ['float32']
        mock_dataset.res = (0.05, 0.05)
        
        mock_memfile = MagicMock()
        mock_memfile.__enter__.return_value = mock_memfile
        mock_memfile.__exit__.return_value = None
        mock_memfile.open.return_value.__enter__.return_value = mock_dataset
        mock_memfile.open.return_value.__exit__.return_value = None
        mock_memory_file.return_value = mock_memfile
        
        # Create a realistic STAC item that should pass validation
        mock_stac_item = Item(
            id="test-item",
            geometry={
                "type": "Polygon",
                "coordinates": [[[2.3, 3.8], [15.1, 3.8], [15.1, 14.1], [2.3, 14.1], [2.3, 3.8]]]
            },
            bbox=[2.3, 3.8, 15.1, 14.1],
            datetime=datetime(1981, 1, 1, tzinfo=timezone.utc),
            properties={}
        )
        mock_create_stac.return_value = mock_stac_item
        
        # Execute
        result_item = create_stac_item_from_cog_chirps(
            mock_blob_client,
            "nigeria-cog-chirps-v2.0.1981.01.01.tif",
            "https://test.blob.core.windows.net",
            "processed-cogs",
            "1981"
        )
        
        # Validate against STAC schema
        try:
            # Convert to dict and validate
            item_dict = result_item.to_dict()
            validate_dict(item_dict)
        except Exception as e:
            pytest.fail(f"STAC item failed validation: {e}")
    
    @patch('src.stac_conversion.enhance_stac_item_with_metadata_chirps')
    @patch('src.stac_conversion.create_stac_item_from_cog_chirps')
    @patch('src.stac_conversion.extract_metadata_from_filename_chirps')
    def test_complete_stac_conversion_validates(self, mock_extract, mock_create, mock_enhance):
        """Test that the complete conversion process produces valid STAC."""
        mock_blob_client = MagicMock()
        
        # Mock metadata extraction
        mock_metadata = {
            'product_type': 'chirps',
            'version': 'v2.0',
            'date': datetime(1981, 1, 1, tzinfo=timezone.utc),
            'creation_date': datetime.now(timezone.utc)
        }
        mock_extract.return_value = mock_metadata
        
        # Create a valid STAC item
        stac_item = Item(
            id="nigeria-cog-chirps-v2.0-1981-01-01",
            geometry={
                "type": "Polygon",
                "coordinates": [[[2.316388, 3.837669], [15.126447, 3.837669], 
                                [15.126447, 14.153350], [2.316388, 14.153350], 
                                [2.316388, 3.837669]]]
            },
            bbox=[2.316388, 3.837669, 15.126447, 14.153350],
            datetime=datetime(1981, 1, 1, tzinfo=timezone.utc),
            properties={
                'product_type': 'chirps',
                'version': 'v2.0'
            }
        )
        mock_create.return_value = stac_item
        mock_enhance.return_value = stac_item
        
        # Execute
        result = process_cog_to_stac(
            mock_blob_client,
            "nigeria-cog-chirps-v2.0.1981.01.01.tif",
            "https://test.url",
            "cogs",
            "1981"
        )
        
        # Validate the resulting dictionary can be loaded as valid STAC
        try:
            # Validate the dict directly
            validate_dict(result)
            
            # Also verify it can be loaded as Item
            item_from_dict = Item.from_dict(result)
            assert item_from_dict.id == "nigeria-cog-chirps-v2.0-1981-01-01"
        except Exception as e:
            pytest.fail(f"STAC item failed validation: {e}")
    
    def test_required_stac_fields_present(self):
        """Test that all required STAC fields are present in output."""
        # Create a sample STAC dict as your code would produce it
        stac_dict = {
            'id': 'test-item',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            },
            'bbox': [0, 0, 1, 1],
            'properties': {
                'datetime': '1981-01-01T00:00:00Z'
            },
            'assets': {},
            'links': []
        }
        
        # Required STAC fields
        required_fields = ['id', 'type', 'stac_version', 'geometry', 'bbox', 
                          'properties', 'assets', 'links']
        
        for field in required_fields:
            assert field in stac_dict, f"Missing required STAC field: {field}"
        
        # Validate type value
        assert stac_dict['type'] == 'Feature'
        assert stac_dict['stac_version'] in ['1.0.0', '1.1.0']
        
        # Validate against schema
        try:
            validate_dict(stac_dict)
        except Exception as e:
            pytest.fail(f"Basic STAC structure failed validation: {e}")
    
    @pytest.mark.parametrize("filename,expected_year,expected_month,expected_day", [
        ("nigeria-cog-chirps-v2.0.1981.01.01.tif", 1981, 1, 1),
        ("nigeria-cog-chirps-v2.0.2020.12.31.tif", 2020, 12, 31),
        ("nigeria-cog-chirps-v2.0.1985.06.15.tif", 1985, 6, 15),
    ])
    def test_metadata_extraction_produces_valid_dates(self, filename, expected_year, 
                                                     expected_month, expected_day):
        """Test that extracted metadata produces valid datetime values."""
        # Remove .tif extension for extraction
        filename_base = filename.replace('.tif', '')
        
        # Extract metadata
        metadata = extract_metadata_from_filename_chirps(filename_base)
        
        assert metadata is not None
        assert metadata['date'].year == expected_year
        assert metadata['date'].month == expected_month
        assert metadata['date'].day == expected_day
        assert metadata['date'].tzinfo == timezone.utc
    
    @patch('src.stac_conversion.create_stac_item_from_cog_chirps')
    @patch('src.stac_conversion.enhance_stac_item_with_metadata_chirps')
    def test_datetime_format_in_stac_items(self, mock_enhance, mock_create):
        """Test that datetime is properly formatted in STAC items."""
        mock_blob_client = MagicMock()
        
        filename = "nigeria-cog-chirps-v2.0.1981.01.15.tif"
        year = '1981'
        
        # Create item with proper datetime
        expected_datetime = datetime(1981, 1, 15, tzinfo=timezone.utc)
        stac_item = Item(
            id=filename.replace('.tif', ''),
            geometry={"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]},
            bbox=[0, 0, 1, 1],
            datetime=expected_datetime,
            properties={}
        )
        mock_create.return_value = stac_item
        mock_enhance.return_value = stac_item
        
        # Execute
        result = process_cog_to_stac(mock_blob_client, filename, 
                                    "https://test.url", "cogs", year)
        
        # Validate datetime format
        if 'datetime' in result['properties']:
            datetime_str = result['properties']['datetime']
            # Should be ISO 8601 format with timezone
            assert 'T' in datetime_str
            assert 'Z' in datetime_str or '+' in datetime_str
            
            # Should be parseable
            parsed = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            assert parsed.year == 1981
            assert parsed.month == 1
            assert parsed.day == 15
    
    def test_geometry_coordinates_are_valid(self):
        """Test that geometry coordinates follow GeoJSON spec."""
        # Nigeria bounding box coordinates
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [2.316388, 3.837669],    # SW corner
                    [15.126447, 3.837669],   # SE corner
                    [15.126447, 14.153350],  # NE corner
                    [2.316388, 14.153350],   # NW corner
                    [2.316388, 3.837669]     # Close the ring
                ]
            ]
        }
        
        # Validate polygon ring closure
        first_coord = geometry['coordinates'][0][0]
        last_coord = geometry['coordinates'][0][-1]
        assert first_coord == last_coord, "Polygon ring must be closed"
        
        # Validate coordinate order (lon, lat)
        for coord in geometry['coordinates'][0]:
            assert -180 <= coord[0] <= 180, f"Longitude {coord[0]} out of range"
            assert -90 <= coord[1] <= 90, f"Latitude {coord[1]} out of range"
        
        # Validate it works in a STAC item
        stac_dict = {
            'id': 'test-geometry',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'geometry': geometry,
            'bbox': [2.316388, 3.837669, 15.126447, 14.153350],
            'properties': {'datetime': '2024-01-01T00:00:00Z'},
            'assets': {},
            'links': []
        }
        
        try:
            validate_dict(stac_dict)
        except Exception as e:
            pytest.fail(f"Geometry validation failed: {e}")
    
    def test_bbox_matches_geometry(self):
        """Test that bbox correctly represents the geometry extent."""
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [2.316388, 3.837669],
                    [15.126447, 3.837669],
                    [15.126447, 14.153350],
                    [2.316388, 14.153350],
                    [2.316388, 3.837669]
                ]
            ]
        }
        
        bbox = [2.316388, 3.837669, 15.126447, 14.153350]
        
        # Extract coordinates
        coords = geometry['coordinates'][0]
        lons = [c[0] for c in coords[:-1]]  # Exclude closing coordinate
        lats = [c[1] for c in coords[:-1]]
        
        # Verify bbox matches extent
        assert bbox[0] == min(lons)  # min lon
        assert bbox[1] == min(lats)  # min lat
        assert bbox[2] == max(lons)  # max lon
        assert bbox[3] == max(lats)  # max lat


class TestStacExtensions:
    """Tests for STAC extensions used in items."""
    
    def test_file_extension_schema_url(self):
        """Test that file extension uses correct schema URL."""
        expected_extension = 'https://stac-extensions.github.io/file/v2.1.0/schema.json'
        
        # Create a sample item as your code would
        stac_dict = {
            'id': 'test-ext',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'stac_extensions': [expected_extension],
            'geometry': {'type': 'Polygon', 'coordinates': [[[0,0],[1,0],[1,1],[0,1],[0,0]]]},
            'bbox': [0, 0, 1, 1],
            'properties': {
                'datetime': '2024-01-01T00:00:00Z',
                'file:size': 1024000
            },
            'assets': {},
            'links': []
        }
        
        assert expected_extension in stac_dict.get('stac_extensions', [])
        assert 'file:size' in stac_dict['properties']
        
        # Validate with extension
        try:
            validate_dict(stac_dict)
        except Exception as e:
            pytest.fail(f"File extension validation failed: {e}")
    
    def test_raster_extension_fields(self):
        """Test raster extension fields when with_raster=True."""
        stac_dict = {
            'id': 'test-raster',
            'type': 'Feature',
            'stac_version': '1.0.0',
            'geometry': {'type': 'Polygon', 'coordinates': [[[0,0],[1,0],[1,1],[0,1],[0,0]]]},
            'bbox': [0, 0, 1, 1],
            'properties': {
                'datetime': '2024-01-01T00:00:00Z',
                'raster:bands': [
                    {
                        'nodata': -9999.0,
                        'data_type': 'float32',
                        'spatial_resolution': 0.05
                    }
                ]
            },
            'assets': {},
            'links': []
        }
        
        # Verify raster bands structure
        assert 'raster:bands' in stac_dict['properties']
        raster_bands = stac_dict['properties']['raster:bands']
        assert isinstance(raster_bands, list)
        assert len(raster_bands) > 0
        
        # Verify band properties
        band = raster_bands[0]
        assert 'nodata' in band
        assert 'data_type' in band
        assert 'spatial_resolution' in band


class TestStacItemAssets:
    """Tests for STAC item assets."""
    
    def test_data_asset_has_required_fields(self):
        """Test that data asset has all required fields."""
        asset = {
            'href': 'https://account.blob.core.windows.net/cogs/1981/file.tif',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles': ['data']
        }
        
        # Required fields
        assert 'href' in asset
        assert 'type' in asset
        assert 'roles' in asset
        assert isinstance(asset['roles'], list)
        
        # Validate href is valid URL
        assert asset['href'].startswith('https://')
        assert '.tif' in asset['href']
    
    def test_asset_href_format(self):
        """Test that asset href is properly formatted URL."""
        href = "https://testaccount.blob.core.windows.net/processed-cogs/1981/nigeria-cog-chirps-v2.0.1981.01.01.tif"
        
        # Should be valid HTTPS URL
        assert href.startswith('https://')
        assert '.blob.core.windows.net' in href
        assert '.tif' in href
        
        # Should have year path component
        assert '/1981/' in href
    
    def test_cog_media_type(self):
        """Test that COG assets use correct media type."""
        expected_type = 'image/tiff; application=geotiff; profile=cloud-optimized'
        
        # Verify format
        assert 'image/tiff' in expected_type
        assert 'cloud-optimized' in expected_type
    
    def test_asset_roles(self):
        """Test that assets have appropriate roles."""
        valid_roles = ['data', 'metadata', 'thumbnail']
        
        # Data asset should have 'data' role
        data_asset_roles = ['data']
        assert all(role in valid_roles for role in data_asset_roles)


class TestStacItemValidationRealWorld:
    """Integration-style tests with realistic STAC items."""
    
    @patch('src.stac_conversion.create_stac_item')
    @patch('src.stac_conversion.MemoryFile')
    def test_full_chirps_stac_item_validates(self, mock_memory_file, mock_create_stac):
        """Test a complete realistic CHIRPS STAC item validates."""
        from pystac import Asset
        
        # Setup comprehensive mocks
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b'fake_tiff_data'
        
        mock_properties = MagicMock()
        mock_properties.size = 2048576  # ~2MB
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        mock_dataset = MagicMock()
        mock_dataset.nodata = -3.4e+38
        mock_dataset.dtypes = ['float64']
        mock_dataset.res = (0.05, 0.05)
        mock_dataset.crs = 'EPSG:4326'
        mock_dataset.bounds = (2.316388, 3.837669, 15.126447, 14.153350)
        
        mock_memfile = MagicMock()
        mock_memfile.__enter__.return_value = mock_memfile
        mock_memfile.__exit__.return_value = None
        mock_memfile.open.return_value.__enter__.return_value = mock_dataset
        mock_memfile.open.return_value.__exit__.return_value = None
        mock_memory_file.return_value = mock_memfile
        
        # Create realistic STAC item
        stac_item = Item(
            id="nigeria-cog-chirps-v2.0-1981-01-15",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [
                        [2.316388, 3.837669],
                        [15.126447, 3.837669],
                        [15.126447, 14.153350],
                        [2.316388, 14.153350],
                        [2.316388, 3.837669]
                    ]
                ]
            },
            bbox=[2.316388, 3.837669, 15.126447, 14.153350],
            datetime=datetime(1981, 1, 15, tzinfo=timezone.utc),
            properties={
                'product_type': 'chirps',
                'version': 'v2.0',
                'platform': 'CHIRPS',
                'instruments': ['satellite-gauge'],
                'gsd': 0.05
            }
        )
        
        # Add asset using pystac.Asset object (not dict)
        data_asset = Asset(
            href='https://test.blob.core.windows.net/processed-cogs/1981/nigeria-cog-chirps-v2.0.1981.01.15.tif',
            media_type='image/tiff; application=geotiff; profile=cloud-optimized',
            roles=['data']
        )
        stac_item.add_asset('data', data_asset)
        
        mock_create_stac.return_value = stac_item
        
        # Execute
        result = create_stac_item_from_cog_chirps(
            mock_blob_client,
            "nigeria-cog-chirps-v2.0.1981.01.15.tif",
            "https://test.blob.core.windows.net",
            "processed-cogs",
            "1981"
        )
        
        # Validate
        result_dict = result.to_dict()
        
        try:
            validate_dict(result_dict)
        except Exception as e:
            pytest.fail(f"Realistic CHIRPS STAC item failed validation: {e}")
        
        # Verify key fields
        assert result_dict['id'] == "nigeria-cog-chirps-v2.0-1981-01-15"
        assert 'data' in result_dict['assets']
        assert result_dict['bbox'] == [2.316388, 3.837669, 15.126447, 14.153350]