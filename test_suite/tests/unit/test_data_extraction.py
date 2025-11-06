"""
Unit tests for data_extraction module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from src.cog_creation.data_extraction import (
    get_table_from_link,
    find_data_storage,
    find_tiff_url
)


class TestGetTableFromLink:
    """Tests for get_table_from_link function."""
    
    @patch('src.cog_creation.data_extraction.requests.get')
    def test_get_table_from_link_success(self, mock_get, mock_html_response):
        """Test successful extraction of table data."""
        # Setup mock response
        mock_response = Mock()
        mock_response.content = mock_html_response
        mock_get.return_value = mock_response
        
        # Test link extraction
        result = get_table_from_link("http://test.url", "link")
        
        assert len(result) == 4  # Should find 4 td elements with class="link"
        assert all(td.name == 'td' for td in result)
        assert result[0].find('a')['href'] == '1981/'
        
    @patch('src.cog_creation.data_extraction.requests.get')
    def test_get_table_from_link_with_size_class(self, mock_get, mock_html_response):
        """Test extraction with size class."""
        mock_response = Mock()
        mock_response.content = mock_html_response
        mock_get.return_value = mock_response
        
        result = get_table_from_link("http://test.url", "size")
        
        assert len(result) == 4  # Should find 4 size elements
        assert '125.5 KiB' in result[0].text
        
    @patch('src.cog_creation.data_extraction.requests.get')
    def test_get_table_from_link_no_table(self, mock_get):
        """Test when no table with id='list' exists."""
        mock_response = Mock()
        mock_response.content = "<html><body><p>No table here</p></body></html>"
        mock_get.return_value = mock_response
        
        with pytest.raises(AttributeError):
            get_table_from_link("http://test.url", "link")
    
    @patch('src.cog_creation.data_extraction.requests.get')
    def test_get_table_from_link_empty_table(self, mock_get):
        """Test when table exists but has no matching elements."""
        mock_response = Mock()
        mock_response.content = '<html><body><table id="list"></table></body></html>'
        mock_get.return_value = mock_response
        
        result = get_table_from_link("http://test.url", "link")
        assert result == []


class TestFindDataStorage:
    """Tests for find_data_storage function."""
    
    @patch('src.cog_creation.data_extraction.get_table_from_link')
    def test_find_data_storage_calculation(self, mock_get_table):
        """Test storage calculation from size data."""
        # Create mock BeautifulSoup objects
        mock_sizes = []
        for size_text in ['125.5 KiB', '130.2 KiB', '1024.0 KiB']:
            tag = MagicMock()
            tag.text = size_text
            mock_sizes.append(tag)
        
        mock_get_table.return_value = mock_sizes
        
        # The function uses pattern parameter but ignores it in current implementation
        result = find_data_storage("http://test.url", r"\d+\.\d+")
        
        # (125.5 + 130.2 + 1024.0) * 0.001024 = 1309.9648 MB
        expected = (125.5 + 130.2 + 1024.0) * 0.001024
        assert abs(result - expected) < 0.001
    
    @patch('src.cog_creation.data_extraction.get_table_from_link')
    def test_find_data_storage_no_matching_pattern(self, mock_get_table):
        """Test when no sizes match the pattern."""
        mock_sizes = []
        for size_text in ['invalid', 'no numbers here']:
            tag = MagicMock()
            tag.text = size_text
            mock_sizes.append(tag)
        
        mock_get_table.return_value = mock_sizes
        
        # Pattern that requires digits
        result = find_data_storage("http://test.url", r"\d+\.\d+")
        assert result == 0  # No matching sizes
    
    @patch('src.cog_creation.data_extraction.get_table_from_link')
    def test_find_data_storage_empty_list(self, mock_get_table):
        """Test with empty size list."""
        mock_get_table.return_value = []
        
        result = find_data_storage("http://test.url", r"\d+\.\d+")
        assert result == 0


class TestFindTiffUrl:
    """Tests for find_tiff_url function."""
    
    @patch('src.cog_creation.data_extraction.get_table_from_link')
    def test_find_tiff_url_year_pattern(self, mock_get_table):
        """Test finding year directory URLs."""
        # Create mock link elements
        mock_links = []
        for href in ['1981/', '1982/', 'readme.txt', '2020/']:
            tag = MagicMock()
            anchor = MagicMock()
            # Use default parameter to capture href value for each iteration
            anchor.__getitem__ = lambda self, key, h=href: h if key == 'href' else None
            tag.find_all.return_value = [anchor]
            mock_links.append(tag)
        
        mock_get_table.return_value = mock_links
        
        base_url = "https://data.chc.ucsb.edu/products/"
        result = find_tiff_url(base_url, r"\d{4}\/")
        
        assert len(result) == 3  # Should match 1981/, 1982/, 2020/
        assert f"{base_url}1981/" in result
        assert f"{base_url}1982/" in result
        assert f"{base_url}2020/" in result
        assert f"{base_url}readme.txt" not in result
    
    @patch('src.cog_creation.data_extraction.get_table_from_link')
    def test_find_tiff_url_chirps_pattern(self, mock_get_table):
        """Test finding CHIRPS data file URLs."""
        mock_links = []
        for href in ['chirps-v2.0.1981.01.01.tif.gz', 'chirps-v2.0.1981.01.02.tif.gz', 'other.txt']:
            tag = MagicMock()
            anchor = MagicMock()
            anchor.__getitem__ = lambda self, key, h=href: h if key == 'href' else None
            tag.find_all.return_value = [anchor]
            mock_links.append(tag)
        
        mock_get_table.return_value = mock_links
        
        base_url = "https://data.chc.ucsb.edu/1981/"
        result = find_tiff_url(base_url, r"chirps-.*")
        
        assert len(result) == 2  # Should match only chirps files
        assert f"{base_url}chirps-v2.0.1981.01.01.tif.gz" in result
        assert f"{base_url}chirps-v2.0.1981.01.02.tif.gz" in result
        assert f"{base_url}other.txt" not in result
    
    @patch('src.cog_creation.data_extraction.get_table_from_link')
    def test_find_tiff_url_no_matches(self, mock_get_table):
        """Test when no URLs match the pattern."""
        mock_links = []
        tag = MagicMock()
        anchor = MagicMock()
        anchor.__getitem__ = lambda self, key: 'readme.txt' if key == 'href' else None
        tag.find_all.return_value = [anchor]
        mock_links.append(tag)
        
        mock_get_table.return_value = mock_links
        
        result = find_tiff_url("https://test.url/", r"chirps-.*")
        assert result == []