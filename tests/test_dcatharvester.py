"""
Tests for DCATHarvester base class.
Tests the content fetching and utility methods used by DCAT-based harvesters.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import HTTPError, ConnectionError, Timeout
from molgenis_fdp_harvester.rdf_harvester.dcatharvester import DCATHarvester


@pytest.fixture
def harvester():
    """Create a DCATHarvester instance"""
    return DCATHarvester()


# ============================================================================
# Local File Tests
# ============================================================================

def test_get_content_and_type_local_file_success(harvester):
    """Test reading content from valid local file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
        f.write("@prefix dcat: <http://www.w3.org/ns/dcat#> .")
        temp_path = f.name

    try:
        with patch('rdflib.util.guess_format', return_value='turtle'):
            content, content_type = harvester._get_content_and_type(temp_path)

        assert content == "@prefix dcat: <http://www.w3.org/ns/dcat#> ."
        assert content_type == 'turtle'
    finally:
        os.unlink(temp_path)


def test_get_content_and_type_local_file_with_explicit_type(harvester):
    """Test that explicit content_type overrides guessed format"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
        f.write("@prefix dcat: <http://www.w3.org/ns/dcat#> .")
        temp_path = f.name

    try:
        with patch('rdflib.util.guess_format') as mock_guess:
            content, content_type = harvester._get_content_and_type(
                temp_path,
                content_type='xml'
            )

        # Should not call guess_format when content_type is provided
        mock_guess.assert_not_called()
        assert content_type == 'xml'
    finally:
        os.unlink(temp_path)


def test_get_content_and_type_local_file_not_found(harvester):
    """Test error when local file doesn't exist"""
    with patch.object(harvester, '_save_gather_error') as mock_save_error:
        content, content_type = harvester._get_content_and_type('/nonexistent/file.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()
        assert "Could not get content for this url" in mock_save_error.call_args[0][0]


# ============================================================================
# HTTP Success Tests
# ============================================================================

def test_get_content_and_type_http_success(harvester):
    """Test successful HTTP GET request"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'text/turtle; charset=utf-8'}
    mock_response.iter_content.return_value = [b"@prefix dcat: ", b"<http://www.w3.org/ns/dcat#> ."]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content == "@prefix dcat: <http://www.w3.org/ns/dcat#> ."
        assert content_type == 'text/turtle'
        mock_session.head.assert_called_once()
        mock_session.get.assert_called_once()


def test_get_content_and_type_http_head_405_fallback(harvester):
    """Test HEAD returning 405 triggers direct GET"""
    mock_head_response = Mock()
    mock_head_response.status_code = 405

    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.headers = {'content-type': 'text/turtle'}
    mock_get_response.iter_content.return_value = [b"test content"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_head_response
        mock_session.get.return_value = mock_get_response

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content == "test content"
        # GET should be called once after 405 (with stream=True, did_get=True skips second GET)
        assert mock_session.get.call_count == 1


def test_get_content_and_type_http_head_400_fallback(harvester):
    """Test HEAD returning 400 triggers direct GET"""
    mock_head_response = Mock()
    mock_head_response.status_code = 400

    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.headers = {'content-type': 'text/turtle'}
    mock_get_response.iter_content.return_value = [b"test content"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_head_response
        mock_session.get.return_value = mock_get_response

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content == "test content"
        # GET should be called once after 400 (with stream=True, did_get=True skips second GET)
        assert mock_session.get.call_count == 1


def test_get_content_and_type_pagination_with_query(harvester):
    """Test page parameter adds & when URL has existing query string"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'text/turtle'}
    mock_response.iter_content.return_value = [b"page 2"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        harvester._get_content_and_type('http://example.com/catalog.ttl?format=ttl', page=2)

        # Check that the URL was called with &page=2
        head_call_url = mock_session.head.call_args[0][0]
        assert '?format=ttl&page=2' in head_call_url


def test_get_content_and_type_pagination_without_query(harvester):
    """Test page parameter adds ? when URL has no query string"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'text/turtle'}
    mock_response.iter_content.return_value = [b"page 2"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        harvester._get_content_and_type('http://example.com/catalog.ttl', page=2)

        # Check that the URL was called with ?page=2
        head_call_url = mock_session.head.call_args[0][0]
        assert '?page=2' in head_call_url


def test_get_content_and_type_content_type_with_charset(harvester):
    """Test content-type with charset is properly parsed"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'application/rdf+xml; charset=utf-8'}
    mock_response.iter_content.return_value = [b"<rdf>test</rdf>"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.rdf')

        # Charset should be stripped
        assert content_type == 'application/rdf+xml'


# ============================================================================
# File Size Limit Tests
# ============================================================================

def test_get_content_and_type_file_too_large_header(harvester):
    """Test rejection when Content-Length header exceeds limit"""
    max_size = 1024 * 1024 * 50  # 50MB
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-length': str(max_size + 1)}

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response

        content, content_type = harvester._get_content_and_type('http://example.com/huge.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()
        assert "too big" in mock_save_error.call_args[0][0].lower()


def test_get_content_and_type_file_too_large_streaming(harvester):
    """Test rejection when streamed content exceeds limit during download"""
    # Create chunks that will exceed the limit
    chunk_size = 1024 * 512  # 512KB
    num_chunks = 101  # 101 * 512KB > 50MB

    mock_head_response = Mock()
    mock_head_response.status_code = 200
    mock_head_response.headers = {}  # No content-length

    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.headers = {}
    mock_get_response.iter_content.return_value = [b'x' * chunk_size for _ in range(num_chunks)]

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_head_response
        mock_session.get.return_value = mock_get_response

        content, content_type = harvester._get_content_and_type('http://example.com/huge.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()
        assert "too big" in mock_save_error.call_args[0][0].lower()


def test_get_content_and_type_file_just_under_limit(harvester):
    """Test successful download of file just under the limit"""
    # Create content just under 50MB
    chunk_size = 1024 * 512  # 512KB
    num_chunks = 99  # 99 * 512KB < 50MB

    mock_head_response = Mock()
    mock_head_response.status_code = 200
    mock_head_response.headers = {}

    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.headers = {'content-type': 'text/turtle'}
    mock_get_response.iter_content.return_value = [b'x' * chunk_size for _ in range(num_chunks)]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_head_response
        mock_session.get.return_value = mock_get_response

        content, content_type = harvester._get_content_and_type('http://example.com/large.ttl')

        assert content is not None
        assert len(content) == chunk_size * num_chunks


# ============================================================================
# HTTP Error Handling Tests
# ============================================================================

def test_get_content_and_type_http_error_non_404(harvester):
    """Test HTTPError handling for non-404 errors"""
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.reason = 'Internal Server Error'

    mock_error = HTTPError(response=mock_response)

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.side_effect = mock_error

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()
        error_msg = mock_save_error.call_args[0][0]
        assert '500' in error_msg
        assert 'Internal Server Error' in error_msg


def test_get_content_and_type_http_error_404_page_1(harvester):
    """Test HTTPError 404 on page 1 is caught and logged"""
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.reason = 'Not Found'

    mock_error = HTTPError(response=mock_response)

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.side_effect = mock_error

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl', page=1)

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()


def test_get_content_and_type_http_error_404_page_2_raises(harvester):
    """Test HTTPError 404 on page > 1 is re-raised (not caught)"""
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.reason = 'Not Found'

    mock_error = HTTPError(response=mock_response)

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.side_effect = mock_error

        # Should raise the HTTPError instead of catching it
        with pytest.raises(HTTPError):
            harvester._get_content_and_type('http://example.com/catalog.ttl', page=2)


def test_get_content_and_type_connection_error(harvester):
    """Test ConnectionError handling"""
    mock_error = ConnectionError("Connection refused")

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.side_effect = mock_error

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()
        error_msg = mock_save_error.call_args[0][0]
        assert "connection error" in error_msg.lower()


def test_get_content_and_type_timeout(harvester):
    """Test Timeout handling"""
    mock_error = Timeout("Connection timed out")

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.side_effect = mock_error

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()
        error_msg = mock_save_error.call_args[0][0]
        assert "timed out" in error_msg.lower()


def test_get_content_and_type_raise_for_status_error(harvester):
    """Test that raise_for_status errors are caught"""
    mock_response = Mock()
    mock_response.status_code = 403
    mock_response.reason = 'Forbidden'
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)

    with patch('requests.Session') as mock_session_class, \
         patch.object(harvester, '_save_gather_error') as mock_save_error:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content is None
        assert content_type is None
        mock_save_error.assert_called_once()


# ============================================================================
# _get_package_name() Tests
# ============================================================================

def test_get_package_name_generates_new_name_none_package(harvester):
    """Test name generation when harvest_object.package is None"""
    harvest_object = Mock()
    harvest_object.package = None

    with patch.object(harvester, '_gen_new_name', return_value='test-dataset'):
        name = harvester._get_package_name(harvest_object, 'Test Dataset')

        assert name == 'test-dataset'
        harvester._gen_new_name.assert_called_once_with('Test Dataset')


def test_get_package_name_generates_new_name_different_title(harvester):
    """Test name generation when package exists but title differs"""
    harvest_object = Mock()
    harvest_object.package = Mock()
    harvest_object.package.title = 'Old Title'
    harvest_object.package.name = 'old-title'

    with patch.object(harvester, '_gen_new_name', return_value='new-title'):
        name = harvester._get_package_name(harvest_object, 'New Title')

        assert name == 'new-title'
        harvester._gen_new_name.assert_called_once_with('New Title')


def test_get_package_name_uses_existing_name_same_title(harvester):
    """Test using existing package.name when title matches"""
    harvest_object = Mock()
    harvest_object.package = Mock()
    harvest_object.package.title = 'Test Dataset'
    harvest_object.package.name = 'existing-name'

    with patch.object(harvester, '_gen_new_name') as mock_gen:
        name = harvester._get_package_name(harvest_object, 'Test Dataset')

        assert name == 'existing-name'
        # Should NOT call _gen_new_name when using existing name
        mock_gen.assert_not_called()


def test_get_package_name_raises_exception_empty_name(harvester):
    """Test exception when _gen_new_name returns empty/None"""
    harvest_object = Mock()
    harvest_object.package = None

    with patch.object(harvester, '_gen_new_name', return_value=None):
        with pytest.raises(Exception) as exc_info:
            harvester._get_package_name(harvest_object, 'Test Dataset')

        assert "Could not generate a unique name" in str(exc_info.value)


def test_get_package_name_raises_exception_empty_string(harvester):
    """Test exception when _gen_new_name returns empty string"""
    harvest_object = Mock()
    harvest_object.package = None

    with patch.object(harvester, '_gen_new_name', return_value=''):
        with pytest.raises(Exception) as exc_info:
            harvester._get_package_name(harvest_object, 'Test Dataset')

        assert "Could not generate a unique name" in str(exc_info.value)


# ============================================================================
# Edge Cases
# ============================================================================

def test_get_content_and_type_mixed_case_http_protocol(harvester):
    """Test that mixed case HTTP protocol is recognized"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'text/turtle'}
    mock_response.iter_content.return_value = [b"test"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        content, content_type = harvester._get_content_and_type('HTTP://example.com/catalog.ttl')

        assert content == "test"
        assert content_type == 'text/turtle'


def test_get_content_and_type_no_content_type_header(harvester):
    """Test handling when response has no content-type header"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {}  # No content-type
    mock_response.iter_content.return_value = [b"test content"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        content, content_type = harvester._get_content_and_type('http://example.com/catalog.ttl')

        assert content == "test content"
        assert content_type is None


def test_get_content_and_type_explicit_content_type_parameter(harvester):
    """Test that explicit content_type parameter is preserved"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {'content-type': 'text/plain'}
    mock_response.iter_content.return_value = [b"test"]

    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.head.return_value = mock_response
        mock_session.get.return_value = mock_response

        # Provide explicit content_type
        content, content_type = harvester._get_content_and_type(
            'http://example.com/catalog.ttl',
            content_type='turtle'
        )

        # Should return the explicitly provided type, not from headers
        assert content_type == 'turtle'
