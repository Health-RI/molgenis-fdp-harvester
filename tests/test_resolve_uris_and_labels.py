# Test _resolve_uris_and_labels
# value is a list, multiple values for countries;
# http://publications.europa.eu/resource/authority/country/NLD for NLD,
# http://publications.europa.eu/resource/authority/country/ESP for ESP,
# http://publications.europa.eu/resource/authority/country/DEU for DEU
#   returned_value are all values
#       _resolve_uris_and_labels returns 'NLD,ESP,DEU'
#   returned_value are all values, except the middle one
#       _resolve_uris_and_labels should return 'NLD,DEU'
#   returned_value is not a value for none of them
#       _resolve_uris_and_labels should return None

# value is not a list; NLD
#   returned_value is a value
#       _resolve_uris_and_labels returns NLD
#   returned_value is None
#       _resolve_uris_and_labels returns None

from unittest.mock import patch


def test_resolve_uris_and_labels_list_all_values_found(harvester):
    """Test when value is a list and all values are found"""
    # Setup: mock the _resolve_uri method to return values for all URIs
    values = [
        'http://publications.europa.eu/resource/authority/country/NLD',
        'http://publications.europa.eu/resource/authority/country/ESP',
        'http://publications.europa.eu/resource/authority/country/DEU'
    ]

    def mock_resolve_uri(value, table):
        uri_to_name = {
            'http://publications.europa.eu/resource/authority/country/NLD': [{'name': 'NLD'}],
            'http://publications.europa.eu/resource/authority/country/ESP': [{'name': 'ESP'}],
            'http://publications.europa.eu/resource/authority/country/DEU': [{'name': 'DEU'}]
        }
        return uri_to_name.get(value, None)

    with patch.object(harvester, '_resolve_uri', side_effect=mock_resolve_uri):
        with patch.object(harvester, '_resolve_label', return_value=None):
            result = harvester._resolve_uris_and_labels(values, 'countries')

    # Verify all values are returned as comma-separated string
    assert result == 'NLD,ESP,DEU'


def test_resolve_uris_and_labels_list_some_values_found(harvester):
    """Test when value is a list and only some values are found (middle one missing)"""
    # Setup: mock the _resolve_uri method to return values for NLD and DEU only
    values = [
        'http://publications.europa.eu/resource/authority/country/NLD',
        'http://publications.europa.eu/resource/authority/country/ESP',
        'http://publications.europa.eu/resource/authority/country/DEU'
    ]

    def mock_resolve_uri(value, table):
        uri_to_name = {
            'http://publications.europa.eu/resource/authority/country/NLD': [{'name': 'NLD'}],
            'http://publications.europa.eu/resource/authority/country/DEU': [{'name': 'DEU'}]
        }
        return uri_to_name.get(value, None)

    with patch.object(harvester, '_resolve_uri', side_effect=mock_resolve_uri):
        with patch.object(harvester, '_resolve_label', return_value=None):
            result = harvester._resolve_uris_and_labels(values, 'countries')

    # Verify only found values are returned (middle one omitted)
    assert result == 'NLD,DEU'


def test_resolve_uris_and_labels_list_no_values_found(harvester):
    """Test when value is a list and none of the values are found"""
    # Setup: mock the _resolve_uri method to return None for all URIs
    values = [
        'http://publications.europa.eu/resource/authority/country/NLD',
        'http://publications.europa.eu/resource/authority/country/ESP',
        'http://publications.europa.eu/resource/authority/country/DEU'
    ]

    with patch.object(harvester, '_resolve_uri', return_value=None):
        with patch.object(harvester, '_resolve_label', return_value=None):
            result = harvester._resolve_uris_and_labels(values, 'countries')

    # Verify None is returned when no values are found
    assert result is None


def test_resolve_uris_and_labels_single_value_found(harvester):
    """Test when value is not a list and the value is found"""
    # Setup: mock the _resolve_uri method to return a value for NLD
    value = 'http://publications.europa.eu/resource/authority/country/NLD'

    with patch.object(harvester, '_resolve_uri', return_value=[{'name': 'NLD'}]):
        with patch.object(harvester, '_resolve_label', return_value=None):
            result = harvester._resolve_uris_and_labels(value, 'countries')

    # Verify the name is returned
    assert result == 'NLD'


def test_resolve_uris_and_labels_single_value_not_found(harvester):
    """Test when value is not a list and the value is not found"""
    # Setup: mock the _resolve_uri method to return None
    value = 'http://publications.europa.eu/resource/authority/country/NLD'

    with patch.object(harvester, '_resolve_uri', return_value=None):
        with patch.object(harvester, '_resolve_label', return_value=None):
            result = harvester._resolve_uris_and_labels(value, 'countries')

    # Verify None is returned when value is not found
    assert result is None

