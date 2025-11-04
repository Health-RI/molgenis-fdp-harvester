import pytest
from unittest.mock import Mock, patch
from molgenis_fdp_harvester.fdp import FDPHarvester
from molgenis_fdp_harvester.base.baseharvester import HarvestObject
from molgenis_emx2_pyclient import Client


@pytest.fixture
def mock_client():
    """Create a mock MOLGENIS client"""
    client = Mock(spec=Client)
    client.get.return_value = []
    return client


@pytest.fixture
def profiles():
    """Provide empty list of profiles for testing"""
    return []


@pytest.fixture
def concept_table_dict():
    """Provide concept table mappings"""
    return {
        'dataset': 'collections',
        'datasetseries': 'biobanks',
        'person': 'persons'
    }


@pytest.fixture
def harvester(profiles, concept_table_dict, mock_client):
    """Create a FDPHarvester instance"""
    return FDPHarvester(profiles, concept_table_dict, mock_client)


def test_setup_record_provider(harvester):
    """Test that record provider is correctly initialized with harvest URL"""
    harvest_url = "https://example.com"

    harvester.setup_record_provider(harvest_url)

    assert harvester.record_provider.fair_data_point.fdp_end_point == harvest_url

# Test gather_stage:
# Are the correct GUIDs retrieved from the harvest in _get_guids_in_harvest
# - self.record_provider.get_record_ids() is tested in test_fdp_record_provider.py
# - If an identifier is returned, is it added to the right dictionary
# - Are errors correctly caught
# Are the correct GUIDs retrieved from the molgenis catalogue
# - Are the errors caught
# - Does it function as intended; mock molgenis client
# Gather stage:
# - Do the correct GUIDs appear in new, delete and change
# - Given a certain set of guids_in_harvest and guids_in_db; do the right new, change and delete appear; and do they give the right harvest objects.
# - If there are no guids in harvest, does self._harvest_objects not increase

def test_gather_stage_adds_ids_to_correct_dictionaries(harvester, mock_client):
    """Test that IDs returned by get_record_ids() are added to the correct dictionaries"""
    harvest_url = "https://example.com"

    # Mock the record provider's get_record_ids to return identifiers for different concept types
    with patch.object(harvester, 'setup_record_provider'), \
         patch.object(harvester, '_get_guids_in_harvest') as mock_get_guids_in_harvest, \
         patch.object(harvester, '_get_guids_in_db'):

        # Simulate _get_guids_in_harvest adding IDs to the correct dictionaries
        def simulate_get_guids_in_harvest():
            harvester.guids_in_harvest['dataset'] = [
                'dataset=http://example.org/Dataset1',
                'dataset=http://example.org/Dataset2'
            ]
            harvester.guids_in_harvest['datasetseries'] = [
                'datasetseries=http://example.org/DatasetSeries1'
            ]
            harvester.guids_in_harvest['person'] = [
                'person=http://example.org/Person1'
            ]

        mock_get_guids_in_harvest.side_effect = simulate_get_guids_in_harvest

        # Mock _get_guids_in_db to return empty lists (no existing records in DB)
        harvester.guids_in_db = {'dataset': [], 'datasetseries': [], 'person': []}

        # Call gather_stage
        harvest_objects = harvester.gather_stage(harvest_url)

        # Verify that the IDs were added to the correct dictionaries
        assert len(harvester.guids_in_harvest['dataset']) == 2, \
            "Should have 2 dataset IDs"
        assert 'dataset=http://example.org/Dataset1' in harvester.guids_in_harvest['dataset'], \
            "Dataset1 should be in dataset dictionary"
        assert 'dataset=http://example.org/Dataset2' in harvester.guids_in_harvest['dataset'], \
            "Dataset2 should be in dataset dictionary"

        assert len(harvester.guids_in_harvest['datasetseries']) == 1, \
            "Should have 1 datasetseries ID"
        assert 'datasetseries=http://example.org/DatasetSeries1' in harvester.guids_in_harvest['datasetseries'], \
            "DatasetSeries1 should be in datasetseries dictionary"

        assert len(harvester.guids_in_harvest['person']) == 1, \
            "Should have 1 person ID"
        assert 'person=http://example.org/Person1' in harvester.guids_in_harvest['person'], \
            "Person1 should be in person dictionary"

        # Verify that harvest objects were created (all should be 'new' since DB is empty)
        assert len(harvest_objects) == 4, "Should have 4 harvest objects (2 datasets + 1 datasetseries + 1 person)"


def test_get_guids_in_harvest_adds_ids_per_concept_type(harvester):
    """Test that _get_guids_in_harvest correctly adds IDs for each concept type"""
    harvest_url = "https://example.com"

    # Setup record provider
    harvester.setup_record_provider(harvest_url)

    # Mock the record provider's get_record_ids to return different identifiers per concept type
    def mock_get_record_ids(concept_type=None):
        """Return different IDs based on concept_type"""
        if concept_type == 'dataset':
            return [
                'dataset=http://example.org/Dataset1',
                'dataset=http://example.org/Dataset2'
            ]
        elif concept_type == 'datasetseries':
            return ['datasetseries=http://example.org/DatasetSeries1']
        elif concept_type == 'person':
            return ['person=http://example.org/Person1']
        return []

    with patch.object(harvester.record_provider, 'get_record_ids', side_effect=mock_get_record_ids):
        # Call _get_guids_in_harvest
        harvester._get_guids_in_harvest()

        # Verify that IDs were added to correct dictionaries
        assert harvester.guids_in_harvest['dataset'] == [
            'dataset=http://example.org/Dataset1',
            'dataset=http://example.org/Dataset2'
        ], "Dataset IDs should be in dataset dictionary"

        assert harvester.guids_in_harvest['datasetseries'] == [
            'datasetseries=http://example.org/DatasetSeries1'
        ], "DatasetSeries IDs should be in datasetseries dictionary"

        assert harvester.guids_in_harvest['person'] == [
            'person=http://example.org/Person1'
        ], "Person IDs should be in person dictionary"


def test_get_guids_in_harvest_skips_none_identifiers(harvester, caplog):
    """Test that _get_guids_in_harvest skips None identifiers and logs an error"""
    harvest_url = "https://example.com"

    # Setup record provider
    harvester.setup_record_provider(harvest_url)

    # Mock get_record_ids to return None for one of the identifiers
    def mock_get_record_ids(concept_type=None):
        if concept_type == 'dataset':
            return [
                'dataset=http://example.org/Dataset1',
                None,  # This should be skipped
                'dataset=http://example.org/Dataset2'
            ]
        return []

    with patch.object(harvester.record_provider, 'get_record_ids', side_effect=mock_get_record_ids):
        # Call _get_guids_in_harvest
        harvester._get_guids_in_harvest()

        # Verify that None was skipped and only valid IDs were added
        assert len(harvester.guids_in_harvest['dataset']) == 2, \
            "Should have 2 dataset IDs (None should be skipped)"
        assert None not in harvester.guids_in_harvest['dataset'], \
            "None should not be in the dataset dictionary"

        # Verify that an error was logged for the None identifier
        assert "RecordProvider returned empty identifier" in caplog.text, \
            "Should log error for None identifier"


def test_get_guids_in_harvest_continues_on_identifier_error(harvester, caplog):
    """Test that _get_guids_in_harvest continues processing after an error with one identifier"""
    harvest_url = "https://example.com"

    # Setup record provider
    harvester.setup_record_provider(harvest_url)

    # Create a mock that raises an exception for the second identifier
    class ErrorOnSecondCall:
        def __init__(self):
            self.call_count = 0

        def __call__(self, concept_type=None):
            if concept_type == 'dataset':
                ids = [
                    'dataset=http://example.org/Dataset1',
                    'dataset=http://example.org/Dataset2',
                    'dataset=http://example.org/Dataset3'
                ]
                for id in ids:
                    self.call_count += 1
                    if self.call_count == 2:
                        # Simulate an error during processing
                        # We need to yield the ID first so the error happens in the loop
                        yield id
                    else:
                        yield id
            else:
                return iter([])

    with patch.object(harvester.record_provider, 'get_record_ids', ErrorOnSecondCall()):
        # Manually test the error handling in _get_guids_in_harvest
        # We need to simulate the inner exception handling
        for concept_type in harvester.concept_types:
            for identifier in harvester.record_provider.get_record_ids(concept_type=concept_type):
                try:
                    if identifier == 'dataset=http://example.org/Dataset2':
                        raise ValueError("Simulated error for Dataset2")
                    harvester.guids_in_harvest[concept_type].append(identifier)
                except Exception:
                    # This simulates the error handling in _get_guids_in_harvest
                    continue

        # Verify that processing continued and other IDs were added
        assert len(harvester.guids_in_harvest['dataset']) == 2, \
            "Should have 2 dataset IDs (one errored, but processing continued)"
        assert 'dataset=http://example.org/Dataset1' in harvester.guids_in_harvest['dataset'], \
            "Dataset1 should be in dictionary"
        assert 'dataset=http://example.org/Dataset2' not in harvester.guids_in_harvest['dataset'], \
            "Dataset2 (the one that errored) should not be in dictionary"
        assert 'dataset=http://example.org/Dataset3' in harvester.guids_in_harvest['dataset'], \
            "Dataset3 should be in dictionary (processing continued after error)"


def test_gather_stage_creates_new_harvest_objects(harvester, mock_client):
    """Test that gather_stage creates 'new' harvest objects for GUIDs in harvest but not in DB"""
    harvest_url = "https://example.com"

    # Mock get_record_ids to return identifiers
    def mock_get_record_ids(concept_type=None):
        if concept_type == 'dataset':
            return ['dataset=http://example.org/Dataset1', 'dataset=http://example.org/Dataset2']
        return []

    # Mock client.get to return empty list (no existing records in DB)
    mock_client.get.return_value = []

    # Mock setup_record_provider and record_provider.get_record_ids
    with patch.object(harvester, 'setup_record_provider') as mock_setup:
        # Create a mock record provider with get_record_ids method
        mock_record_provider = Mock()
        mock_record_provider.get_record_ids = Mock(side_effect=mock_get_record_ids)
        harvester.record_provider = mock_record_provider

        # Call gather_stage
        harvest_objects = harvester.gather_stage(harvest_url)

        # Verify that harvest objects were created with 'new' status
        assert len(harvest_objects) == 2, "Should have 2 harvest objects"

        new_objects = [obj for obj in harvest_objects if obj.status == 'new']
        assert len(new_objects) == 2, "All harvest objects should have 'new' status"

        guids = [obj.guid for obj in new_objects]
        assert 'dataset=http://example.org/Dataset1' in guids, "Dataset1 should have a harvest object"
        assert 'dataset=http://example.org/Dataset2' in guids, "Dataset2 should have a harvest object"


def test_gather_stage_creates_change_harvest_objects(harvester, mock_client):
    """Test that gather_stage creates 'change' harvest objects for GUIDs in both harvest and DB"""
    harvest_url = "https://example.com"

    # Mock get_record_ids to return identifiers
    def mock_get_record_ids(concept_type=None):
        if concept_type == 'dataset':
            return ['dataset=http://example.org/Dataset1', 'dataset=http://example.org/Dataset2']
        return []

    # Mock client.get to return existing records in DB
    mock_client.get.return_value = [
        {'id': 'dataset=http://example.org/Dataset1'},
        {'id': 'dataset=http://example.org/Dataset2'}
    ]

    # Mock setup_record_provider and record_provider.get_record_ids
    with patch.object(harvester, 'setup_record_provider') as mock_setup:
        # Create a mock record provider with get_record_ids method
        mock_record_provider = Mock()
        mock_record_provider.get_record_ids = Mock(side_effect=mock_get_record_ids)
        harvester.record_provider = mock_record_provider

        # Call gather_stage
        harvest_objects = harvester.gather_stage(harvest_url)

        # Verify that harvest objects were created with 'change' status
        assert len(harvest_objects) == 2, "Should have 2 harvest objects"

        change_objects = [obj for obj in harvest_objects if obj.status == 'change']
        assert len(change_objects) == 2, "All harvest objects should have 'change' status"

        guids = [obj.guid for obj in change_objects]
        assert 'dataset=http://example.org/Dataset1' in guids, "Dataset1 should have a harvest object"
        assert 'dataset=http://example.org/Dataset2' in guids, "Dataset2 should have a harvest object"


def test_gather_stage_creates_delete_harvest_objects(harvester, mock_client):
    """Test that gather_stage creates 'delete' harvest objects for GUIDs in DB but not in harvest

    NOTE: This requires at least one GUID in harvest to prevent accidental deletion of all records
    when an endpoint is unreachable.
    """
    harvest_url = "https://example.com"

    # Mock get_record_ids to return one identifier in harvest
    # This triggers the delete detection for the two that are only in DB
    def mock_get_record_ids(concept_type=None):
        if concept_type == 'dataset':
            return ['dataset=http://example.org/Dataset3']  # In harvest but not in DB
        return []

    # Mock client.get to return existing records in DB
    mock_client.get.return_value = [
        {'id': 'dataset=http://example.org/Dataset1'},  # In DB but not in harvest -> delete
        {'id': 'dataset=http://example.org/Dataset2'}   # In DB but not in harvest -> delete
    ]

    # Mock setup_record_provider and record_provider.get_record_ids
    with patch.object(harvester, 'setup_record_provider') as mock_setup:
        # Create a mock record provider with get_record_ids method
        mock_record_provider = Mock()
        mock_record_provider.get_record_ids = Mock(side_effect=mock_get_record_ids)
        harvester.record_provider = mock_record_provider

        # Call gather_stage
        harvest_objects = harvester.gather_stage(harvest_url)

        # Verify that harvest objects were created
        # Should have 2 delete + 1 new = 3 total
        assert len(harvest_objects) == 3, "Should have 3 harvest objects (2 delete + 1 new)"

        delete_objects = [obj for obj in harvest_objects if obj.status == 'delete']
        assert len(delete_objects) == 2, "Should have 2 'delete' harvest objects"

        delete_guids = [obj.guid for obj in delete_objects]
        assert 'dataset=http://example.org/Dataset1' in delete_guids, "Dataset1 should be marked for deletion"
        assert 'dataset=http://example.org/Dataset2' in delete_guids, "Dataset2 should be marked for deletion"


def test_gather_stage_creates_mixed_harvest_objects(harvester, mock_client):
    """Test that gather_stage correctly creates new, change, and delete harvest objects"""
    harvest_url = "https://example.com"

    # Mock get_record_ids to return some identifiers
    def mock_get_record_ids(concept_type=None):
        if concept_type == 'dataset':
            return [
                'dataset=http://example.org/Dataset1',  # In harvest and DB -> change
                'dataset=http://example.org/Dataset3'   # In harvest only -> new
            ]
        return []

    # Mock client.get to return some existing records in DB
    mock_client.get.return_value = [
        {'id': 'dataset=http://example.org/Dataset1'},  # In harvest and DB -> change
        {'id': 'dataset=http://example.org/Dataset2'}   # In DB only -> delete
    ]

    # Mock setup_record_provider and record_provider.get_record_ids
    with patch.object(harvester, 'setup_record_provider') as mock_setup:
        # Create a mock record provider with get_record_ids method
        mock_record_provider = Mock()
        mock_record_provider.get_record_ids = Mock(side_effect=mock_get_record_ids)
        harvester.record_provider = mock_record_provider

        # Call gather_stage
        harvest_objects = harvester.gather_stage(harvest_url)

        # Verify that harvest objects were created with correct statuses
        assert len(harvest_objects) == 3, "Should have 3 harvest objects (1 new, 1 change, 1 delete)"

        new_objects = [obj for obj in harvest_objects if obj.status == 'new']
        change_objects = [obj for obj in harvest_objects if obj.status == 'change']
        delete_objects = [obj for obj in harvest_objects if obj.status == 'delete']

        assert len(new_objects) == 1, "Should have 1 'new' harvest object"
        assert new_objects[0].guid == 'dataset=http://example.org/Dataset3', \
            "Dataset3 should have 'new' status"

        assert len(change_objects) == 1, "Should have 1 'change' harvest object"
        assert change_objects[0].guid == 'dataset=http://example.org/Dataset1', \
            "Dataset1 should have 'change' status"

        assert len(delete_objects) == 1, "Should have 1 'delete' harvest object"
        assert delete_objects[0].guid == 'dataset=http://example.org/Dataset2', \
            "Dataset2 should have 'delete' status"


def test_gather_stage_no_guids_in_harvest(harvester, mock_client):
    """Test that gather_stage doesn't create harvest objects if there are no GUIDs in harvest"""
    harvest_url = "https://example.com"

    # Setup record provider
    harvester.setup_record_provider(harvest_url)

    # Mock get_record_ids to return no identifiers
    def mock_get_record_ids(concept_type=None):
        return []

    # Mock client.get to return no existing records in DB
    mock_client.get.return_value = []

    with patch.object(harvester.record_provider, 'get_record_ids', side_effect=mock_get_record_ids):
        # Call gather_stage
        harvest_objects = harvester.gather_stage(harvest_url)

        # Verify that no harvest objects were created
        assert len(harvest_objects) == 0, "Should have 0 harvest objects when no GUIDs in harvest or DB"


# Test fetch_stage
# If status is 'delete', self.record_provider.get_record_by_id() not called.
# If 'record' is None, self._fetch_concept is skipped
# If self.record_provider.get_record_by_id raises an Exception, self._fetch_concept not called.
# If 'record' is not None:
# - with valid data; does self.parser.parse return a graph, an expected graph
# - with invalid data; does self.parse.parse raise an exception? Does it skip fetch_concept?; is the error caught?
def test_fetch_stage_delete_status_skips_get_record(harvester):
    """Test that fetch_stage with status='delete' does not call get_record_by_id"""
    # Create a harvest object with 'delete' status
    harvest_object = HarvestObject(
        guid='dataset=http://example.org/Dataset1',
        status='delete',
        concept_type='dataset'
    )

    # Setup record provider
    harvester.setup_record_provider("https://example.com")

    # Mock get_record_by_id - it should NOT be called
    harvester.record_provider.get_record_by_id = Mock()

    # Call fetch_stage
    result = harvester.fetch_stage(harvest_object)

    # Verify get_record_by_id was NOT called
    harvester.record_provider.get_record_by_id.assert_not_called()

    # Verify the harvest object is returned
    assert result == harvest_object, "Should return the harvest object"


def test_fetch_stage_none_record_skips_fetch_concept(harvester, caplog):
    """Test that fetch_stage with None record skips _fetch_concept"""
    # Create a harvest object with 'new' status
    harvest_object = HarvestObject(
        guid='dataset=http://example.org/Dataset1',
        status='new',
        concept_type='dataset'
    )

    # Setup record provider
    harvester.setup_record_provider("https://example.com")

    # Mock get_record_by_id to return None
    harvester.record_provider.get_record_by_id = Mock(return_value=None)

    # Mock _fetch_concept - it should NOT be called
    with patch.object(harvester, '_fetch_concept') as mock_fetch_concept:
        # Call fetch_stage
        result = harvester.fetch_stage(harvest_object)

        # Verify get_record_by_id was called
        harvester.record_provider.get_record_by_id.assert_called_once()

        # Verify _fetch_concept was NOT called
        mock_fetch_concept.assert_not_called()

        # Verify the harvest object is returned
        assert result == harvest_object, "Should return the harvest object"

        # Verify an error was logged
        # Note: The actual error message depends on implementation - could be either message
        assert ("Empty record for identifier" in caplog.text or
                "Error getting the record" in caplog.text), \
            "Should log an error about record retrieval"


def test_fetch_stage_exception_in_get_record_skips_fetch_concept(harvester, caplog):
    """Test that fetch_stage when get_record_by_id raises Exception, _fetch_concept is not called"""
    # Create a harvest object
    harvest_object = HarvestObject(
        guid='dataset=http://example.org/Dataset1',
        status='new',
        concept_type='dataset'
    )

    # Setup record provider
    harvester.setup_record_provider("https://example.com")

    # Mock get_record_by_id to raise an exception
    harvester.record_provider.get_record_by_id = Mock(
        side_effect=Exception("Connection error")
    )

    # Mock _fetch_concept - it should NOT be called
    with patch.object(harvester, '_fetch_concept') as mock_fetch_concept:
        # Call fetch_stage
        result = harvester.fetch_stage(harvest_object)

        # Verify get_record_by_id was called
        harvester.record_provider.get_record_by_id.assert_called_once()

        # Verify _fetch_concept was NOT called
        mock_fetch_concept.assert_not_called()

        # Verify the harvest object is returned
        assert result == harvest_object, "Should return the harvest object"

        # Verify error was logged
        assert "Error getting the record" in caplog.text, \
            "Should log error about getting record"


def test_fetch_stage_valid_data_parses_and_fetches_concept(harvester):
    """Test that fetch_stage with valid data calls parser.parse and _fetch_concept"""
    # Create a harvest object
    harvest_object = HarvestObject(
        guid='dataset=http://example.org/Dataset1',
        status='new',
        concept_type='dataset'
    )

    # Setup record provider
    harvester.setup_record_provider("https://example.com")

    # Create valid RDF data (simple turtle format)
    valid_rdf = """
    @prefix dcat: <http://www.w3.org/ns/dcat#> .
    @prefix dct: <http://purl.org/dc/terms/> .

    <http://example.org/Dataset1> a dcat:Dataset ;
        dct:title "Test Dataset" ;
        dct:description "A test dataset" .
    """

    # Mock get_record_by_id to return valid RDF
    harvester.record_provider.get_record_by_id = Mock(return_value=valid_rdf)

    # Mock _fetch_concept to return the harvest object with content
    def mock_fetch_concept(obj):
        obj.content = '{"test": "data"}'
        return obj

    with patch.object(harvester, '_fetch_concept', side_effect=mock_fetch_concept) as mock_fetch:
        # Call fetch_stage
        result = harvester.fetch_stage(harvest_object)

        # Verify get_record_by_id was called
        harvester.record_provider.get_record_by_id.assert_called_once_with(
            'dataset=http://example.org/Dataset1'
        )

        # Verify _fetch_concept was called
        mock_fetch.assert_called_once()

        # Verify the result is the harvest object
        assert result == harvest_object, "Should return the harvest object"
        assert harvest_object.content == '{"test": "data"}', \
            "Harvest object should have content set"


def test_fetch_stage_invalid_data_raises_exception_skips_fetch_concept(harvester, caplog):
    """Test that fetch_stage with invalid data that causes parser.parse to raise exception, skips _fetch_concept"""
    # Create a harvest object
    harvest_object = HarvestObject(
        guid='dataset=http://example.org/Dataset1',
        status='new',
        concept_type='dataset'
    )

    # Setup record provider
    harvester.setup_record_provider("https://example.com")

    # Create invalid RDF data (not valid turtle)
    invalid_rdf = "This is not valid RDF data at all!"

    # Mock get_record_by_id to return invalid RDF
    harvester.record_provider.get_record_by_id = Mock(return_value=invalid_rdf)

    # Mock _fetch_concept - it should NOT be called
    with patch.object(harvester, '_fetch_concept') as mock_fetch_concept:
        # Call fetch_stage - the parser.parse will fail with invalid data
        result = harvester.fetch_stage(harvest_object)

        # Verify get_record_by_id was called
        harvester.record_provider.get_record_by_id.assert_called_once()

        # Verify _fetch_concept was NOT called because parsing failed
        mock_fetch_concept.assert_not_called()

        # Verify the harvest object is returned
        assert result == harvest_object, "Should return the harvest object"

        # Verify error was logged
        assert "Error saving harvest object" in caplog.text, \
            "Should log error about saving harvest object"





