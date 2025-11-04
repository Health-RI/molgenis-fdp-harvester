# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from unittest.mock import Mock, patch
import json

from molgenis_emx2_pyclient import Client
from molgenis_fdp_harvester.rdf import DCATRDFHarvester
from molgenis_fdp_harvester.base.baseharvester import HarvestObject, munge_title_to_name
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from molgenis_fdp_harvester.utils import HarvesterException

"""
Test Plan for DCATRDFHarvester (src/molgenis_fdp_harvester/rdf.py)

Initialization:
- Does _initialize_tracking_dictionaries() create the correct empty dictionaries for guids_in_harvest,
  guids_in_db (all keyed by concept_types), and _datasets_without_datasetseries list?

Gather Stage (gather_stage method):
- Does _load_rdf_content() call _get_rdf() and raise HarvesterException on failure?
- Does _get_rdf() parse RDF content into the parser graph using the correct format?
  - For invalid RDF, does it call _save_gather_error() with appropriate message?
- Does _extract_concepts_from_rdf() iterate through parser.persons(), parser.datasetseries(),
  and parser.datasets() in that order?
  - Does it call _gather_concept_guid() for each concept with correct concept_type?
  - Are exceptions caught and re-raised as HarvesterException?
- Does _gather_concept_guid() extract the GUID using _get_guid() and append to guids_in_harvest?
  - When GUID extraction fails, does it call _save_gather_error() with descriptive message?
- Does _get_guids_in_db() retrieve existing IDs from molgenis_client.get() for each entity?
  - Are the IDs correctly extracted from the response and stored in guids_in_db[concept_type]?
  - Are exceptions logged and does it set guids_in_db[concept_type] to empty list on error?
- Does _create_harvest_objects() create HarvestObject instances for all GUIDs in guids_in_harvest?
  - Are objects created with status="new" and correct concept_type?
  - Are all objects added to _harvest_objects list?

Fetch Stage (fetch_stage and _fetch_concept methods):
- Does fetch_stage() delegate to _fetch_concept()?
- Does _fetch_concept() call parser.get_concept() with URIRef(guid) and concept_type?
- When concept_dict has no "name", does it call _generate_unique_name() with title and concept_type?
- When concept_dict has no "id", does it generate one using munge_title_to_name(guid)?
- For datasets when auto_create_datasetseries=True and no 'biobank' field:
  - Is dataset info (name, id, description, guid) appended to _datasets_without_datasetseries?
- Is the concept_dict serialized to JSON and stored in harvest_object.content?

Name Generation (_generate_unique_name):
- Does _generate_unique_name() call _gen_new_name() to create base name from title?
- For first occurrence, is the base name returned and added to _names_taken?
- For duplicates, does it count existing names with same base and append "-{count}" suffix?
- Are all generated names tracked in _names_taken[concept_type]?

Auto-generate Dataset Series (generate_missing_datasetseries):
- When _datasets_without_datasetseries is empty, does it return early without processing?
- For each dataset in _datasets_without_datasetseries:
  - Does _create_datasetseries_for_dataset() create a datasetseries dict with matching id/name?
  - Is the synthetic GUID created as "{dataset_guid}_datasetseries"?
  - Is a HarvestObject created with status="new" and concept_type="datasetseries"?
  - Is the datasetseries object appended to _harvest_objects?
  - Is the corresponding dataset's content updated with 'biobank' field pointing to datasetseries_id?
- Are the log messages about creation count emitted correctly?

GUID Extraction (_get_guid):
- Does it first check for "uri" field, then "identifier" field?
- When both fail and dataset has "name":
  - If source_url provided, does it concatenate source_url + "/" + name?
  - Otherwise, does it return just the name?
- Does it return None when no GUID can be determined?

Import Stage (import_stage):
- For status="delete", does it log warning and return True without deletion?
- For None/empty content, does it log error and return False?
- For invalid JSON content, does it catch ValueError, log error, and return False?
- For valid content:
  - Is the JSON parsed and concept_type extracted?
  - Is the correct entity_name retrieved from concept_table_link?
  - Does it call molgenis_client.save_schema() with correct table and data?
  - For status="new", is "Adding dataset" logged?
  - For status="change", is "Updating dataset" logged?
  - On client exceptions, is error logged with dataset name and traceback, returning False?
  - On success, does it return True?
"""

@pytest.fixture
def mock_client():
    return Mock(spec=Client)


@pytest.fixture
def profiles():
    return [MolgenisEUCAIMDCATAPProfile]


@pytest.fixture
def concept_table_dict():
    return {
        'dataset': 'datasets',
        'datasetseries': 'datasetseries',
        'person': 'persons'
    }


@pytest.fixture
def harvester(profiles, concept_table_dict, mock_client):
    return DCATRDFHarvester(
        profiles=profiles,
        concept_table_dict=concept_table_dict,
        molgenis_client=mock_client
    )


@pytest.fixture
def catalog_url():
    return "tests/test_data/rdf_catalog.ttl"

def test_harvester_initialization(harvester, profiles, concept_table_dict, mock_client):
    """Test that the harvester initializes correctly"""
    assert harvester._profiles == profiles
    assert harvester.concept_table_link == concept_table_dict
    assert harvester.concept_types == list(concept_table_dict.keys())
    assert harvester.molgenis_client == mock_client

    # Check initialized tracking dictionaries
    for concept_type in concept_table_dict.keys():
        assert concept_type in harvester.guids_in_harvest
        assert concept_type in harvester.guids_in_db
        assert harvester.guids_in_harvest[concept_type] == []
        assert harvester.guids_in_db[concept_type] == []

    # Check _datasets_without_datasetseries list exists
    assert harvester._datasets_without_datasetseries == []


@patch.object(DCATRDFHarvester, '_get_rdf')
@patch.object(DCATRDFHarvester, '_get_guids_in_db')
def test_gather_stage(mock_get_guids_in_db, mock_get_rdf, harvester, catalog_url):
    """Test the gather_stage method"""
    # Setup parser with mock data
    harvester.parser.parse(open('tests/test_data/rdf_dataset1.ttl').read(), _format='turtle')
    harvester.parser.parse(open('tests/test_data/rdf_dataset2.ttl').read(), _format='turtle')

    # Call gather_stage
    result = harvester.gather_stage(catalog_url)

    # Verify _get_rdf was called
    mock_get_rdf.assert_called_once_with(catalog_url)

    # Verify _get_guids_in_db_records was called
    mock_get_guids_in_db.assert_called_once()

    # Verify harvest objects were created
    assert len(result) == 2
    assert isinstance(result[0], HarvestObject)


@patch.object(DCATRDFHarvester, '_get_rdf')
def test_gather_stage_rdf_load_failure(mock_get_rdf, harvester, catalog_url):
    """Test gather_stage handles RDF loading errors"""

    # Setup mock to raise exception
    mock_get_rdf.side_effect = Exception("Failed to fetch RDF")

    # Call gather_stage and expect HarvesterException
    with pytest.raises(HarvesterException) as exc_info:
        harvester.gather_stage(catalog_url)

    assert "Failed to gather objects" in str(exc_info.value)


@patch.object(DCATRDFHarvester, '_get_rdf')
@patch.object(DCATRDFHarvester, '_extract_concepts_from_rdf')
def test_gather_stage_extraction_failure(mock_extract, mock_get_rdf, harvester, catalog_url):
    """Test gather_stage handles concept extraction errors"""

    # Setup mock to raise exception
    mock_extract.side_effect = HarvesterException("Failed to extract concepts")

    # Call gather_stage and expect HarvesterException
    with pytest.raises(HarvesterException):
        harvester.gather_stage(catalog_url)


def test_load_rdf_content_success(harvester):
    """Test _load_rdf_content calls _get_rdf"""
    with patch.object(harvester, '_get_rdf') as mock_get_rdf:
        harvester._load_rdf_content("http://example.com/rdf")
        mock_get_rdf.assert_called_once_with("http://example.com/rdf")


def test_load_rdf_content_failure(harvester):
    """Test _load_rdf_content raises HarvesterException on failure"""

    with patch.object(harvester, '_get_rdf', side_effect=Exception("RDF fetch failed")):
        with pytest.raises(HarvesterException) as exc_info:
            harvester._load_rdf_content("http://example.com/rdf")

        assert "Failed to load RDF" in str(exc_info.value)


def test_extract_concepts_from_rdf_success(harvester):
    """Test _extract_concepts_from_rdf extracts all concept types"""
    # Setup mock parser methods
    mock_persons = [{'uri': 'http://example.com/person1', 'name': 'Person 1'}]
    mock_datasetseries = [{'uri': 'http://example.com/series1', 'name': 'Series 1'}]
    mock_datasets = [
        {'uri': 'http://example.com/dataset1', 'name': 'Dataset 1'},
        {'uri': 'http://example.com/dataset2', 'name': 'Dataset 2'}
    ]

    with patch.object(harvester.parser, 'persons', return_value=mock_persons), \
         patch.object(harvester.parser, 'datasetseries', return_value=mock_datasetseries), \
         patch.object(harvester.parser, 'datasets', return_value=mock_datasets), \
         patch.object(harvester, '_gather_concept_guid') as mock_gather:

        harvester._extract_concepts_from_rdf()

        # Verify _gather_concept_guid was called for each concept
        assert mock_gather.call_count == 4
        mock_gather.assert_any_call(mock_persons[0], 'person')
        mock_gather.assert_any_call(mock_datasetseries[0], 'datasetseries')
        mock_gather.assert_any_call(mock_datasets[0], 'dataset')
        mock_gather.assert_any_call(mock_datasets[1], 'dataset')


def test_extract_concepts_from_rdf_failure(harvester):
    """Test _extract_concepts_from_rdf handles parser errors"""
    with patch.object(harvester.parser, 'persons', side_effect=Exception("Parser error")):
        with pytest.raises(HarvesterException) as exc_info:
            harvester._extract_concepts_from_rdf()

        assert "Failed to extract concepts" in str(exc_info.value)


def test_create_harvest_objects(harvester):
    """Test _create_harvest_objects creates objects from GUIDs"""
    # Setup test data
    harvester.guids_in_harvest['dataset'] = ['http://example.com/dataset1', 'http://example.com/dataset2']
    harvester.guids_in_harvest['person'] = ['http://example.com/person1']

    # Call method
    result = harvester._create_harvest_objects()

    # Verify objects were created
    assert len(result) == 3
    assert all(isinstance(obj, HarvestObject) for obj in result)
    assert all(obj.status == "new" for obj in result)

    # Check dataset objects
    dataset_objects = [obj for obj in result if obj.concept_type == 'dataset']
    assert len(dataset_objects) == 2
    dataset_guids = {obj.guid for obj in dataset_objects}
    assert 'http://example.com/dataset1' in dataset_guids
    assert 'http://example.com/dataset2' in dataset_guids

    # Check person objects
    person_objects = [obj for obj in result if obj.concept_type == 'person']
    assert len(person_objects) == 1
    assert person_objects[0].guid == 'http://example.com/person1'


def test_gather_concept_guid_failure(harvester):
    """Test _gather_concept_guid handles missing GUID"""
    # Setup test data - uri is required but returns None from _get_guid
    concept_dict = {"uri": "http://example.com/dataset1", "title": "Test Dataset"}
    concept_type = "dataset"

    with patch.object(harvester, '_save_gather_error') as mock_save_error, \
         patch.object(harvester, '_get_guid', return_value=None):

        # Call method
        harvester._gather_concept_guid(concept_dict, concept_type)

        # Verify error was saved
        mock_save_error.assert_called_once()
        assert "Could not get a unique identifier" in mock_save_error.call_args[0][0]

        # Verify guid was NOT added to guids_in_harvest
        assert len(harvester.guids_in_harvest[concept_type]) == 0


def test_get_guids_in_db(harvester, mock_client):
    """Test _get_guids_in_db method"""
    # Setup mock client response
    mock_client.get.side_effect = [
        [{"id": "dataset1-id"}, {"id": "dataset2-id"}],  # datasets
        [{"id": "series1-id"}],  # datasetseries
        [{"id": "person1-id"}, {"id": "person2-id"}]  # persons
    ]

    # Call method
    harvester._get_guids_in_db()

    # Verify client calls
    assert mock_client.get.call_count == 3

    # Verify guids_in_db was populated
    assert harvester.guids_in_db['dataset'] == ["dataset1-id", "dataset2-id"]
    assert harvester.guids_in_db['datasetseries'] == ["series1-id"]
    assert harvester.guids_in_db['person'] == ["person1-id", "person2-id"]


def test_get_guids_in_db_error_handling(harvester, mock_client):
    """Test error handling in _get_guids_in_db"""
    # Setup mock client to raise exception
    mock_client.get.side_effect = Exception("Database error")

    # Call method
    harvester._get_guids_in_db()

    # Verify guids_in_db for failed entities is empty
    for concept_type in harvester.concept_types:
        assert harvester.guids_in_db[concept_type] == []


def test_gather_concept_guid(harvester):
    """Test _gather_concept_guid method"""
    # Setup test data
    concept_dict = {"uri": "http://example.com/dataset1", "name": "Test Dataset"}
    concept_type = "dataset"

    # Call method
    harvester._gather_concept_guid(concept_dict, concept_type)

    # Verify guid was added to guids_in_harvest
    assert "http://example.com/dataset1" in harvester.guids_in_harvest[concept_type]


def test_get_guid(harvester):
    """Test _get_guid method with various inputs"""
    # Test with URI
    dataset_with_uri = {"uri": "http://example.com/dataset1"}
    assert harvester._get_guid(dataset_with_uri) == "http://example.com/dataset1"

    # Test with identifier
    dataset_with_identifier = {"identifier": "dataset-123"}
    assert harvester._get_guid(dataset_with_identifier) == "dataset-123"

    # Test with name and source_url
    dataset_with_name = {"name": "test-dataset"}
    assert harvester._get_guid(dataset_with_name, source_url="http://example.com") == "http://example.com/test-dataset"

    # Test with just name
    assert harvester._get_guid(dataset_with_name) == "test-dataset"


@patch.object(DCATRDFHarvester, '_fetch_concept')
def test_fetch_stage(mock_fetch_concept, harvester):
    """Test fetch_stage method"""
    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    # Setup mock
    mock_fetch_concept.return_value = harvest_object

    # Call method
    result = harvester.fetch_stage(harvest_object)

    # Verify
    mock_fetch_concept.assert_called_once_with(harvest_object)
    assert result == harvest_object


def test_fetch_concept_with_name_and_id(harvester):
    """Test _fetch_concept when concept has name and id"""

    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "existing-name",
        "id": "existing-id",
        "title": "Test Dataset"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        result = harvester._fetch_concept(harvest_object)

        # Verify name and id were not changed
        content = json.loads(result.content)
        assert content["name"] == "existing-name"
        assert content["id"] == "existing-id"


def test_fetch_concept_without_name(harvester):
    """Test _fetch_concept uses title as name when name is missing"""

    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    mock_concept = {
        "uri": "http://example.com/dataset1",
        "id": "existing-id",
        "title": "Test Dataset"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        result = harvester._fetch_concept(harvest_object)

        # Verify name was set to title
        content = json.loads(result.content)
        assert content["name"] == "Test Dataset"


def test_fetch_concept_without_id(harvester):
    """Test _fetch_concept generates id when missing"""

    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-name",
        "title": "Test Dataset"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        result = harvester._fetch_concept(harvest_object)

        # Verify id was generated from guid
        content = json.loads(result.content)
        expected_id = munge_title_to_name("http://example.com/dataset1")
        assert content["id"] == expected_id


def test_fetch_concept_dataset_without_biobank_auto_create_enabled(harvester):
    """Test _fetch_concept tracks dataset without biobank when auto_create enabled"""
    # Enable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': True}

    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-dataset",
        "id": "test-id",
        "title": "Test Dataset",
        "description": "Test description"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester._fetch_concept(harvest_object)

        # Verify dataset was tracked
        assert len(harvester._datasets_without_datasetseries) == 1
        tracked = harvester._datasets_without_datasetseries[0]
        assert tracked['dataset_name'] == 'test-dataset'
        assert tracked['dataset_id'] == 'test-id'
        assert tracked['dataset_description'] == 'Test description'
        assert tracked['dataset_guid'] == 'http://example.com/dataset1'


def test_fetch_concept_dataset_with_biobank_no_tracking(harvester):
    """Test _fetch_concept does not track dataset with existing biobank"""

    # Enable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': True}

    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-dataset",
        "id": "test-id",
        "biobank": "existing-biobank-id"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester._fetch_concept(harvest_object)

        # Verify dataset was NOT tracked
        assert len(harvester._datasets_without_datasetseries) == 0


def test_fetch_concept_dataset_auto_create_disabled(harvester):
    """Test _fetch_concept does not track when auto_create disabled"""

    # Disable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': False}

    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.concept_type = "dataset"

    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-dataset",
        "id": "test-id"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester._fetch_concept(harvest_object)

        # Verify dataset was NOT tracked
        assert len(harvester._datasets_without_datasetseries) == 0


def test_fetch_concept_non_dataset_not_tracked(harvester):
    """Test _fetch_concept does not track non-dataset concepts"""

    # Enable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': True}

    # Setup test data for person
    harvest_object = HarvestObject(guid="http://example.com/person1", content=None)
    harvest_object.concept_type = "person"

    mock_concept = {
        "uri": "http://example.com/person1",
        "name": "test-person",
        "id": "test-id"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester._fetch_concept(harvest_object)

        # Verify person was NOT tracked
        assert len(harvester._datasets_without_datasetseries) == 0


def test_get_guid_no_identifiers(harvester):
    """Test _get_guid returns None when no identifiers present"""
    dataset_without_identifiers = {"title": "Some Title"}
    assert harvester._get_guid(dataset_without_identifiers) is None


def test_get_guid_priority(harvester):
    """Test _get_guid respects priority: uri > identifier > name"""
    # When all three are present, uri should be used
    dataset_all = {
        "uri": "http://example.com/uri",
        "identifier": "some-identifier",
        "name": "some-name"
    }
    assert harvester._get_guid(dataset_all) == "http://example.com/uri"

    # When uri missing, identifier should be used
    dataset_no_uri = {
        "identifier": "some-identifier",
        "name": "some-name"
    }
    assert harvester._get_guid(dataset_no_uri) == "some-identifier"


def test_create_datasetseries_for_dataset(harvester):
    """Test _create_datasetseries_for_dataset creates correct HarvestObject"""
    dataset_info = {
        'dataset_name': 'test-dataset',
        'dataset_id': 'test-id',
        'dataset_description': 'Test description',
        'dataset_guid': 'http://example.com/dataset1'
    }

    datasetseries_object, datasetseries_id = harvester._create_datasetseries_for_dataset(dataset_info)

    # Verify HarvestObject properties
    assert isinstance(datasetseries_object, HarvestObject)
    assert datasetseries_object.guid == 'http://example.com/dataset1_datasetseries'
    assert datasetseries_object.status == 'new'
    assert datasetseries_object.concept_type == 'datasetseries'

    # Verify content
    content = json.loads(datasetseries_object.content)
    assert content['id'] == 'test-id'
    assert content['name'] == 'test-dataset'
    assert content['description'] == 'Test description'
    assert content['concept_type'] == 'datasetseries'

    # Verify returned id
    assert datasetseries_id == 'test-id'


def test_generate_missing_datasetseries_empty_list(harvester):
    """Test generate_missing_datasetseries returns early when no datasets tracked"""
    # Ensure list is empty
    harvester._datasets_without_datasetseries = []

    # Call method
    harvester.generate_missing_datasetseries()

    # Verify no objects were added
    assert len(harvester._harvest_objects) == 0


def test_generate_missing_datasetseries_creates_objects(harvester):
    """Test generate_missing_datasetseries creates datasetseries objects"""
    # Setup tracked datasets
    harvester._datasets_without_datasetseries = [
        {
            'dataset_name': 'dataset1',
            'dataset_id': 'id1',
            'dataset_description': 'Description 1',
            'dataset_guid': 'http://example.com/dataset1'
        },
        {
            'dataset_name': 'dataset2',
            'dataset_id': 'id2',
            'dataset_description': 'Description 2',
            'dataset_guid': 'http://example.com/dataset2'
        }
    ]

    # Add corresponding dataset HarvestObjects
    dataset1 = HarvestObject(guid='http://example.com/dataset1', content=json.dumps({'name': 'dataset1'}))
    dataset1.concept_type = 'dataset'
    dataset2 = HarvestObject(guid='http://example.com/dataset2', content=json.dumps({'name': 'dataset2'}))
    dataset2.concept_type = 'dataset'
    harvester._harvest_objects = [dataset1, dataset2]

    # Call method
    harvester.generate_missing_datasetseries()

    # Verify datasetseries objects were created
    datasetseries_objects = [obj for obj in harvester._harvest_objects if obj.concept_type == 'datasetseries']
    assert len(datasetseries_objects) == 2

    # Verify first datasetseries
    assert datasetseries_objects[0].guid == 'http://example.com/dataset1_datasetseries'
    content1 = json.loads(datasetseries_objects[0].content)
    assert content1['name'] == 'dataset1'
    assert content1['id'] == 'id1'

    # Verify second datasetseries
    assert datasetseries_objects[1].guid == 'http://example.com/dataset2_datasetseries'
    content2 = json.loads(datasetseries_objects[1].content)
    assert content2['name'] == 'dataset2'
    assert content2['id'] == 'id2'


def test_generate_missing_datasetseries_updates_datasets(harvester):
    """Test generate_missing_datasetseries updates dataset biobank references"""
    # Setup tracked dataset
    harvester._datasets_without_datasetseries = [{
        'dataset_name': 'test-dataset',
        'dataset_id': 'test-id',
        'dataset_description': 'Test description',
        'dataset_guid': 'http://example.com/dataset1'
    }]

    # Add corresponding dataset HarvestObject
    dataset = HarvestObject(
        guid='http://example.com/dataset1',
        content=json.dumps({'name': 'test-dataset', 'title': 'Test Dataset'})
    )
    dataset.concept_type = 'dataset'
    harvester._harvest_objects = [dataset]

    # Call method
    harvester.generate_missing_datasetseries()

    # Verify dataset was updated with biobank reference
    dataset_content = json.loads(dataset.content)
    assert 'biobank' in dataset_content
    assert dataset_content['biobank'] == 'test-id'


def test_generate_missing_datasetseries_logs_correctly(harvester):
    """Test generate_missing_datasetseries logs creation messages"""
    # Setup tracked datasets
    harvester._datasets_without_datasetseries = [{
        'dataset_name': 'dataset1',
        'dataset_id': 'id1',
        'dataset_description': 'Description 1',
        'dataset_guid': 'http://example.com/dataset1'
    }]

    # Add corresponding dataset HarvestObject
    dataset = HarvestObject(guid='http://example.com/dataset1', content=json.dumps({'name': 'dataset1'}))
    dataset.concept_type = 'dataset'
    harvester._harvest_objects = [dataset]

    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        harvester.generate_missing_datasetseries()

        # Verify log messages
        assert mock_log.info.call_count == 2
        mock_log.info.assert_any_call('Auto-generating 1 datasetseries for datasets without them')
        mock_log.info.assert_any_call('Successfully created 1 auto-generated datasetseries')


@patch('json.loads')
def test_import_stage_success(mock_json_loads, harvester, mock_client):
    """Test successful import_stage"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1", 
        content=json.dumps({"name": "Test Dataset", "concept_type": "dataset"})
    )
    harvest_object.status = "new"

    # Setup mock
    mock_json_loads.return_value = {"name": "Test Dataset", "concept_type": "dataset"}

    # Call method
    result = harvester.import_stage(harvest_object)

    # Verify
    mock_client.save_schema.assert_called_once_with(
        table="datasets", 
        data=[{"name": "Test Dataset", "concept_type": "dataset"}]
    )
    assert result


def test_import_stage_delete(harvester):
    """Test import_stage with delete status"""
    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.status = "delete"

    # Call method
    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.warning.assert_called_once()
        assert result


def test_import_stage_empty_content(harvester):
    """Test import_stage with empty content"""
    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None)
    harvest_object.status = "new"

    # Call method
    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.error.assert_called_once()
        assert not result


def test_import_stage_invalid_json(harvester):
    """Test import_stage with invalid JSON content"""
    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content="invalid json")
    harvest_object.status = "new"

    # Call method
    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.error.assert_called_once()
        assert not result


def test_import_stage_client_error(harvester, mock_client):
    """Test import_stage with client error"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Test Dataset", "concept_type": "dataset"})
    )
    harvest_object.status = "new"

    # Setup mock
    mock_client.save_schema.side_effect = Exception("Database error")

    # Call method
    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.error.assert_called_once()
        assert not result


def test_import_stage_change_status(harvester, mock_client):
    """Test import_stage with change status"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Updated Dataset", "concept_type": "dataset"})
    )
    harvest_object.status = "change"

    # Call method
    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_client.save_schema.assert_called_once_with(
            table="datasets",
            data=[{"name": "Updated Dataset", "concept_type": "dataset"}]
        )
        mock_log.info.assert_called_once()
        assert "Updating dataset" in mock_log.info.call_args[0][0]
        assert result


def test_import_stage_correct_table_mapping(harvester, mock_client):
    """Test import_stage uses correct table name for each concept type"""
    # Test dataset
    dataset_obj = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Dataset", "concept_type": "dataset"})
    )
    dataset_obj.status = "new"
    harvester.import_stage(dataset_obj)
    mock_client.save_schema.assert_called_with(table="datasets", data=[{"name": "Dataset", "concept_type": "dataset"}])

    # Test datasetseries
    mock_client.reset_mock()
    series_obj = HarvestObject(
        guid="http://example.com/series1",
        content=json.dumps({"name": "Series", "concept_type": "datasetseries"})
    )
    series_obj.status = "new"
    harvester.import_stage(series_obj)
    mock_client.save_schema.assert_called_with(table="datasetseries", data=[{"name": "Series", "concept_type": "datasetseries"}])

    # Test person
    mock_client.reset_mock()
    person_obj = HarvestObject(
        guid="http://example.com/person1",
        content=json.dumps({"name": "Person", "concept_type": "person"})
    )
    person_obj.status = "new"
    harvester.import_stage(person_obj)
    mock_client.save_schema.assert_called_with(table="persons", data=[{"name": "Person", "concept_type": "person"}])


def test_import_stage_logs_adding_for_new(harvester, mock_client):
    """Test import_stage logs 'Adding dataset' for new status"""
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "New Dataset", "concept_type": "dataset"})
    )
    harvest_object.status = "new"

    with patch('molgenis_fdp_harvester.rdf.log') as mock_log:
        harvester.import_stage(harvest_object)

        mock_log.info.assert_called_once()
        assert "Adding dataset" in mock_log.info.call_args[0][0]
        assert "New Dataset" in mock_log.info.call_args[0][0]
