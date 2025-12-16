# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from unittest.mock import patch

from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester
from molgenis_fdp_harvester.base.baseharvester import HarvestObject
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

    dataset_objects = [obj for obj in result if obj.concept_type == 'dataset']
    dataset_guids = {obj.guid for obj in dataset_objects}
    person_objects = [obj for obj in result if obj.concept_type == 'person']

    # Verify objects were created
    assert len(result) == 3
    assert all(isinstance(obj, HarvestObject) for obj in result)
    assert all(obj.status == "new" for obj in result)

    # Check dataset objects
    assert len(dataset_objects) == 2
    assert 'http://example.com/dataset1' in dataset_guids
    assert 'http://example.com/dataset2' in dataset_guids

    # Check person objects
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

    # Test with name and source_url
    dataset_with_name = {"name": "test-dataset"}
    assert harvester._get_guid(dataset_with_name, source_url="http://example.com") == "http://example.com/test-dataset"

