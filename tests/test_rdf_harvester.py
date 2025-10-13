# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from unittest.mock import Mock, patch
import json

from molgenis_emx2_pyclient import Client
from molgenis_fdp_harvester.rdf import DCATRDFHarvester
from molgenis_fdp_harvester.base.baseharvester import HarvestObject
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile


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
        assert concept_type in harvester._names_taken


@patch.object(DCATRDFHarvester, '_get_rdf')
@patch.object(DCATRDFHarvester, '_load_existing_records')
def test_gather_stage(mock_load_records, mock_get_rdf, harvester, catalog_url):
    """Test the gather_stage method"""
    # Setup parser with mock data
    harvester.parser.parse(open('tests/test_data/rdf_dataset1.ttl').read(), _format='turtle')
    harvester.parser.parse(open('tests/test_data/rdf_dataset2.ttl').read(), _format='turtle')

    # Call gather_stage
    result = harvester.gather_stage(catalog_url)

    # Verify _get_rdf was called
    mock_get_rdf.assert_called_once_with(catalog_url)

    # Verify _load_existing_records was called
    mock_load_records.assert_called_once()

    # Verify harvest objects were created
    assert len(result) > 0
    assert isinstance(result[0], HarvestObject)

def test_load_existing_records(harvester, mock_client):
    """Test _load_existing_records method"""
    # Setup mock client response
    mock_client.get.side_effect = [
        [{"id": "dataset1-id"}, {"id": "dataset2-id"}],  # datasets
        [{"id": "series1-id"}],  # datasetseries
        [{"id": "person1-id"}, {"id": "person2-id"}]  # persons
    ]

    # Call method
    harvester._load_existing_records()

    # Verify client calls
    assert mock_client.get.call_count == 3

    # Verify guids_in_db was populated
    assert harvester.guids_in_db['dataset'] == ["dataset1-id", "dataset2-id"]
    assert harvester.guids_in_db['datasetseries'] == ["series1-id"]
    assert harvester.guids_in_db['person'] == ["person1-id", "person2-id"]


def test_load_existing_records_error_handling(harvester, mock_client):
    """Test error handling in _load_existing_records"""
    # Setup mock client to raise exception
    mock_client.get.side_effect = Exception("Database error")

    # Call method
    harvester._load_existing_records()

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


def test_generate_unique_name(harvester):
    """Test _generate_unique_name method"""
    # Test first name
    name1 = harvester._generate_unique_name("Test Dataset", "dataset")
    assert name1 == "test-dataset"
    assert name1 in harvester._names_taken["dataset"]

    # Test duplicate name
    name2 = harvester._generate_unique_name("Test Dataset", "dataset")
    assert name2 == "test-dataset-1"
    assert name2 in harvester._names_taken["dataset"]

    # Test another duplicate
    name3 = harvester._generate_unique_name("Test Dataset", "dataset")
    assert name3 == "test-dataset-2"
    assert name3 in harvester._names_taken["dataset"]


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
