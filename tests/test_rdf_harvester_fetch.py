import json
from unittest.mock import patch

from molgenis_fdp_harvester.base.baseharvester import HarvestObject, munge_title_to_name
from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester


def test_fetch_concept_with_name_and_id(harvester, empty_harvestobject_dataset):
    """Test fetch_stage when dataset has name and id"""
    # Setup test data
    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "existing-name",
        "id": "existing-id",
        "title": "Test Dataset"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        result = harvester.fetch_stage(empty_harvestobject_dataset)

        # Verify name and id were not changed
        content = json.loads(result.content)
        assert content["name"] == "existing-name"
        assert content["id"] == "existing-id"


def test_fetch_concept_without_name(harvester, empty_harvestobject_dataset):
    """Test fetch_stage uses title as name when name is missing"""
    # Setup test data
    mock_concept = {
        "uri": "http://example.com/dataset1",
        "id": "existing-id",
        "title": "Test Dataset"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        result = harvester.fetch_stage(empty_harvestobject_dataset)

        # Verify name was set to title
        content = json.loads(result.content)
        assert content["name"] == "Test Dataset"


def test_fetch_concept_without_id(harvester, empty_harvestobject_dataset):
    """Test fetch_stage generates id when missing"""
    # Setup test data
    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-name",
        "title": "Test Dataset"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        result = harvester.fetch_stage(empty_harvestobject_dataset)

        # Verify id was generated from guid
        content = json.loads(result.content)
        expected_id = munge_title_to_name("http://example.com/dataset1")
        assert content["id"] == expected_id


def test_fetch_concept_dataset_without_biobank_auto_create_enabled(harvester, empty_harvestobject_dataset):
    """Test fetch_stage tracks dataset without biobank when auto_create enabled"""
    # Enable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': True}

    # Setup test data
    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-dataset",
        "id": "test-id",
        "title": "Test Dataset",
        "description": "Test description"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester.fetch_stage(empty_harvestobject_dataset)
        tracked = harvester._datasets_without_datasetseries[0]

        # Verify dataset was tracked
        assert len(harvester._datasets_without_datasetseries) == 1
        assert tracked['dataset_name'] == 'test-dataset'
        assert tracked['dataset_id'] == 'test-id'
        assert tracked['dataset_description'] == 'Test description'
        assert tracked['dataset_guid'] == 'http://example.com/dataset1'


def test_fetch_concept_dataset_with_biobank_no_tracking(harvester, empty_harvestobject_dataset):
    """Test fetch_stage does not track dataset with existing biobank"""
    # Enable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': True}

    # Setup test data
    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-dataset",
        "id": "test-id",
        "biobank": "existing-biobank-id"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester.fetch_stage(empty_harvestobject_dataset)

        # Verify dataset was NOT tracked
        assert len(harvester._datasets_without_datasetseries) == 0


def test_fetch_concept_dataset_auto_create_disabled(harvester, empty_harvestobject_dataset):
    """Test fetch_stage does not track when auto_create disabled"""

    # Disable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': False}

    # Setup test data
    mock_concept = {
        "uri": "http://example.com/dataset1",
        "name": "test-dataset",
        "id": "test-id"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester.fetch_stage(empty_harvestobject_dataset)

        # Verify dataset was NOT tracked
        assert len(harvester._datasets_without_datasetseries) == 0


def test_fetch_concept_non_dataset_not_tracked(harvester):
    """Test fetch_stage does not track non-dataset concepts"""

    # Enable auto_create_datasetseries
    harvester.harvester_config = {'auto_create_datasetseries': True}

    # Setup test data for person
    harvest_object = HarvestObject(
        guid="http://example.com/person1",
        content=None,
        concept_type="person"
    )

    mock_concept = {
        "uri": "http://example.com/person1",
        "name": "test-person",
        "id": "test-id"
    }

    with patch.object(harvester.parser, 'get_concept', return_value=mock_concept):
        harvester.fetch_stage(harvest_object)

        # Verify person was NOT tracked
        assert len(harvester._datasets_without_datasetseries) == 0


def test_create_datasetseries_for_dataset(harvester):
    """Test _create_datasetseries_for_dataset creates correct HarvestObject"""
    dataset_info = {
        'dataset_name': 'test-dataset',
        'dataset_id': 'test-id',
        'dataset_description': 'Test description',
        'dataset_guid': 'http://example.com/dataset1'
    }

    datasetseries_object, datasetseries_id = harvester._create_datasetseries_for_dataset(dataset_info)
    content = json.loads(datasetseries_object.content)

    # Verify HarvestObject properties
    assert isinstance(datasetseries_object, HarvestObject)
    assert datasetseries_object.guid == 'http://example.com/dataset1_datasetseries'
    assert datasetseries_object.status == 'new'
    assert datasetseries_object.concept_type == 'datasetseries'

    # Verify content
    assert content['id'] == 'test-id'
    assert content['name'] == 'test-dataset'
    assert content['description'] == 'Test description'

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

    datasetseries_objects = [obj for obj in harvester._harvest_objects if obj.concept_type == 'datasetseries']
    content1 = json.loads(datasetseries_objects[0].content)
    content2 = json.loads(datasetseries_objects[1].content)

    # Verify datasetseries objects were created
    assert len(datasetseries_objects) == 2

    # Verify first datasetseries
    assert datasetseries_objects[0].guid == 'http://example.com/dataset1_datasetseries'
    assert content1['name'] == 'dataset1'
    assert content1['id'] == 'id1'

    # Verify second datasetseries
    assert datasetseries_objects[1].guid == 'http://example.com/dataset2_datasetseries'
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

    with patch('molgenis_fdp_harvester.rdf_harvester.rdf.log') as mock_log:
        harvester.generate_missing_datasetseries()

        # Verify log messages
        assert mock_log.info.call_count == 2
        mock_log.info.assert_any_call('Auto-generating 1 datasetseries for datasets without them')
        mock_log.info.assert_any_call('Successfully created 1 auto-generated datasetseries')
