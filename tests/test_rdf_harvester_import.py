import json
from unittest.mock import patch

from molgenis_fdp_harvester.base.baseharvester import HarvestObject


def test_import_stage_success(harvester, mock_client):
    """Test successful import_stage"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Test Dataset"}),
        concept_type="dataset",
        status="new"
    )

    # Call method
    result = harvester.import_stage(harvest_object)

    # Verify
    mock_client.save_schema.assert_called_once_with(
        table="datasets",
        data=[{"name": "Test Dataset"}]
    )
    assert result


def test_import_stage_empty_content(harvester):
    """Test import_stage with empty content"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=None,
        status="new"
    )

    # Call method
    with patch('molgenis_fdp_harvester.rdf_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.error.assert_called_once()
        assert not result


def test_import_stage_client_error(harvester, mock_client):
    """Test import_stage with client error"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Test Dataset"}),
        concept_type="dataset",
        status="new"
    )
    # Setup mock
    mock_client.save_schema.side_effect = Exception("Database error")

    # Call method
    with patch('molgenis_fdp_harvester.rdf_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.error.assert_called_once()
        assert not result


def test_import_stage_change_status(harvester, mock_client):
    """Test import_stage with change status"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Updated Dataset"}),
        concept_type="dataset",
        status="change"
    )

    # Call method
    with patch('molgenis_fdp_harvester.rdf_harvester.rdf.log') as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_client.save_schema.assert_called_once_with(
            table="datasets",
            data=[{"name": "Updated Dataset"}]
        )
        mock_log.info.assert_called_once()
        assert "Updating dataset" in mock_log.info.call_args[0][0]
        assert result


def test_import_stage_logs_adding_for_new(harvester, mock_client):
    """Test import_stage logs 'Adding dataset' for new status"""
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "New Dataset"}),
        concept_type="dataset",
        status="new"
    )

    with patch('molgenis_fdp_harvester.rdf_harvester.rdf.log') as mock_log:
        harvester.import_stage(harvest_object)

        mock_log.info.assert_called_once()
        assert "Adding dataset" in mock_log.info.call_args[0][0]
        assert "New Dataset" in mock_log.info.call_args[0][0]
