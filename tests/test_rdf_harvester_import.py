import json
import logging

from molgenis_fdp_harvester.base.baseharvester import HarvestObject


def test_import_stage_success(harvester, mock_client):
    """Test successful import_stage"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Test Dataset"}),
        concept_type="dataset",
        status="new",
    )

    # Call method
    result = harvester.import_stage(harvest_object)

    # Verify
    mock_client.save_table.assert_called_once_with(table="datasets", data=[{"name": "Test Dataset"}])
    assert result


def test_import_stage_empty_content(harvester, caplog):
    """Test import_stage with empty content"""
    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None, status="new")

    # Call method
    with caplog.at_level(logging.ERROR):
        result = harvester.import_stage(harvest_object)

    # Verify: the failure is both logged and recorded on the harvester
    assert not result
    assert len(harvester._import_errors) == 1
    assert harvester.has_errors
    assert "Empty content" in caplog.text


def test_import_stage_client_error(harvester, mock_client, caplog):
    """Test import_stage with client error"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Test Dataset"}),
        concept_type="dataset",
        status="new",
    )
    # Setup mock
    mock_client.save_table.side_effect = Exception("Database error")

    # Call method
    with caplog.at_level(logging.ERROR):
        result = harvester.import_stage(harvest_object)

    # Verify: the failure is both logged and recorded on the harvester
    assert not result
    assert len(harvester._import_errors) == 1
    assert harvester.has_errors
    assert "Database error" in caplog.text


def test_import_stage_change_status(harvester, mock_client, caplog):
    """Test import_stage with change status"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Updated Dataset"}),
        concept_type="dataset",
        status="change",
    )

    # Call method
    with caplog.at_level(logging.INFO):
        result = harvester.import_stage(harvest_object)

    # Verify
    mock_client.save_table.assert_called_once_with(table="datasets", data=[{"name": "Updated Dataset"}])
    assert "Updating dataset Updated Dataset" in caplog.text
    assert result


def test_import_stage_logs_adding_for_new(harvester, caplog):
    """Test import_stage logs 'Adding dataset' for new status"""
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"title": "New Dataset"}),
        concept_type="dataset",
        status="new",
    )

    with caplog.at_level(logging.INFO):
        harvester.import_stage(harvest_object)

    assert "Adding dataset New Dataset" in caplog.text


def test_import_stage_does_not_re_report_a_fetch_failure(harvester, caplog):
    """A record already reported in the fetch stage must not be counted a second time."""
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1", content=None, concept_type="dataset", status="new"
    )
    harvest_object.fetch_failed = True

    with caplog.at_level(logging.WARNING):
        result = harvester.import_stage(harvest_object)

    assert not result
    assert harvester._import_errors == []
    assert not harvester.has_errors
    assert caplog.records == []
