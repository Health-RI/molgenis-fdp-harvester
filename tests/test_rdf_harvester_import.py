import json
from unittest.mock import call, patch

import pytest

from molgenis_fdp_harvester.base.baseharvester import HarvestObject


def test_upsert_other_identifier_table_saves_record(harvester, mock_client):
    """Save other identifier rows when notation is provided."""
    harvester._upsert_other_identifier_table("ABC123", "https://example.org")

    mock_client.save_table.assert_called_once_with(
        table="other_identifier",
        data=[{"notation": "ABC123", "schemaAgency": "https://example.org"}],
    )


def test_upsert_other_identifier_table_skips_empty_notation(harvester, mock_client):
    """Do not save when the notation is empty."""
    harvester._upsert_other_identifier_table("", "https://example.org")

    mock_client.save_table.assert_not_called()


def test_check_previous_import_detects_same_agency(harvester, mock_client):
    """Return the existing record id and agency match status."""
    mock_client.get.return_value = [{"id": "dataset-42", "other_identifier": "https://example.org"}]

    result = harvester._check_previous_import(
        {"title": "Dataset"},
        "https://example.org",
        "ABC123",
    )

    assert result == ("dataset-42", True)
    mock_client.get.assert_called_once_with(
        table="collections",
        query_filter="other_identifier.notation == ABC123",
    )


def test_check_previous_import_detects_different_agency(harvester, mock_client):
    """Return False when an existing collection belongs to another agency."""
    mock_client.get.return_value = [{"id": "dataset-42", "other_identifier": "https://other.org"}]

    result = harvester._check_previous_import(
        {"title": "Dataset"},
        "https://example.org",
        "ABC123",
    )

    assert result == ("dataset-42", False)


def test_upsert_collections_creates_new_record_and_other_identifier(harvester, mock_client):
    """New collections should save the other_identifier row and the collection record."""
    dataset = {"title": "Dataset A", "name": "dataset-a"}
    mock_client.get.return_value = []

    result = harvester._upsert_collections(
        dataset,
        agency="https://example.org",
        dataset_name="Dataset A",
        other_identifier_notation="ABC123",
    )

    assert result is True
    assert mock_client.save_table.call_args_list == [
        call(
            table="other_identifier",
            data=[{"notation": "ABC123", "schemaAgency": "https://example.org"}],
        ),
        call(table="collections", data=[dataset]),
    ]


def test_upsert_collections_skips_update_for_different_agency(harvester, mock_client):
    """A collection from another agency should not be overwritten."""
    dataset = {"title": "Dataset A", "name": "dataset-a"}
    mock_client.get.return_value = [{"id": "dataset-41", "other_identifier": "https://other.org"}]

    result = harvester._upsert_collections(
        dataset,
        agency="https://example.org",
        dataset_name="Dataset A",
        other_identifier_notation="ABC123",
    )

    assert result is False
    mock_client.save_table.assert_not_called()


@pytest.mark.parametrize(
    ("status", "expected_calls"),
    [
        ("new", [call(table="datasets", data=[{"title": "Dataset A"}])]),
        ("change", [call(table="datasets", data=[{"title": "Dataset A"}])]),
    ],
)
def test_upsert_table_saves_dataset_for_status(harvester, mock_client, status, expected_calls):
    """The table helper delegates to save_table for both new and change records."""
    dataset = {"title": "Dataset A"}

    result = harvester._upsert_table(
        dataset,
        status=status,
        entity_name="datasets",
        dataset_name="Dataset A",
    )

    assert result is True
    assert mock_client.save_table.call_args_list == expected_calls


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


def test_import_stage_empty_content(harvester):
    """Test import_stage with empty content"""
    # Setup test data
    harvest_object = HarvestObject(guid="http://example.com/dataset1", content=None, status="new")

    # Call method
    with patch("molgenis_fdp_harvester.rdf_harvester.rdf.log") as mock_log:
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
        status="new",
    )
    # Setup mock
    mock_client.save_table.side_effect = Exception("Database error")

    # Call method
    with patch("molgenis_fdp_harvester.rdf_harvester.rdf.log") as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_log.exception.assert_called_once()
        assert not result


def test_import_stage_change_status(harvester, mock_client):
    """Test import_stage with change status"""
    # Setup test data
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"name": "Updated Dataset"}),
        concept_type="dataset",
        status="change",
    )

    # Call method
    with patch("molgenis_fdp_harvester.rdf_harvester.rdf.log") as mock_log:
        result = harvester.import_stage(harvest_object)

        # Verify
        mock_client.save_table.assert_called_once_with(table="datasets", data=[{"name": "Updated Dataset"}])
        mock_log.info.assert_called_once()
        assert "Updating dataset" in mock_log.info.call_args[0][0]
        assert result


def test_import_stage_logs_adding_for_new(harvester):
    """Test import_stage logs 'Adding dataset' for new status"""
    harvest_object = HarvestObject(
        guid="http://example.com/dataset1",
        content=json.dumps({"title": "New Dataset"}),
        concept_type="dataset",
        status="new",
    )

    with patch("molgenis_fdp_harvester.rdf_harvester.rdf.log") as mock_log:
        harvester.import_stage(harvest_object)

        mock_log.info.assert_called_once()
        assert "Adding dataset" in mock_log.info.call_args[0][0]
        assert "New Dataset" in mock_log.info.call_args[0][0]
