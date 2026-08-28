import logging
from unittest.mock import Mock, patch

from molgenis_fdp_harvester.base.baseharvester import HarvestObject
from molgenis_fdp_harvester.harvester import execute_harvest

CONCEPT_TYPE_ORDER = {
    "provenancestatement": 0,
    "kind": 1,
    "publisher": 2,
    "datasetseries": 3,
    "dataset": 4,
}


def _make_harvester(harvest_objects, gather_errors=0, import_errors=0):
    harvester = Mock()
    harvester._harvest_objects = harvest_objects
    harvester.gather_error_count = gather_errors
    harvester.import_error_count = import_errors
    harvester.has_errors = bool(gather_errors or import_errors)
    return harvester


def test_execute_harvest_returns_true_when_no_errors():
    harvest_objects = [HarvestObject(guid="g1", concept_type="dataset")]
    harvester = _make_harvester(harvest_objects)

    result = execute_harvest(harvester, "http://example.com/fdp", CONCEPT_TYPE_ORDER)

    assert result is True
    harvester.gather_stage.assert_called_once_with("http://example.com/fdp")
    harvester.fetch_stage.assert_called_once_with(harvest_objects[0])
    harvester.import_stage.assert_called_once_with(harvest_objects[0])


def test_execute_harvest_returns_false_when_gather_errors_present():
    harvest_objects = [HarvestObject(guid="g1", concept_type="dataset")]
    harvester = _make_harvester(harvest_objects, gather_errors=1)

    result = execute_harvest(harvester, "http://example.com/fdp", CONCEPT_TYPE_ORDER)

    assert result is False


def test_execute_harvest_returns_false_when_an_import_fails():
    harvest_objects = [
        HarvestObject(guid="g1", concept_type="dataset"),
        HarvestObject(guid="g2", concept_type="publisher"),
    ]
    harvester = _make_harvester(harvest_objects, import_errors=1)

    result = execute_harvest(harvester, "http://example.com/fdp", CONCEPT_TYPE_ORDER)

    assert result is False
    # a failing object does not stop the rest of the import from being attempted
    assert harvester.import_stage.call_count == 2


def test_invalid_record_is_reported_once_across_fetch_and_import(harvester, caplog):
    """A record that fails validation is one error, not one per stage that notices it."""
    harvest_objects = [
        HarvestObject(guid="http://example.com/valid", concept_type="dataset", status="new"),
        HarvestObject(guid="http://example.com/invalid", concept_type="dataset", status="new"),
    ]
    harvester._harvest_objects = harvest_objects
    concepts = {
        "http://example.com/valid": {"uri": "http://example.com/valid", "title": "Valid Dataset"},
        "http://example.com/invalid": {"uri": "http://example.com/invalid"},  # no title
    }

    with (
        patch.object(harvester.parser, "get_concept", side_effect=lambda uri, _type: concepts[str(uri)]),
        patch.object(harvester, "gather_stage"),
        patch.object(harvester, "generate_missing_datasetseries"),
        caplog.at_level(logging.WARNING),
    ):
        result = execute_harvest(harvester, "http://example.com/fdp", CONCEPT_TYPE_ORDER)

    assert result is False
    assert harvester.gather_error_count == 1
    assert harvester.import_error_count == 0
    assert "1 gather/validation error(s), 0 import error(s), 2 object(s) processed" in caplog.text
    # the valid record was still imported
    harvester.molgenis_client.save_table.assert_called_once()


def test_execute_harvest_warns_when_nothing_was_harvested(caplog):
    """An empty harvest is not an error, but it must be visible in the log."""
    harvester = _make_harvester([])

    with caplog.at_level(logging.WARNING):
        result = execute_harvest(harvester, "http://example.com/fdp", CONCEPT_TYPE_ORDER)

    assert result is True
    assert "produced no objects" in caplog.text
