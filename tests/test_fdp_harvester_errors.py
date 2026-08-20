"""Failures inside the FDP client must count towards the run's errors.

Without this, an FDP that returns nothing but unparseable records or HTTP errors would be
logged about but still reported as a clean run, and a scheduled harvest would exit 0.
"""

import logging
from unittest.mock import Mock, patch

import pytest
import requests

from molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point import FairDataPoint
from molgenis_fdp_harvester.fdp_harvester.fdp import FDPHarvester


@pytest.fixture
def fdp_harvester(profiles, concept_table_dict, mock_client):
    return FDPHarvester(
        profiles=profiles,
        concept_table_dict=concept_table_dict,
        molgenis_client=mock_client,
        harvester_config={},
    )


def test_unparseable_record_is_recorded_not_just_logged(caplog):
    fdp = FairDataPoint("https://fdp.example.com")

    with patch.object(FairDataPoint, "_get_data", return_value="I am not a graph"), caplog.at_level(logging.ERROR):
        graph = fdp.get_graph("https://fdp.example.com/dataset/1")

    assert len(graph) == 0
    assert len(fdp.errors) == 1
    assert "could not be parsed" in fdp.errors[0]


def test_failed_http_request_is_recorded_not_just_logged():
    fdp = FairDataPoint("https://fdp.example.com")

    with patch("requests.request", side_effect=requests.exceptions.ConnectionError("refused")):
        graph = fdp.get_graph("https://fdp.example.com/dataset/1")

    assert len(graph) == 0
    assert fdp.errors == ["FDP query https://fdp.example.com/dataset/1 was not successful: refused"]


def test_draining_errors_reports_each_one_once():
    fdp = FairDataPoint("https://fdp.example.com")
    fdp.errors = ["first problem", "second problem"]

    assert fdp.drain_errors() == ["first problem", "second problem"]
    assert fdp.drain_errors() == []


def test_fdp_client_errors_count_towards_the_harvest(fdp_harvester):
    """A run against an FDP whose records all fail must not look like a clean run."""
    fdp_harvester.record_provider = Mock()
    fdp_harvester.record_provider.get_record_ids.return_value = []
    fdp_harvester.record_provider.fair_data_point.drain_errors.return_value = [
        "Record from FDP https://fdp.example.com at /dataset/1 could not be parsed: bad syntax"
    ]

    fdp_harvester._convert_fdp_to_rdf()

    assert fdp_harvester.has_errors
    assert fdp_harvester.gather_error_count == 1


def _record_provider_returning(identifier):
    """A record provider that yields one dataset identifier and nothing for other types."""
    record_provider = Mock()
    record_provider.get_record_ids.side_effect = lambda concept_type: [identifier] if concept_type == "dataset" else []
    record_provider.fair_data_point.drain_errors.return_value = []
    return record_provider


def test_empty_record_counts_towards_the_harvest(fdp_harvester):
    fdp_harvester.record_provider = _record_provider_returning("dataset=https://fdp.example.com/1")
    fdp_harvester.record_provider.get_record_by_id.return_value = ""

    fdp_harvester._convert_fdp_to_rdf()

    assert fdp_harvester.gather_error_count == 1
    assert "Empty record" in fdp_harvester._gather_errors[0]


def test_malformed_identifier_counts_towards_the_harvest(fdp_harvester):
    fdp_harvester.record_provider = _record_provider_returning("no-separator-here")

    fdp_harvester._convert_fdp_to_rdf()

    assert fdp_harvester.gather_error_count == 1
    assert "in gather phase" in fdp_harvester._gather_errors[0]
    # a bad identifier does not stop the record provider being asked for the other types
    assert fdp_harvester.record_provider.get_record_ids.call_count == len(fdp_harvester.concept_types)


def test_gather_stage_failure_is_not_logged_twice(fdp_harvester, caplog):
    """gather_stage re-raises for the caller to log; logging it here too would duplicate it."""
    fdp_harvester.record_provider = Mock()
    fdp_harvester.record_provider.get_record_ids.side_effect = RuntimeError("boom")

    with (
        caplog.at_level(logging.ERROR),
        patch.object(fdp_harvester, "setup_record_provider"),
        pytest.raises(Exception, match="Failed to gather objects"),
    ):
        fdp_harvester.gather_stage("https://fdp.example.com")

    assert caplog.records == []
