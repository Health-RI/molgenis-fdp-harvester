# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from unittest.mock import MagicMock, patch

import pytest

from molgenis_fdp_harvester.fdp_harvester.fdp import FDPHarvester
from molgenis_fdp_harvester.utils import HarvesterException


@pytest.fixture
def fdp_harvester(profiles, concept_table_dict, mock_client):
    return FDPHarvester(
        profiles=profiles,
        concept_table_dict=concept_table_dict,
        molgenis_client=mock_client,
        harvester_config={"pid_service_url": "https://pid.example.com"},
    )


def test_setup_record_provider(fdp_harvester):
    with patch("molgenis_fdp_harvester.fdp_harvester.fdp.FairDataPointRecordProvider") as mock_provider_cls:
        fdp_harvester.setup_record_provider("https://fdp.example.com")

    mock_provider_cls.assert_called_once_with("https://fdp.example.com")
    assert fdp_harvester.record_provider is mock_provider_cls.return_value


def test_gather_stage_returns_harvest_objects(fdp_harvester):
    with (
        patch.object(fdp_harvester, "setup_record_provider"),
        patch.object(fdp_harvester, "_convert_fdp_to_rdf"),
        patch.object(fdp_harvester, "_gather_stage"),
    ):
        result = fdp_harvester.gather_stage("https://fdp.example.com")

    assert result is fdp_harvester._harvest_objects


def test_gather_stage_raises_harvester_exception_on_error(fdp_harvester):
    with (
        patch.object(fdp_harvester, "setup_record_provider"),
        patch.object(fdp_harvester, "_convert_fdp_to_rdf", side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(HarvesterException, match="Failed to gather objects"):
            fdp_harvester.gather_stage("https://fdp.example.com")


def test_convert_fdp_to_rdf_parses_records(fdp_harvester):
    mock_provider = MagicMock()
    mock_provider.get_record_ids.return_value = ["url=https://fdp.example.com/dataset/1"]
    mock_provider.get_record_by_id.return_value = "<> a <http://www.w3.org/ns/dcat#Dataset> ."
    fdp_harvester.record_provider = mock_provider

    with patch.object(fdp_harvester.parser, "parse") as mock_parse:
        fdp_harvester._convert_fdp_to_rdf()

    mock_parse.assert_any_call("<> a <http://www.w3.org/ns/dcat#Dataset> .", _format="ttl")


def test_convert_fdp_to_rdf_logs_empty_record(fdp_harvester):
    mock_provider = MagicMock()
    mock_provider.get_record_ids.return_value = ["url=https://fdp.example.com/dataset/1"]
    mock_provider.get_record_by_id.return_value = None
    fdp_harvester.record_provider = mock_provider

    with patch("molgenis_fdp_harvester.fdp_harvester.fdp.log") as mock_log:
        fdp_harvester._convert_fdp_to_rdf()

    mock_log.error.assert_called()
    assert "Empty record" in mock_log.error.call_args[0][0]
