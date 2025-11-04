# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-only

from pathlib import Path

import pytest
import requests_mock
from pytest_mock import class_mocker, mocker
from rdflib import Graph

from molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point_record_provider import (
    FairDataPointRecordProvider,
)

TEST_DATA_DIRECTORY = Path("tests/test_data")


def get_graph_by_id(*args, **kwargs):
    file_id = args[0]
    file_id = "_".join(file_id.split("/")[-2:])
    path_to_file = Path(TEST_DATA_DIRECTORY, f"{file_id}.ttl")
    return Graph().parse(path_to_file)


class TestRecordProvider:
    fdp_record_provider = FairDataPointRecordProvider("http://test_end_point.com")

    @pytest.mark.parametrize(
        "fdp_response_file,expected",
        [
            (
                    Path(TEST_DATA_DIRECTORY, "root_fdp_response.ttl"),
                    {
                        'datasetseries=http://example.org/DatasetSeries1', 'dataset=http://example.org/Dataset1'
                    }
            ),
            (
                    Path(TEST_DATA_DIRECTORY, "root_fdp_response_no_catalogs.ttl"),
                    set()
            ),
        ],
    )
    def test_get_record_ids(self, mocker, fdp_response_file, expected):
        fdp_get_graph = mocker.MagicMock(name="get_data")
        mocker.patch(
            "molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point.FairDataPoint.get_graph",
            new=fdp_get_graph,
        )
        fdp_get_graph.return_value = Graph().parse(fdp_response_file)

        actual = self.fdp_record_provider.get_record_ids()

        actual = set(actual)

        assert actual == expected

    @pytest.mark.parametrize(
        "fdp_response_file,expected",
        [
            (
                    Path(TEST_DATA_DIRECTORY, "fdp_multiple_parents.ttl"),
                    {
                        'datasetseries=http://example.org/Dataseries1',
                        'dataset=http://example.org/Dataset1'
                    },
            )
        ],
    )
    def test_get_record_ids_multiple_parents(self, mocker, fdp_response_file, expected):
        fdp_get_graph = mocker.MagicMock(name="get_data")
        mocker.patch(
            "molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point.FairDataPoint.get_graph",
            new=fdp_get_graph,
        )
        fdp_get_graph.return_value = Graph().parse(fdp_response_file)
        actual = self.fdp_record_provider.get_record_ids()

        actual = set(actual)
        assert actual == expected


    def test_get_record_ids_pass_none(self, mocker):
        with pytest.raises(
                ValueError, match="rdf_graph cannot be None"):
            fdp_get_graph = mocker.MagicMock(name="get_data")
            mocker.patch(
                "molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point.FairDataPoint.get_graph",
                new=fdp_get_graph,
            )
            fdp_get_graph.return_value = None
            self.fdp_record_provider.get_record_ids()

    def test_get_record_by_id(self, mocker):
        """A dataset with no distributions"""
        fdp_get_graph = mocker.MagicMock(name="get_data")
        mocker.patch(
            "molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point.FairDataPoint.get_graph",
            new=fdp_get_graph,
        )
        guid = (
            "catalog=https://fair.healthinformationportal.eu/catalog/1c75c2c9-d2cc-44cb-aaa8-cf8c11515c8d;"
            "dataset=https://fair.healthinformationportal.eu/dataset/898ca4b8-197b-4d40-bc81-d9cd88197670"
        )
        fdp_get_graph.side_effect = get_graph_by_id
        actual = self.fdp_record_provider.get_record_by_id(guid)
        expected = (
            Graph()
            .parse(
                Path(
                    TEST_DATA_DIRECTORY,
                    "dataset_898ca4b8-197b-4d40-bc81-d9cd88197670.ttl",
                )
            )
            .serialize()
        )
        assert actual == expected
