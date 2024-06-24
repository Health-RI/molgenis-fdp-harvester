# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from unittest.mock import ANY, Mock, patch
import pytest
import requests
import requests_mock as rm
from urllib.parse import urlparse
import molgenis.client

from molgenis_fdp_harvester.ckan_harvest.dcatrdfharvester import DCATRDFHarvester
from molgenis_fdp_harvester.ckan_harvest.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)

from molgenis_fdp_harvester.harvester import cli

# Will need to patch out requests and molgenis


def custom_matcher(request):
    print(f"Incoming: `{request.url}`")
    parsed_url = urlparse(request.url)
    if parsed_url.hostname == "example.com":
        if parsed_url.path.lstrip("/") in ["catalog", "dataset1", "dataset2"]:
            resp = requests.Response()
            resp.status_code = 200
            resp.headers = {"content-type": "text/turtle"}
            if request.method == "HEAD":
                return resp
            elif request.method == "GET":
                resp.raw = open(f"tests/{parsed_url.path}.ttl", "rb")
                return resp

    return None


@patch("molgenis.client.Session")
def test_harvest_happypath(molgenis_session, requests_mock, cli_runner):
    molgenis_session.return_value.get.return_value = [
        {"id": "http-example-com-dataset2"}
    ]
    molgenis_session.return_value.login.return_value = None
    molgenis_session.add.return_value = None

    requests_mock.add_matcher(custom_matcher)

    cli_runner.invoke(
        cli,
        [
            "--fdp",
            "http://example.com/catalog",
            "--entity",
            "entity",
            "--username",
            "username",
            "--password",
            "password",
            "--host",
            "http://molgenis.example.com",
        ],
    )

    molgenis_session.return_value.add.assert_any_call(
        "entity",
        {
            "uri": "http://example.com/dataset1",
            "name": "Gryffindor research project",
            "description": "Impact of muggle technical inventions on word's magic presense",
            "biobank": "CHAI-4",
            "provider": "CHAIMELEON",
            "order_of_magnitude": 1,
            "country": "EU",
            "collection_method": "OTHER",
            "type": "ORIGINAL_DATASETS",
            "imaging_modality": "MR",
            "image_access_type": "BY_REQUEST",
            "intended_purpose": "placeholder",
            "id": "http-example-com-dataset1",
        },
    )
    molgenis_session.return_value.update_all.assert_any_call(
        "entity",
        [
            {
                "uri": "http://example.com/dataset2",
                "name": "Slytherin research project",
                "description": "Comarative analysis of magic powers of muggle-born and blood wizards ",
                "biobank": "CHAI-4",
                "provider": "CHAIMELEON",
                "order_of_magnitude": 1,
                "country": "EU",
                "collection_method": "OTHER",
                "type": "ORIGINAL_DATASETS",
                "imaging_modality": "MR",
                "image_access_type": "BY_REQUEST",
                "intended_purpose": "placeholder",
                "id": "http-example-com-dataset2",
            }
        ],
    )

    molgenis_session.return_value.get.assert_called_once_with("entity")

    pass
