from unittest.mock import Mock

import pytest

from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.base.baseharvester import HarvestObject
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester


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


@pytest.fixture()
def empty_harvestobject_dataset():
    return HarvestObject(
        guid="http://example.com/dataset1",
        content=None,
        concept_type="dataset"
    )
