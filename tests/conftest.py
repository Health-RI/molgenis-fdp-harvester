from unittest.mock import Mock

import pytest
import rdflib

from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.base.baseharvester import HarvestObject
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester


@pytest.fixture
def mock_client():
    return Mock(spec=Client, save_table=Mock())

TEST_HARVESTER_CONFIG = {'pid_service_url': 'https://pid.example.com', 'fdp_id_prefix': 'testorg'}


class _ConfiguredProfile(MolgenisEUCAIMDCATAPProfile):
    """Profile subclass with test PID config pre-set."""
    def __init__(self, graph):
        super().__init__(graph)
        self.config = TEST_HARVESTER_CONFIG


@pytest.fixture
def profiles():
    return [_ConfiguredProfile]


@pytest.fixture
def concept_table_dict():
    return {
        'dataset': 'datasets',
        'datasetseries': 'datasetseries',
        'kind': 'kind',
        'publisher': 'publisher',
        'provenancestatement': 'provenancestatement'
    }


@pytest.fixture
def harvester(profiles, concept_table_dict, mock_client):
    return DCATRDFHarvester(
        profiles=profiles,
        concept_table_dict=concept_table_dict,
        molgenis_client=mock_client,
        harvester_config=TEST_HARVESTER_CONFIG
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


@pytest.fixture
def graph_vcard_contact():
    """Load RDF graph with VCARD contact."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_vcard_contact.ttl", format="turtle")
    return g


@pytest.fixture
def graph_vcard_missing():
    """Load RDF graph without contact field."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_vcard_missing.ttl", format="turtle")
    return g


@pytest.fixture
def graph_foaf_person():
    """Load RDF graph with FOAF Person."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_foaf_person.ttl", format="turtle")
    return g


@pytest.fixture
def graph_foaf_wrong_type():
    """Load RDF graph with wrong type for provider."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_foaf_wrong_type.ttl", format="turtle")
    return g


@pytest.fixture
def graph_date_range():
    """Load RDF graph with date range."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_date_range.ttl", format="turtle")
    return g


@pytest.fixture
def graph_date_range_missing():
    """Load RDF graph without date range."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_date_range_missing.ttl", format="turtle")
    return g


@pytest.fixture
def graph_datasetseries_with_id():
    """Load RDF graph with DatasetSeries having identifier."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_datasetseries_with_id.ttl", format="turtle")
    return g


@pytest.fixture
def graph_datasetseries_no_id():
    """Load RDF graph with DatasetSeries without identifier."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_datasetseries_no_id.ttl", format="turtle")
    return g


@pytest.fixture
def graph_dataset_integration():
    """Load RDF graph for dataset integration test."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_dataset_integration.ttl", format="turtle")
    return g


@pytest.fixture
def graph_person_vcard():
    """Load RDF graph for person with VCARD."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_person_vcard.ttl", format="turtle")
    return g
