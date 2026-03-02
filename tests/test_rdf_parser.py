# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest

import rdflib
from rdflib import URIRef

from molgenis_fdp_harvester.base.processor import RDFParser
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from molgenis_fdp_harvester.utils import HarvesterException


TEST_PROFILE_CONFIG = {'pid_service_url': 'https://pid.example.com', 'fdp_id_prefix': 'testorg'}


class ConfiguredProfile(MolgenisEUCAIMDCATAPProfile):
    """Profile subclass with test config pre-set so handle_pids() can run."""
    def __init__(self, graph):
        super().__init__(graph)
        self.config = TEST_PROFILE_CONFIG


@pytest.fixture
def profiles():
    return [ConfiguredProfile]


@pytest.fixture
def parser(profiles):
    return RDFParser(profiles)


@pytest.fixture
def catalog_data():
    with open("tests/test_data/rdf_catalog.ttl", "r") as f:
        return f.read()


@pytest.fixture
def dataset1_data():
    with open("tests/test_data/rdf_dataset1.ttl", "r") as f:
        return f.read()


@pytest.fixture
def dataset2_data():
    with open("tests/test_data/rdf_dataset2.ttl", "r") as f:
        return f.read()

def test_parser_initialization(profiles):
    """Test that the parser initializes correctly with profiles"""
    parser = RDFParser(profiles)
    assert parser._profiles == profiles
    assert isinstance(parser.g, rdflib.ConjunctiveGraph)


def test_parse_catalog_ttl(parser, catalog_data):
    """Test parsing a Turtle catalog file"""
    parser.parse(data=catalog_data, _format="turtle")

    # Verify catalog parsed correctly
    catalogs = list(parser._catalogs())
    assert len(catalogs) == 1
    assert str(catalogs[0]) == "http://example.com/catalog"

    # Verify datasets referenced in catalog
    datasets_in_catalog = list(parser.dataset_in_catalog())
    assert len(datasets_in_catalog) == 2
    dataset_uris = [str(d) for d in datasets_in_catalog]
    assert "http://example.com/dataset1" in dataset_uris
    assert "http://example.com/dataset2" in dataset_uris

def test_parse_dataset_ttl(parser, dataset1_data, dataset2_data):
    """Test parsing dataset Turtle files"""
    # Parse both datasets
    parser.parse(data=dataset1_data, _format="turtle")
    parser.parse(data=dataset2_data, _format="turtle")

    # Verify datasets parsed correctly
    datasets = list(parser._datasets())
    assert len(datasets) == 2
    dataset_uris = [str(d) for d in datasets]
    assert "http://example.com/dataset1" in dataset_uris
    assert "http://example.com/dataset2" in dataset_uris


def test_datasets_generator(parser, dataset1_data, dataset2_data):
    """Test that the datasets() generator produces correct dataset dicts"""
    # Parse datasets
    parser.parse(data=dataset1_data, _format="turtle")
    parser.parse(data=dataset2_data, _format="turtle")

    # Get dataset dicts
    dataset_dicts = list(parser.datasets())
    assert len(dataset_dicts) == 2

    # Check dataset dicts have required fields
    for dataset in dataset_dicts:
        assert 'uri' in dataset
        assert 'title' in dataset
        assert 'description' in dataset
        assert dataset['concept_type'] == 'dataset'

    # Verify specific dataset content
    gryffindor = next(d for d in dataset_dicts if d['title'] == "Gryffindor research project")
    assert gryffindor['uri'] == "http://example.com/dataset1"
    assert gryffindor['description'] == "Impact of muggle technical inventions on word's magic presense"

    slytherin = next(d for d in dataset_dicts if d['title'] == "Slytherin research project")
    assert slytherin['uri'] == "http://example.com/dataset2"
    assert slytherin['description'] == "Comarative analysis of magic powers of muggle-born and blood wizards "

def test_get_concept(parser, dataset1_data):
    """Test retrieving a specific concept by URI"""
    # Parse datasets
    parser.parse(data=dataset1_data, _format="turtle")

    # Get concept by URI
    dataset_uri = URIRef("http://example.com/dataset1")
    concept = parser.get_concept(dataset_uri, 'dataset')

    # Verify concept fields
    assert concept['uri'] == "http://example.com/dataset1"
    assert concept['title'] == "Gryffindor research project"


def test_parse_invalid_data(parser):
    """Test handling of invalid RDF data"""
    invalid_data = "This is not valid RDF data"

    with pytest.raises(HarvesterException):
        parser.parse(data=invalid_data, _format="turtle")


def test_supported_formats(parser):
    """Test the supported_formats method returns a list of formats"""
    formats = parser.supported_formats()
    assert isinstance(formats, list)
    assert 'turtle' in formats


def test_publisher_generator(parser):
    """publisher() yields dicts with concept_type 'publisher' for FOAF.Organization resources."""
    with open("tests/test_data/extraction_foaf_organization.ttl", "r") as f:
        parser.parse(data=f.read(), _format="turtle")

    publishers = list(parser.publisher())
    assert len(publishers) == 1
    assert publishers[0]['concept_type'] == 'publisher'
    assert publishers[0]['name'] == 'Test Publisher Org'


def test_kind_generator(parser):
    """kind() yields dicts with concept_type 'kind' for VCARD.Kind resources."""
    with open("tests/test_data/extraction_vcard_contact.ttl", "r") as f:
        parser.parse(data=f.read(), _format="turtle")

    kinds = list(parser.kind())
    assert len(kinds) == 1
    assert kinds[0]['concept_type'] == 'kind'
    assert kinds[0]['fn'] == 'John Doe Contact'


def test_provenancestatement_generator(parser):
    """provenancestatement() yields dicts with concept_type 'provenancestatement'."""
    with open("tests/test_data/extraction_provenancestatement.ttl", "r") as f:
        parser.parse(data=f.read(), _format="turtle")

    provs = list(parser.provenancestatement())
    assert len(provs) == 1
    assert provs[0]['concept_type'] == 'provenancestatement'
    assert provs[0]['label'] == 'Data collected from hospital records'


def test_get_concept_publisher(parser):
    """get_concept() with type 'publisher' returns a dict with publisher fields."""
    with open("tests/test_data/extraction_foaf_organization.ttl", "r") as f:
        parser.parse(data=f.read(), _format="turtle")

    publisher_uri = URIRef("http://example.com/org1")
    concept = parser.get_concept(publisher_uri, 'publisher')

    assert concept['uri'] == "http://example.com/org1"
    assert concept['name'] == "Test Publisher Org"


def test_get_concept_kind(parser):
    """get_concept() with type 'kind' returns a dict with kind fields."""
    with open("tests/test_data/extraction_vcard_contact.ttl", "r") as f:
        parser.parse(data=f.read(), _format="turtle")

    kind_uri = URIRef("http://example.com/contact1")
    concept = parser.get_concept(kind_uri, 'kind')

    assert concept['uri'] == "http://example.com/contact1"
    assert concept['fn'] == "John Doe Contact"


def test_get_concept_provenancestatement(parser):
    """get_concept() with type 'provenancestatement' returns a dict with provenance fields."""
    with open("tests/test_data/extraction_provenancestatement.ttl", "r") as f:
        parser.parse(data=f.read(), _format="turtle")

    prov_uri = URIRef("http://example.com/prov1")
    concept = parser.get_concept(prov_uri, 'provenancestatement')

    assert concept['uri'] == "http://example.com/prov1"
    assert concept['label'] == "Data collected from hospital records"
