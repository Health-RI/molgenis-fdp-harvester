# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest

import rdflib
from rdflib import URIRef

from molgenis_fdp_harvester.base.processor import RDFParser
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from molgenis_fdp_harvester.utils import HarvesterException


@pytest.fixture
def profiles():
    return [MolgenisEUCAIMDCATAPProfile]


@pytest.fixture
def parser(profiles):
    return RDFParser(profiles)


@pytest.fixture
def catalog_data():
    with open("tests/catalog.ttl", "r") as f:
        return f.read()


@pytest.fixture
def dataset1_data():
    with open("tests/dataset1.ttl", "r") as f:
        return f.read()


@pytest.fixture
def dataset2_data():
    with open("tests/dataset2.ttl", "r") as f:
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
        assert 'name' in dataset
        assert 'description' in dataset
        assert dataset['concept_type'] == 'dataset'

    # Verify specific dataset content
    gryffindor = next(d for d in dataset_dicts if d['name'] == "Gryffindor research project")
    assert gryffindor['uri'] == "http://example.com/dataset1"
    assert gryffindor['description'] == "Impact of muggle technical inventions on word's magic presense"

    slytherin = next(d for d in dataset_dicts if d['name'] == "Slytherin research project")
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
    assert concept['name'] == "Gryffindor research project"
    assert concept['concept_type'] == 'dataset'


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
