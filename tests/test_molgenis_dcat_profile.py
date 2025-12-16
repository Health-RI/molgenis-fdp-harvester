# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest

import rdflib
from rdflib import URIRef, Literal
from rdflib.namespace import DCAT, RDF, FOAF, DCTERMS

from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile


@pytest.fixture
def rdf_graph():
    # Create RDF graph
    g = rdflib.Dataset()

    # Load test RDF data
    with open("tests/test_data/rdf_dataset1.ttl", "r") as f:
        dataset1_data = f.read()

    g.parse(data=dataset1_data, format="turtle")
    return g


@pytest.fixture
def profile(rdf_graph):
    return MolgenisEUCAIMDCATAPProfile(rdf_graph)


@pytest.fixture
def dataset_ref():
    return URIRef("http://example.com/dataset1")


def test_parse_dataset(profile, dataset_ref):
    """Test parsing a dataset reference into a dict"""
    dataset_dict = {}
    profile.parse_dataset(dataset_dict, dataset_ref)

    # Verify basic fields
    assert dataset_dict["uri"] == "http://example.com/dataset1"
    assert dataset_dict["name"] == "Gryffindor research project"
    assert dataset_dict["description"] == "Impact of muggle technical inventions on word's magic presense"


def test_extract_concept_dict():
    """Test _extract_concept_dict method"""
    # Create a simple test graph
    test_g = rdflib.Dataset()
    test_uri = URIRef("http://example.com/test")

    # Add some triples
    test_g.add((test_uri, DCTERMS.title, Literal("Test Title")))
    test_g.add((test_uri, DCTERMS.description, Literal("Test Description")))

    # Create profile with test graph
    test_profile = MolgenisEUCAIMDCATAPProfile(test_g)

    # Test extraction
    concept_dict = {}
    key_predicate_tuple = ((
        ("name", DCTERMS.title),
        ("description", DCTERMS.description),
        ("theme", DCAT.theme)
    ))

    result = test_profile._extract_concept_dict(
        test_uri, concept_dict, key_predicate_tuple
    )

    # Verify results
    assert result["name"] == "Test Title"
    assert result["description"] == "Test Description"


def test_extract_concept_dict_unwraps_single_item_list():
    """Test that single-item lists are unwrapped to scalar values."""
    # Create a test graph
    test_g = rdflib.Dataset()
    test_uri = URIRef("http://example.com/test")

    # Add a triple (will be returned as single-item list by _object_value)
    test_g.add((test_uri, DCTERMS.title, Literal("Single Title")))

    # Create profile with test graph
    test_profile = MolgenisEUCAIMDCATAPProfile(test_g)

    # Test extraction
    concept_dict = {}
    key_predicate_tuple = (
        ("name", DCTERMS.title),
    )

    result = test_profile._extract_concept_dict(
        test_uri, concept_dict, key_predicate_tuple
    )

    # Verify that the result is a string, not a list
    assert isinstance(result["name"], str)
    assert result["name"] == "Single Title"


def test_parse_person():
    """Test parsing a person reference"""
    # Create a person in the graph
    person_g = rdflib.Dataset()
    person_uri = URIRef("http://example.com/person/1")

    person_g.add((person_uri, RDF.type, FOAF.Person))
    person_g.add((person_uri, DCTERMS.identifier, Literal("test-person")))
    person_g.add((person_uri, FOAF.name, Literal("John Doe")))
    # person_g.add((person_uri, FOAF.lastName, Literal("Doe")))
    person_g.add((person_uri, FOAF.mbox, Literal("mailto:john.doe@example.com")))

    # Create profile with person graph
    person_profile = MolgenisEUCAIMDCATAPProfile(person_g)

    # Test parsing
    person_dict = {}
    person_profile.parse_person(person_dict, person_uri)

    # Verify results
    assert person_dict["uri"] == "http://example.com/person/1"
    assert person_dict["id"] == "test-person"
    assert person_dict["name"] == "John Doe"
    # assert person_dict["first_name"] == "John"
    assert person_dict["last_name"] == "John Doe"
    assert person_dict["email"] == "john.doe@example.com" # mailto: prefix removed


def test_parse_datasetseries():
    """Test parsing a datasetseries reference"""
    # Create a datasetseries in the graph
    series_g = rdflib.Dataset()
    series_uri = URIRef("http://example.com/series/1")

    series_g.add((series_uri, RDF.type, DCAT.DatasetSeries))
    series_g.add((series_uri, DCTERMS.title, Literal("Test Series")))
    series_g.add((series_uri, DCTERMS.description, Literal("Series Description")))
    series_g.add((series_uri, DCTERMS.publisher, Literal("Test Publisher")))
    series_g.add((series_uri, DCAT.landingPage, URIRef("http://example.com/series/landing")))

    # Create profile with series graph
    series_profile = MolgenisEUCAIMDCATAPProfile(series_g)

    # Test parsing
    series_dict = {}
    series_profile.parse_datasetseries(series_dict, series_uri)

    # Verify results
    assert series_dict["uri"] == "http://example.com/series/1"
    assert series_dict["name"] == "Test Series"
    assert series_dict["description"] == "Series Description"
    assert series_dict["juridical_person"] == "Test Publisher"
    assert series_dict["url"] == "http://example.com/series/landing"
