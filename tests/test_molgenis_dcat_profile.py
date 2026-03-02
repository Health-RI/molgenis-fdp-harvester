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
    p = MolgenisEUCAIMDCATAPProfile(rdf_graph)
    p.config = {'pid_service_url': 'https://pid.example.com', 'fdp_id_prefix': 'testorg'}
    return p


@pytest.fixture
def dataset_ref():
    return URIRef("http://example.com/dataset1")


def test_parse_dataset(profile, dataset_ref):
    """Test parsing a dataset reference into a dict"""
    dataset_dict = {}
    profile.parse_dataset(dataset_dict, dataset_ref)

    # Verify basic fields
    assert dataset_dict["uri"] == "http://example.com/dataset1"
    assert dataset_dict["title"] == "Gryffindor research project"
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


def test_parse_datasetseries():
    """Test parsing a datasetseries reference"""
    # Create a datasetseries in the graph
    series_g = rdflib.Dataset()
    series_uri = URIRef("http://example.com/series/1")

    series_g.add((series_uri, RDF.type, DCAT.DatasetSeries))
    series_g.add((series_uri, DCTERMS.title, Literal("Test Series")))
    series_g.add((series_uri, DCTERMS.description, Literal("Series Description")))
    series_g.add((series_uri, DCTERMS.publisher, Literal("Test Publisher")))

    # Create profile with series graph
    series_profile = MolgenisEUCAIMDCATAPProfile(series_g)

    # Test parsing
    series_dict = {}
    series_profile.parse_datasetseries(series_dict, series_uri)

    # Verify results
    assert series_dict["uri"] == "http://example.com/series/1"
    assert series_dict["title"] == "Test Series"
    assert series_dict["description"] == "Series Description"
    assert series_dict["publisher"] == "Test Publisher"


# --- handle_pids tests ---

def test_handle_pids_no_pid(profile):
    """Plain string identifier: id gets prefixed, identifier becomes PID service URL."""
    dataset_dict = {'identifier': 'mydata'}
    result = profile.handle_pids(dataset_dict)

    assert result['id'] == 'testorg-mydata'
    assert result['identifier'] == 'https://pid.example.com/testorg-mydata'


def test_handle_pids_external_pid(profile):
    """External URL identifier: id is sanitised via munge_title_to_name."""
    dataset_dict = {'identifier': 'https://other.pid/dataset/abc'}
    result = profile.handle_pids(dataset_dict)

    assert result['id'] == 'https-other-pid-dataset-abc'


def test_handle_pids_generated_pid(profile):
    """Identifier is a previously-generated PID service URL: id is the stable suffix, identifier unchanged."""
    pid_url = 'https://pid.example.com/testorg-mydata'
    dataset_dict = {'identifier': pid_url}
    result = profile.handle_pids(dataset_dict)

    assert result['id'] == 'testorg-mydata'
    assert result['identifier'] == pid_url


# --- _extract_name_publisher tests ---

def test_extract_name_publisher_valid(profile):
    """URI typed as FOAF.Organization: name is lowercased with spaces stripped."""
    org_uri = URIRef("http://example.com/org1")
    profile.g.add((org_uri, RDF.type, FOAF.Organization))
    profile.g.add((org_uri, FOAF.name, Literal("Test Publisher Org")))

    dataset_dict = {'publisher': str(org_uri)}
    result = profile._extract_name_publisher(dataset_dict, 'publisher')

    assert result['publisher'] == 'testpublisherorg'


def test_extract_name_publisher_wrong_type(profile):
    """URI with a different RDF type: field is left unchanged."""
    uri = URIRef("http://example.com/thing1")
    profile.g.add((uri, RDF.type, DCAT.Dataset))

    dataset_dict = {'publisher': str(uri)}
    result = profile._extract_name_publisher(dataset_dict, 'publisher')

    assert result['publisher'] == str(uri)


# --- _remove_default_language tests ---

def test_remove_default_language_removes_english(profile):
    """English is removed; other languages remain."""
    dataset_dict = {
        'language': [
            'http://id.loc.gov/vocabulary/iso639-1/en',
            'http://id.loc.gov/vocabulary/iso639-1/nl',
        ]
    }
    result = profile._remove_default_language(dataset_dict)

    assert result['language'] == ['http://id.loc.gov/vocabulary/iso639-1/nl']


def test_remove_default_language_only_english(profile):
    """If English is the only language, the key is deleted."""
    dataset_dict = {'language': ['http://id.loc.gov/vocabulary/iso639-1/en']}
    result = profile._remove_default_language(dataset_dict)

    assert 'language' not in result


def test_remove_default_language_no_english(profile):
    """If English is absent, the language list is unchanged."""
    dataset_dict = {'language': ['http://id.loc.gov/vocabulary/iso639-1/nl']}
    result = profile._remove_default_language(dataset_dict)

    assert result['language'] == ['http://id.loc.gov/vocabulary/iso639-1/nl']
