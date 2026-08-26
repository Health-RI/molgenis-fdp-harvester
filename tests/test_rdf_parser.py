# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from pathlib import Path

import pytest
import rdflib
from rdflib import URIRef

from molgenis_fdp_harvester.base.processor import RDFParser
from molgenis_fdp_harvester.utils import HarvesterException


@pytest.fixture
def parser(profiles):
    return RDFParser(profiles)


_TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture
def catalog_data():
    return (_TEST_DATA_DIR / "rdf_catalog.ttl").read_text()


@pytest.fixture
def dataset1_data():
    return (_TEST_DATA_DIR / "rdf_dataset1.ttl").read_text()


@pytest.fixture
def dataset2_data():
    return (_TEST_DATA_DIR / "rdf_dataset2.ttl").read_text()


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
        assert "uri" in dataset
        assert "title" in dataset
        assert "description" in dataset
        assert dataset["concept_type"] == "dataset"

    # Verify specific dataset content
    gryffindor = next(d for d in dataset_dicts if d["title"] == "Gryffindor research project")
    assert gryffindor["uri"] == "http://example.com/dataset1"
    assert gryffindor["description"] == "Impact of muggle technical inventions on word's magic presense"

    slytherin = next(d for d in dataset_dicts if d["title"] == "Slytherin research project")
    assert slytherin["uri"] == "http://example.com/dataset2"
    assert slytherin["description"] == "Comarative analysis of magic powers of muggle-born and blood wizards "


def test_get_concept(parser, dataset1_data):
    """Test retrieving a specific concept by URI"""
    # Parse datasets
    parser.parse(data=dataset1_data, _format="turtle")

    # Get concept by URI
    dataset_uri = URIRef("http://example.com/dataset1")
    concept = parser.get_concept(dataset_uri, "dataset")

    # Verify concept fields
    assert concept["uri"] == "http://example.com/dataset1"
    assert concept["title"] == "Gryffindor research project"


def test_parse_invalid_data(parser):
    """Test handling of invalid RDF data"""
    invalid_data = "This is not valid RDF data"

    with pytest.raises(HarvesterException):
        parser.parse(data=invalid_data, _format="turtle")


def test_supported_formats(parser):
    """Test the supported_formats method returns a list of formats"""
    formats = parser.supported_formats()
    assert isinstance(formats, list)
    assert "turtle" in formats


def test_publisher_generator(parser):
    """publisher() yields dicts with concept_type 'publisher' for FOAF.Organization resources."""
    with Path("tests/test_data/extraction_foaf_organization.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    publishers = list(parser.publisher())
    assert len(publishers) == 1
    assert publishers[0]["concept_type"] == "publisher"
    assert publishers[0]["name"] == "Test Publisher Org"


def test_kind_generator(parser):
    """kind() yields dicts with concept_type 'kind' for VCARD.Kind resources."""
    with Path("tests/test_data/extraction_vcard_contact.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    kinds = list(parser.kind())
    assert len(kinds) == 1
    assert kinds[0]["concept_type"] == "kind"
    assert kinds[0]["fn"] == "John Doe Contact"


def test_provenancestatement_generator(parser):
    """provenancestatement() yields dicts with concept_type 'provenancestatement'."""
    with Path("tests/test_data/extraction_provenancestatement.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    provs = list(parser.provenancestatement())
    assert len(provs) == 1
    assert provs[0]["concept_type"] == "provenancestatement"
    assert provs[0]["label"] == "Data collected from hospital records"


def test_purpose_generator(parser):
    """purpose() yields dicts with concept_type 'purpose' for dpv:Purpose resources."""
    with Path("tests/test_data/extraction_purpose.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    purposes = list(parser.purpose())
    assert len(purposes) == 1
    assert purposes[0]["concept_type"] == "purpose"
    assert purposes[0]["description"] == "Scientific research"


def test_creator_generator(parser):
    """creator() yields dicts with concept_type 'creator' for foaf:Agent resources reached via
    a dataset's dct:creator (creator and attribution_agent share the same rdf:type, so this
    can't be a whole-graph type scan like the other generators - it walks the predicate path)."""
    with Path("tests/test_data/extraction_dataset_wired_fields.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    creators = list(parser.creator())
    assert len(creators) == 1
    assert creators[0]["concept_type"] == "creator"
    assert creators[0]["name"] == "Wired Creator"


def test_attribution_agent_generator(parser):
    """attribution_agent() yields dicts with concept_type 'attribution_agent' for foaf:Agent
    resources reached via a dataset's prov:qualifiedAttribution/prov:agent."""
    with Path("tests/test_data/extraction_dataset_wired_fields.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    attribution_agents = list(parser.attribution_agent())
    assert len(attribution_agents) == 1
    assert attribution_agents[0]["concept_type"] == "attribution_agent"
    assert attribution_agents[0]["name"] == "Wired Attribution Agent"


def test_legalbasis_generator(parser):
    """legalbasis() yields dicts with concept_type 'legalbasis' for dpv:LegalBasis resources."""
    with Path("tests/test_data/extraction_legal_basis.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    legal_bases = list(parser.legalbasis())
    assert len(legal_bases) == 1
    assert legal_bases[0]["concept_type"] == "legalbasis"
    assert legal_bases[0]["description"] == "GDPR Art. 9(2)(j)"


def test_rightsstatement_generator(parser):
    """rightsstatement() yields dicts with concept_type 'rightsstatement' for dct:RightsStatement
    resources reached via a dataset's distribution.rights (dct:RightsStatement is also commonly
    used to type a dataset's own dct:accessRights value, so this can't be a whole-graph type
    scan like legalbasis/purpose - it walks the predicate path instead)."""
    with Path("tests/test_data/extraction_dataset_with_rights.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    rightsstatements = list(parser.rightsstatement())
    assert len(rightsstatements) == 1
    assert rightsstatements[0]["concept_type"] == "rightsstatement"
    assert rightsstatements[0]["label"] == "Rights via distribution"


def test_dataservice_generator(parser):
    """dataservice() yields dicts with concept_type 'dataservice' for dcat:DataService resources."""
    with Path("tests/test_data/extraction_dataservice.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    dataservices = list(parser.dataservice())
    assert len(dataservices) == 1
    assert dataservices[0]["concept_type"] == "dataservice"
    assert dataservices[0]["title"] == "Test Data Service"


def test_distribution_generator(parser):
    """distribution() yields dicts with concept_type 'distribution' for dcat:Distribution
    resources."""
    with Path("tests/test_data/extraction_distribution_full.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    distributions = list(parser.distribution())
    assert len(distributions) == 1
    assert distributions[0]["concept_type"] == "distribution"
    assert distributions[0]["title"] == "Full Distribution"


def test_get_concept_publisher(parser):
    """get_concept() with type 'publisher' returns a dict with publisher fields."""
    with Path("tests/test_data/extraction_foaf_organization.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    publisher_uri = URIRef("http://example.com/org1")
    concept = parser.get_concept(publisher_uri, "publisher")

    assert concept["uri"] == "http://example.com/org1"
    assert concept["name"] == "Test Publisher Org"


def test_get_concept_kind(parser):
    """get_concept() with type 'kind' returns a dict with kind fields."""
    with Path("tests/test_data/extraction_vcard_contact.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    kind_uri = URIRef("http://example.com/contact1")
    concept = parser.get_concept(kind_uri, "kind")

    assert concept["uri"] == "http://example.com/contact1"
    assert concept["fn"] == "John Doe Contact"


def test_get_concept_provenancestatement(parser):
    """get_concept() with type 'provenancestatement' returns a dict with provenance fields."""
    with Path("tests/test_data/extraction_provenancestatement.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    prov_uri = URIRef("http://example.com/prov1")
    concept = parser.get_concept(prov_uri, "provenancestatement")

    assert concept["uri"] == "http://example.com/prov1"
    assert concept["label"] == "Data collected from hospital records"


def test_get_concept_purpose(parser):
    """get_concept() with type 'purpose' returns a dict with purpose fields."""
    with Path("tests/test_data/extraction_purpose.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    purpose_uri = URIRef("http://example.com/purpose1")
    concept = parser.get_concept(purpose_uri, "purpose")

    assert concept["uri"] == "http://example.com/purpose1"
    assert concept["description"] == "Scientific research"


def test_get_concept_creator(parser):
    """get_concept() with type 'creator' returns a dict with creator fields."""
    with Path("tests/test_data/extraction_dataset_wired_fields.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    creator_uri = URIRef("http://example.com/dataset_wired/creator")
    concept = parser.get_concept(creator_uri, "creator")

    assert concept["uri"] == "http://example.com/dataset_wired/creator"
    assert concept["name"] == "Wired Creator"


def test_get_concept_attribution_agent(parser):
    """get_concept() with type 'attribution_agent' returns a dict with attribution_agent fields."""
    with Path("tests/test_data/extraction_dataset_wired_fields.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    attribution_agent_uri = URIRef("http://example.com/dataset_wired/attribution_agent")
    concept = parser.get_concept(attribution_agent_uri, "attribution_agent")

    assert concept["uri"] == "http://example.com/dataset_wired/attribution_agent"
    assert concept["name"] == "Wired Attribution Agent"


def test_get_concept_legalbasis(parser):
    """get_concept() with type 'legalbasis' returns a dict with legalbasis fields."""
    with Path("tests/test_data/extraction_legal_basis.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    legalbasis_uri = URIRef("http://example.com/legalbasis1")
    concept = parser.get_concept(legalbasis_uri, "legalbasis")

    assert concept["uri"] == "http://example.com/legalbasis1"
    assert concept["description"] == "GDPR Art. 9(2)(j)"


def test_get_concept_rightsstatement(parser):
    """get_concept() with type 'rightsstatement' returns a dict with rightsstatement fields."""
    with Path("tests/test_data/extraction_distribution_full.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    rightsstatement_uri = URIRef("http://example.com/distribution1/rights")
    concept = parser.get_concept(rightsstatement_uri, "rightsstatement")

    assert concept["uri"] == "http://example.com/distribution1/rights"
    assert concept["label"] == "Access restricted to authorised researchers"


def test_get_concept_dataservice(parser):
    """get_concept() with type 'dataservice' returns a dict with dataservice fields."""
    with Path("tests/test_data/extraction_dataservice.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    dataservice_uri = URIRef("http://example.com/dataservice1")
    concept = parser.get_concept(dataservice_uri, "dataservice")

    assert concept["uri"] == "http://example.com/dataservice1"
    assert concept["title"] == "Test Data Service"


def test_get_concept_distribution(parser):
    """get_concept() with type 'distribution' returns a dict with distribution fields."""
    with Path("tests/test_data/extraction_distribution_full.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    distribution_uri = URIRef("http://example.com/distribution1")
    concept = parser.get_concept(distribution_uri, "distribution")

    assert concept["uri"] == "http://example.com/distribution1"
    assert concept["title"] == "Full Distribution"


def test_supplementary_class_reference_id_is_shared_across_calls(parser):
    """A dataset's reference to supplementary classes (publisher, contactPoint, provenance)
    resolves to the same internal UUID that the classes themselves are assigned, since all
    are resolved through the same, parser-scoped profile instance."""
    with Path("tests/test_data/extraction_dataset_integration.ttl").open() as f:
        parser.parse(data=f.read(), _format="turtle")

    [publisher] = list(parser.publisher())
    [kind] = list(parser.kind())
    [provenancestatement] = list(parser.provenancestatement())
    [dataset] = list(parser.datasets())

    assert dataset["publisher"] == publisher["id"]
    assert dataset["contactPoint"] == kind["id"]
    assert dataset["provenance"] == provenancestatement["id"]
