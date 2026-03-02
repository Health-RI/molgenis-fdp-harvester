# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Tests for MOLGENIS DCAT profile extraction helper methods."""

import rdflib
from rdflib import URIRef

from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile


def test_extract_name_vcard_valid_contact(graph_vcard_contact):
    """Test extracting name from valid VCARD.Kind contact."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_contact)

    dataset_dict = {"contact": "http://example.com/contact1"}
    result = profile._extract_name_vcard(dataset_dict, "contact")

    # Should extract and lowercase name with spaces stripped: "John Doe Contact" -> "johndoecontact"
    assert result["contact"] == "johndoecontact"


def test_extract_name_vcard_missing_key(graph_vcard_missing):
    """Test that missing key doesn't cause errors."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_missing)

    dataset_dict = {}  # No contact key
    result = profile._extract_name_vcard(dataset_dict, "contact")

    # Should return unchanged dict without errors
    assert "contact" not in result


def test_extract_name_agent_valid_foaf_person(graph_foaf_person):
    """Test extracting name from valid FOAF.Person."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_foaf_person)

    dataset_dict = {"provider": "http://example.com/provider1"}
    result = profile._extract_name_agent(dataset_dict, "provider")

    # Should extract name from FOAF.name
    assert result["provider"] == "Jane Smith Provider"


def test_extract_name_agent_wrong_type(graph_foaf_wrong_type):
    """Test that wrong RDF type doesn't modify the field."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_foaf_wrong_type)

    dataset_dict = {"provider": "http://example.com/provider2"}
    original_value = dataset_dict["provider"]
    result = profile._extract_name_agent(dataset_dict, "provider")

    # Should remain unchanged (URI string)
    assert result["provider"] == original_value


def test_convert_image_year_range_valid_period(graph_date_range):
    """Test converting valid DCT.PeriodOfTime to formatted date range."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range)

    dataset_dict = {"image_year_range": "http://example.com/period1"}
    result = profile._convert_image_year_range(dataset_dict)

    # Should format as "YYYY-MM-DD - YYYY-MM-DD"
    assert result["image_year_range"] == "2020-01-01 - 2023-12-31"


def test_convert_image_year_range_missing_key(graph_date_range_missing):
    """Test that missing key doesn't cause errors."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range_missing)

    dataset_dict = {}  # No image_year_range key
    result = profile._convert_image_year_range(dataset_dict)

    # Should return unchanged dict without errors
    assert "image_year_range" not in result


def test_extract_datasetseries_id_with_identifier(graph_datasetseries_with_id):
    """Test extracting DatasetSeries ID when identifier is present."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_datasetseries_with_id)

    dataset_dict = {"in_series": "http://example.com/series1"}
    result = profile._extract_datasetseries_id(dataset_dict)

    # Should use the identifier
    assert result["in_series"] == "biobank-001"


def test_extract_datasetseries_id_fallback_to_title(graph_datasetseries_no_id):
    """Test falling back to munged title when identifier is empty."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_datasetseries_no_id)

    dataset_dict = {"in_series": "http://example.com/series2"}
    result = profile._extract_datasetseries_id(dataset_dict)

    # Should fall back to munged title: "Biobank Without ID" -> "biobank-without-id"
    assert result["in_series"] == "biobank-without-id"


def test_parse_dataset_integration(graph_dataset_integration):
    """Test full dataset parsing with multiple extraction functions."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_dataset_integration)
    profile.config = {'pid_service_url': 'https://pid.example.com', 'fdp_id_prefix': 'testorg'}
    dataset_ref = URIRef("http://example.com/dataset_full")

    dataset_dict = {}
    result = profile.parse_dataset(dataset_dict, dataset_ref)

    # Verify basic fields
    assert result["uri"] == "http://example.com/dataset_full"
    assert result["title"] == "Full Integration Test Dataset"
    assert result["description"] == "A comprehensive dataset for integration testing"

    # Verify PID handling: plain string identifier gets prefixed
    assert result["id"] == "testorg-dataset-full-001"
    assert result["identifier"] == "https://pid.example.com/testorg-dataset-full-001"

    # Verify extracted name from VCARD contact
    assert result["contactPoint"] == "drjanesmith"

    # Verify extracted name from FOAF Organization publisher
    assert result["publisher"] == "testorganization"

    # Verify extracted DatasetSeries ID
    assert result["in_series"] == "biobank-full"


def test_parse_kind(graph_vcard_contact):
    """Test parsing a VCARD.Kind resource."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_contact)
    kind_ref = URIRef("http://example.com/contact1")

    result = profile.parse_kind({}, kind_ref)

    assert result["uri"] == "http://example.com/contact1"
    assert result["fn"] == "John Doe Contact"


def test_parse_publisher():
    """Test parsing a FOAF.Organization resource."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_foaf_organization.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    publisher_ref = URIRef("http://example.com/org1")

    result = profile.parse_publisher({}, publisher_ref)

    assert result["uri"] == "http://example.com/org1"
    assert result["name"] == "Test Publisher Org"
    assert result["description"] == "A test publishing organisation"
    assert result["publishertype"] == "ResearchInstitute"
    assert result["homepage"] == "https://example.com"


def test_parse_provenancestatement():
    """Test parsing a DCT.ProvenanceStatement resource."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_provenancestatement.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    prov_ref = URIRef("http://example.com/prov1")

    result = profile.parse_provenancestatement({}, prov_ref)

    assert result["uri"] == "http://example.com/prov1"
    assert result["label"] == "Data collected from hospital records"
