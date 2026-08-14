# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Tests for MOLGENIS DCAT profile extraction helper methods."""

import uuid

import rdflib
from rdflib import URIRef

from molgenis_fdp_harvester.base.baseparser import VCARD
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile


def test_resolve_reference_id_valid_vcard_contact(graph_vcard_contact):
    """A valid VCARD.Kind contact is replaced by its assigned UUIDv4 reference id."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_contact)
    contact_uri = URIRef("http://example.com/contact1")

    dataset_dict = {"contact": str(contact_uri)}
    result = profile._extract_and_transform_by_type(
        dataset_dict, "contact", VCARD.Kind, profile._resolve_reference_id
    )

    assert result["contact"] == profile._get_or_create_reference_id(str(contact_uri))


def test_resolve_reference_id_missing_key(graph_vcard_missing):
    """Test that missing key doesn't cause errors."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_missing)

    dataset_dict = {}  # No contact key
    result = profile._extract_and_transform_by_type(
        dataset_dict, "contact", VCARD.Kind, profile._resolve_reference_id
    )

    # Should return unchanged dict without errors
    assert "contact" not in result


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

    # Verify the referenced VCARD contact was resolved to its assigned UUIDv4
    assert result["contactPoint"] == profile._get_or_create_reference_id("http://example.com/contact_full")
    uuid.UUID(result["contactPoint"])

    # Verify the referenced FOAF Organization publisher was resolved to its assigned UUIDv4
    assert result["publisher"] == profile._get_or_create_reference_id("http://example.com/provider_org")
    uuid.UUID(result["publisher"])

    # Verify extracted DatasetSeries ID
    assert result["in_series"] == "biobank-full"


def test_parse_kind(graph_vcard_contact):
    """Test parsing a VCARD.Kind resource."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_contact)
    kind_ref = URIRef("http://example.com/contact1")

    result = profile.parse_kind({}, kind_ref)

    assert result["uri"] == "http://example.com/contact1"
    assert result["fn"] == "John Doe Contact"
    assert result["id"] == profile._get_or_create_reference_id("http://example.com/contact1")
    uuid.UUID(result["id"])


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
    assert result["id"] == profile._get_or_create_reference_id("http://example.com/org1")
    uuid.UUID(result["id"])


def test_parse_provenancestatement():
    """Test parsing a DCT.ProvenanceStatement resource."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_provenancestatement.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    prov_ref = URIRef("http://example.com/prov1")

    result = profile.parse_provenancestatement({}, prov_ref)

    assert result["uri"] == "http://example.com/prov1"
    assert result["label"] == "Data collected from hospital records"
    assert result["id"] == profile._get_or_create_reference_id("http://example.com/prov1")
    uuid.UUID(result["id"])
