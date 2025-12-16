# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Tests for MOLGENIS DCAT profile extraction helper methods."""

import rdflib
from rdflib import URIRef
from rdflib.namespace import DCTERMS
from rdflib import Literal

from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from tests.conftest import (
    graph_vcard_contact,
    graph_vcard_missing,
    graph_foaf_person,
    graph_foaf_wrong_type,
    graph_date_range,
    graph_date_range_missing,
    graph_datasetseries_with_id,
    graph_datasetseries_no_id,
    graph_dataset_integration,
    graph_person_vcard
)


def test_extract_name_vcard_valid_contact(graph_vcard_contact):
    """Test extracting name from valid VCARD.Kind contact."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_contact)

    dataset_dict = {"contact": "http://example.com/contact1"}
    result = profile._extract_name_vcard(dataset_dict, "contact")

    # Should extract and format name: "John Doe Contact" -> "john-doe-contact"
    assert result["contact"] == "john-doe-contact"


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

    dataset_dict = {"biobank": "http://example.com/series1"}
    result = profile._extract_datasetseries_id(dataset_dict)

    # Should use the identifier
    assert result["biobank"] == "biobank-001"


def test_extract_datasetseries_id_fallback_to_title(graph_datasetseries_no_id):
    """Test falling back to munged title when identifier is empty."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_datasetseries_no_id)

    dataset_dict = {"biobank": "http://example.com/series2"}
    result = profile._extract_datasetseries_id(dataset_dict)

    # Should fall back to munged title: "Biobank Without ID" -> "biobank-without-id"
    assert result["biobank"] == "biobank-without-id"


def test_parse_dataset_integration(graph_dataset_integration):
    """Test full dataset parsing with multiple extraction functions."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_dataset_integration)
    dataset_ref = URIRef("http://example.com/dataset_full")

    dataset_dict = {}
    result = profile.parse_dataset(dataset_dict, dataset_ref)

    # Verify basic fields
    assert result["uri"] == "http://example.com/dataset_full"
    assert result["id"] == "dataset-full-001"
    assert result["name"] == "Full Integration Test Dataset"
    assert result["description"] == "A comprehensive dataset for integration testing"

    # Verify extracted name from VCARD contact
    assert result["contact"] == "dr-jane-smith"

    # Verify extracted name from FOAF Organization
    assert result["provider"] == "Test Organization"

    # Verify extracted DatasetSeries ID
    assert result["biobank"] == "biobank-full"

    # Verify converted date range
    assert result["image_year_range"] == "2018-06-15 - 2024-08-30"


def test_parse_person_with_vcard(graph_person_vcard):
    """Test parsing person with VCARD type."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_person_vcard)
    person_ref = URIRef("http://example.com/person_vcard")

    person_dict = {}
    result = profile.parse_person(person_dict, person_ref)

    # Verify basic fields
    assert result["uri"] == "http://example.com/person_vcard"
    assert result["id"] == "person-vcard-001"
    assert result["name"] == "Dr. Sarah Johnson"
    assert result["last_name"] == "Dr. Sarah Johnson"

    # Verify mailto: prefix is removed
    assert result["email"] == "sarah.johnson@example.org"

    # Verify first_name is set to empty space (as per implementation)
    assert result["first_name"] == " "
