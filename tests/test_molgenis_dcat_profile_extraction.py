# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Tests for MOLGENIS DCAT profile extraction helper methods."""

import uuid

import pytest
import rdflib
from rdflib import URIRef
from rdflib.namespace import DCTERMS as DCT
from rdflib.namespace import FOAF

from molgenis_fdp_harvester.base.baseparser import VCARD
from molgenis_fdp_harvester.base.molgenis_dcat_profile import MolgenisEUCAIMDCATAPProfile
from molgenis_fdp_harvester.utils import HarvesterException


def test_resolve_reference_id_valid_vcard_contact(graph_vcard_contact):
    """A valid VCARD.Kind contact is replaced by its assigned UUIDv4 reference id."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_contact)
    contact_uri = URIRef("http://example.com/contact1")

    dataset_dict = {"contact": str(contact_uri)}
    result = profile._extract_and_transform_by_type(dataset_dict, "contact", VCARD.Kind, profile._resolve_reference_id)

    assert result["contact"] == profile._get_or_create_reference_id(str(contact_uri))


def test_resolve_reference_id_missing_key(graph_vcard_missing):
    """Test that missing key doesn't cause errors."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_missing)

    dataset_dict = {}  # No contact key
    result = profile._extract_and_transform_by_type(dataset_dict, "contact", VCARD.Kind, profile._resolve_reference_id)

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
    profile.config = {'pid_service_url': 'https://pid.example.com'}
    dataset_ref = URIRef("http://example.com/dataset_full")

    dataset_dict = {}
    result = profile.parse_dataset(dataset_dict, dataset_ref)

    # Verify basic fields
    assert result["uri"] == "http://example.com/dataset_full"
    assert result["title"] == "Full Integration Test Dataset"
    assert result["description"] == "A comprehensive dataset for integration testing"

    # Verify PID handling: original identifier moves to other_identifier, id/identifier are generated
    assert result["other_identifier"] == "dataset-full-001"
    assert result["identifier"] == f"https://pid.example.com/{result['id']}"

    # Verify the referenced VCARD contact was resolved to its assigned UUIDv4
    assert result["contactPoint"] == profile._get_or_create_reference_id("http://example.com/contact_full")
    parsed = uuid.UUID(result["contactPoint"])  # raises ValueError if not a valid UUID
    assert parsed.version == 4

    # Verify the referenced FOAF Organization publisher was resolved to its assigned UUIDv4
    assert result["publisher"] == profile._get_or_create_reference_id("http://example.com/provider_org")
    parsed = uuid.UUID(result["publisher"])  # raises ValueError if not a valid UUID
    assert parsed.version == 4

    # Verify the referenced ProvenanceStatement was resolved to its assigned UUIDv4
    assert result["provenance"] == profile._get_or_create_reference_id("http://example.com/provenance_full")
    parsed = uuid.UUID(result["provenance"])  # raises ValueError if not a valid UUID
    assert parsed.version == 4

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
    parsed = uuid.UUID(result["id"])  # raises ValueError if not a valid UUID
    assert parsed.version == 4


def test_resolve_contactpoint_without_fn_assigns_distinct_ids(graph_vcard_no_fn):
    """Two datasets referencing different Kind resources that both lack vcard:fn must
    resolve to distinct contactPoint ids.

    Regression test: previously the contactPoint FK was derived from `vcard:fn` itself
    (`fn.lower().replace(" ", "")`, replicating Molgenis's server-computed `kind.id`
    formula), which yields the same "" for any Kind missing `fn` — so two different
    nameless contacts collided onto the same (empty) contactPoint reference.
    """
    profile = MolgenisEUCAIMDCATAPProfile(graph_vcard_no_fn)

    dataset1 = profile._extract_and_transform_by_type(
        {"contactPoint": "http://example.com/contact_no_fn_1"},
        "contactPoint",
        VCARD.Kind,
        profile._resolve_reference_id,
    )
    dataset2 = profile._extract_and_transform_by_type(
        {"contactPoint": "http://example.com/contact_no_fn_2"},
        "contactPoint",
        VCARD.Kind,
        profile._resolve_reference_id,
    )

    assert dataset1["contactPoint"] != dataset2["contactPoint"]


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
    assert result["type"] == "http://purl.org/adms/publishertype/Academia-ScientificOrganisation"
    assert result["homepage"] == "https://example.com"
    assert result["id"] == profile._get_or_create_reference_id("http://example.com/org1")
    parsed = uuid.UUID(result["id"])  # raises ValueError if not a valid UUID
    assert parsed.version == 4


def test_resolve_publisher_without_name_assigns_distinct_ids(graph_foaf_organization_no_name):
    """Two datasets referencing different Organization resources that both lack foaf:name
    must resolve to distinct publisher ids.

    Regression test: previously the publisher FK was derived from `foaf:name` itself
    (`name.lower().replace(" ", "")`, replicating Molgenis's server-computed `publisher.id`
    formula), which yields the same "" for any Organization missing `name` — so two
    different nameless publishers collided onto the same (empty) publisher reference.
    """
    profile = MolgenisEUCAIMDCATAPProfile(graph_foaf_organization_no_name)

    dataset1 = profile._extract_and_transform_by_type(
        {"publisher": "http://example.com/org_no_name_1"}, "publisher", FOAF.Organization, profile._resolve_reference_id
    )
    dataset2 = profile._extract_and_transform_by_type(
        {"publisher": "http://example.com/org_no_name_2"}, "publisher", FOAF.Organization, profile._resolve_reference_id
    )

    assert dataset1["publisher"] != dataset2["publisher"]


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
    parsed = uuid.UUID(result["id"])  # raises ValueError if not a valid UUID
    assert parsed.version == 4


def test_resolve_provenance_without_label_assigns_distinct_ids(graph_provenancestatement_no_label):
    """Two datasets referencing different ProvenanceStatement resources that both lack
    rdfs:label must resolve to distinct provenance ids.

    Regression test: previously the provenance FK was derived from `rdfs:label` itself
    (a djb2-style hash of sorted labels, replicating Molgenis's server-computed
    `provenance_statement.id` formula), which hashes to the same value for any
    ProvenanceStatement missing `label` — so two different labelless statements collided
    onto the same provenance reference.
    """
    profile = MolgenisEUCAIMDCATAPProfile(graph_provenancestatement_no_label)

    dataset1 = profile._extract_and_transform_by_type(
        {"provenance": "http://example.com/prov_no_label_1"},
        "provenance",
        DCT.ProvenanceStatement,
        profile._resolve_reference_id,
    )
    dataset2 = profile._extract_and_transform_by_type(
        {"provenance": "http://example.com/prov_no_label_2"},
        "provenance",
        DCT.ProvenanceStatement,
        profile._resolve_reference_id,
    )

    assert dataset1["provenance"] != dataset2["provenance"]


def test_extract_purpose_resolves_nested_purpose_object():
    """hasPurpose_obj pointing to a dpv:Purpose resource should be parsed via parse_purpose,
    and hasPurpose_IRI should be dropped."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_purpose.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {
        "hasPurpose_obj": "http://example.com/purpose1",
        "hasPurpose_IRI": "http://example.com/purpose1",
    }
    result = profile._extract_purpose(dataset_dict)

    assert result["hasPurpose_obj"]["uri"] == "http://example.com/purpose1"
    assert result["hasPurpose_obj"]["description"] == "Scientific research"
    assert "hasPurpose_IRI" not in result


def test_extract_purpose_keeps_plain_vocabulary_iri():
    """hasPurpose_obj pointing to a resource that is not typed dpv:Purpose should be dropped,
    leaving hasPurpose_IRI as a plain IRI string."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_purpose.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    # A plain vocabulary term (e.g. from the DPV purpose vocabulary) referenced directly,
    # with no local dpv:Purpose typing triple in this graph.
    dataset_dict = {
        "hasPurpose_obj": "https://w3id.org/dpv#AcademicResearch",
        "hasPurpose_IRI": "https://w3id.org/dpv#AcademicResearch",
    }
    result = profile._extract_purpose(dataset_dict)

    assert "hasPurpose_obj" not in result
    assert result["hasPurpose_IRI"] == "https://w3id.org/dpv#AcademicResearch"


def test_extract_purpose_missing_key_is_noop():
    """Dataset dict without hasPurpose_obj should be returned unchanged, without KeyError."""
    g = rdflib.Dataset()
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"title": "Some Dataset"}
    result = profile._extract_purpose(dataset_dict)

    assert result == {"title": "Some Dataset"}


def test_extract_purpose_resolves_multiple_mixed_values():
    """dpv:hasPurpose is a ref_array in the Molgenis model, so a dataset may declare more than
    one value. A dpv:Purpose-typed value and a plain vocabulary IRI given together should both
    survive: hasPurpose_obj gets the parsed object, hasPurpose_IRI keeps the plain IRI, rather
    than one silently overwriting/dropping the other."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_purpose.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {
        "hasPurpose_obj": ["http://example.com/purpose1", "https://w3id.org/dpv#AcademicResearch"],
        "hasPurpose_IRI": ["http://example.com/purpose1", "https://w3id.org/dpv#AcademicResearch"],
    }
    result = profile._extract_purpose(dataset_dict)

    assert result["hasPurpose_obj"]["uri"] == "http://example.com/purpose1"
    assert result["hasPurpose_obj"]["description"] == "Scientific research"
    assert result["hasPurpose_IRI"] == "https://w3id.org/dpv#AcademicResearch"


# ---------------------------------------------------------------------------
# _extract_creator / parse_creator (dct:creator -> foaf:Agent)
# Fixtures needed: tests/test_data/extraction_creator_foaf_agent.ttl,
#                  reuses graph_foaf_person / graph_foaf_wrong_type for the wrong-type case
# ---------------------------------------------------------------------------


def test_extract_creator_valid_agent_is_parsed():
    """creator pointing to a foaf:Agent resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_creator_foaf_agent.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"creator": "http://example.com/creator1"}
    result = profile._extract_creator(dataset_dict, "creator")

    assert result["creator"]["uri"] == "http://example.com/creator1"
    assert result["creator"]["name"] == "Test Creator"


def test_extract_creator_wrong_type_left_as_iri(graph_foaf_wrong_type):
    """creator pointing to a resource that is not a foaf:Agent should remain a plain IRI string."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_foaf_wrong_type)

    dataset_dict = {"creator": "http://example.com/provider2"}
    result = profile._extract_creator(dataset_dict, "creator")

    assert result["creator"] == "http://example.com/provider2"


def test_parse_creator_fields():
    """parse_creator should extract uri, name, description, type, mbox, homepage.

    Unlike publisher, the Molgenis 'creator' table has no 'phone' column, so parse_creator
    must not attempt to extract foaf:phone.
    """
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_creator_foaf_agent.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    creator_ref = URIRef("http://example.com/creator1")

    result = profile.parse_creator({}, creator_ref)

    assert result["uri"] == "http://example.com/creator1"
    assert result["name"] == "Test Creator"
    assert result["description"] == "A creator agent"
    assert result["type"] == "http://purl.org/adms/publishertype/Academia-ScientificOrganisation"
    assert result["mbox"] == "creator@example.com"
    assert result["homepage"] == "https://creator.example.com"
    assert "phone" not in result


def test_extract_creator_multiple_values_resolved_independently():
    """dct:creator is a ref_array in the Molgenis model, so a dataset may declare more than one
    creator. _extract_and_transform_by_type must resolve each value independently instead of
    crashing on URIRef(list) -- each matching foaf:Agent gets parsed, non-matching resources
    are left as plain IRI strings, same as the single-value case."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_creator_multi.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {
        "creator": [
            "http://example.com/creator_multi_1",
            "http://example.com/creator_multi_2",
            "http://example.com/creator_multi_not_agent",
        ]
    }
    result = profile._extract_creator(dataset_dict, "creator")

    assert isinstance(result["creator"], list)
    assert len(result["creator"]) == 3
    assert result["creator"][0]["name"] == "First Multi Creator"
    assert result["creator"][1]["name"] == "Second Multi Creator"
    assert result["creator"][2] == "http://example.com/creator_multi_not_agent"


# ---------------------------------------------------------------------------
# _extract_periodoftime / parse_periodoftime
# (dct:temporal / healthdcatap:retentionPeriod -> dct:PeriodOfTime)
# Fixtures: reuses graph_date_range / graph_date_range_missing from conftest.py
# ---------------------------------------------------------------------------


def test_extract_periodoftime_temporal_valid(graph_date_range):
    """temporal pointing to a dct:PeriodOfTime resource should be replaced with a parsed dict."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range)

    dataset_dict = {"temporal": "http://example.com/period1"}
    result = profile._extract_periodoftime(dataset_dict, "temporal")

    assert result["temporal"]["uri"] == "http://example.com/period1"
    assert result["temporal"]["startDate"] == "2020-01-01T00:00:00"
    assert result["temporal"]["endDate"] == "2023-12-31T00:00:00"
    assert result["temporal"]["id"] == "2020-01-01T00:00:00/2023-12-31T00:00:00"


def test_extract_periodoftime_retentionperiod_valid(graph_date_range):
    """retentionPeriod pointing to a dct:PeriodOfTime resource should be replaced with a parsed
    dict, using the same extraction path as temporal."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range)

    dataset_dict = {"retentionPeriod": "http://example.com/period1"}
    result = profile._extract_periodoftime(dataset_dict, "retentionPeriod")

    assert result["retentionPeriod"]["startDate"] == "2020-01-01T00:00:00"
    assert result["retentionPeriod"]["endDate"] == "2023-12-31T00:00:00"


def test_parse_periodoftime_id_is_start_end_range(graph_date_range):
    """parse_periodoftime should build id as '<startDate>/<endDate>'."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range)
    period_ref = URIRef("http://example.com/period1")

    result = profile.parse_periodoftime({}, period_ref)

    assert result["uri"] == "http://example.com/period1"
    assert result["startDate"] == "2020-01-01T00:00:00"
    assert result["endDate"] == "2023-12-31T00:00:00"
    assert result["id"] == "2020-01-01T00:00:00/2023-12-31T00:00:00"


def test_parse_periodoftime_missing_end_date_raises(graph_date_range_start_only):
    """parse_periodoftime should raise when startDate is set but endDate is missing, since the
    composed id would otherwise silently become '<startDate>/None'."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range_start_only)
    period_ref = URIRef("http://example.com/period_start_only")

    with pytest.raises(HarvesterException, match="No end date"):
        profile.parse_periodoftime({}, period_ref)


def test_parse_periodoftime_missing_start_date_raises(graph_date_range_end_only):
    """parse_periodoftime should raise when endDate is set but startDate is missing, since the
    composed id would otherwise silently become 'None/<endDate>'."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range_end_only)
    period_ref = URIRef("http://example.com/period_end_only")

    with pytest.raises(HarvesterException, match="No start date"):
        profile.parse_periodoftime({}, period_ref)


def test_parse_periodoftime_both_dates_missing_raises(graph_date_range_missing):
    """parse_periodoftime should raise when neither startDate nor endDate is present at all."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range_missing)
    period_ref = URIRef("http://example.com/dataset6")

    with pytest.raises(HarvesterException, match="No start date"):
        profile.parse_periodoftime({}, period_ref)


def test_extract_periodoftime_missing_key_is_noop(graph_date_range_missing):
    """Dataset dict without a 'temporal' key should be returned unchanged."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_date_range_missing)

    dataset_dict = {"title": "Test Dataset Without Date Range"}
    result = profile._extract_periodoftime(dataset_dict, "temporal")

    assert "temporal" not in result


# ---------------------------------------------------------------------------
# _extract_attribution / parse_attribution / _extract_attribution_agent /
# parse_attribution_agent (dct:qualifiedAttribution -> prov:Attribution -> prov:agent -> foaf:Agent)
# Fixtures needed: tests/test_data/extraction_attribution.ttl,
#                  tests/test_data/extraction_attribution_wrong_type.ttl
# ---------------------------------------------------------------------------


def test_extract_attribution_valid():
    """qualifiedAttribution pointing to a prov:Attribution resource should be replaced with a
    parsed dict, including a nested parsed agent."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_attribution.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"qualifiedAttribution": "http://example.com/attribution1"}
    result = profile._extract_attribution(dataset_dict, "qualifiedAttribution")

    attribution = result["qualifiedAttribution"]
    assert attribution["uri"] == "http://example.com/attribution1"
    assert attribution["hadRole"] == "http://registry.it.csiro.au/def/isotc211/CI_RoleCode/author"
    assert attribution["agent"]["name"] == "Attribution Agent"


def test_extract_attribution_wrong_type_left_as_iri():
    """qualifiedAttribution pointing to a resource that is not a prov:Attribution should remain
    a plain IRI string."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_attribution_wrong_type.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"qualifiedAttribution": "http://example.com/attribution_wrong"}
    result = profile._extract_attribution(dataset_dict, "qualifiedAttribution")

    assert result["qualifiedAttribution"] == "http://example.com/attribution_wrong"


def test_parse_attribution_fields_and_nested_agent():
    """parse_attribution should extract hadRole and resolve agent into a parsed attribution
    agent dict when the agent resource is typed foaf:Agent."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_attribution.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    attribution_ref = URIRef("http://example.com/attribution1")

    result = profile.parse_attribution({}, attribution_ref)

    assert result["uri"] == "http://example.com/attribution1"
    assert result["hadRole"] == "http://registry.it.csiro.au/def/isotc211/CI_RoleCode/author"
    assert result["agent"]["uri"] == "http://example.com/attribution_agent1"
    assert result["agent"]["name"] == "Attribution Agent"


def test_parse_attribution_agent_fields():
    """parse_attribution_agent should extract name, description, type, mbox, homepage."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_attribution.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    agent_ref = URIRef("http://example.com/attribution_agent1")

    result = profile.parse_attribution_agent({}, agent_ref)

    assert result["uri"] == "http://example.com/attribution_agent1"
    assert result["name"] == "Attribution Agent"
    assert result["description"] == "An attributed agent"
    assert result["type"] == "http://purl.org/adms/publishertype/Academia-ScientificOrganisation"
    assert result["mbox"] == "agent@example.com"
    assert result["homepage"] == "https://agent.example.com"


def test_extract_other_identifier_valid():
    """other_identifier pointing to an adms:Identifier resource should be replaced with a
    parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_other_identifier.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"other_identifier": "http://example.com/other_id1"}
    result = profile._extract_other_identifier(dataset_dict, "other_identifier")

    assert result["other_identifier"]["uri"] == "http://example.com/other_id1"
    assert result["other_identifier"]["notation"] == "ABC-123"
    assert result["other_identifier"]["schemaAgency"] == "Test Agency"


def test_extract_other_identifier_wrong_type_left_as_iri():
    """other_identifier pointing to a resource that is not an adms:Identifier should remain a
    plain IRI string."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_other_identifier_wrong_type.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"other_identifier": "http://example.com/other_id_wrong"}
    result = profile._extract_other_identifier(dataset_dict, "other_identifier")

    assert result["other_identifier"] == "http://example.com/other_id_wrong"


def test_parse_other_identifier_fields():
    """parse_other_identifier should extract notation and schemaAgency."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_other_identifier.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    other_id_ref = URIRef("http://example.com/other_id1")

    result = profile.parse_other_identifier({}, other_id_ref)

    assert result["uri"] == "http://example.com/other_id1"
    assert result["notation"] == "ABC-123"
    assert result["schemaAgency"] == "Test Agency"


def test_extract_distribution_sample_valid():
    """sample pointing to a dcat:Distribution resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"sample": "http://example.com/distribution1"}
    result = profile._extract_distribution(dataset_dict, "sample")

    assert result["sample"]["uri"] == "http://example.com/distribution1"
    assert result["sample"]["title"] == "Full Distribution"


def test_extract_distribution_analytics_valid():
    """analytics pointing to a dcat:Distribution resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"analytics": "http://example.com/distribution1"}
    result = profile._extract_distribution(dataset_dict, "analytics")

    assert result["analytics"]["uri"] == "http://example.com/distribution1"
    assert result["analytics"]["title"] == "Full Distribution"


def test_extract_distribution_wrong_type_left_as_iri():
    """sample/analytics pointing to a resource that is not a dcat:Distribution should remain a
    plain IRI string."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_wrong_type.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"sample": "http://example.com/dist_wrong"}
    result = profile._extract_distribution(dataset_dict, "sample")

    assert result["sample"] == "http://example.com/dist_wrong"


def test_parse_distribution_scalar_fields():
    """parse_distribution should extract the full set of scalar fields from
    tests/test_data/extraction_distribution_full.ttl."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    distribution_ref = URIRef("http://example.com/distribution1")

    result = profile.parse_distribution({}, distribution_ref)

    assert result["uri"] == "http://example.com/distribution1"
    assert result["title"] == "Full Distribution"
    assert result["description"] == "A comprehensive distribution for testing"
    assert result["page"] == "http://example.com/distribution1/docs"
    assert result["accessURL"] == "http://example.com/distribution1/access"
    assert result["downloadURL"] == "http://example.com/distribution1/download"
    assert result["availability"] == "http://publications.europa.eu/resource/authority/planned-availability/AVAILABLE"
    assert result["applicableLegislation"] == "http://data.europa.eu/eli/reg/2025/327/oj"
    assert result["license"] == "http://rdflicense.appspot.com/rdflicense/cc-by-nc-nd3.0"
    assert result["format"] == "http://publications.europa.eu/resource/authority/file-type/DICOM"
    assert result["mediaType"] == "https://www.iana.org/assignments/media-types/application/dicom"
    assert result["compressFormat"] == "https://www.iana.org/assignments/media-types/application/zip"
    assert result["packageFormat"] == "https://www.iana.org/assignments/media-types/application/x-tar"
    assert result["byteSize"] == "1024000"
    assert result["spatialResolutionInMeters"] == "0.5"
    assert result["conformsTo"] == "http://example.com/standards/fhir"
    assert result["language"] == "http://id.loc.gov/vocabulary/iso639-1/en"
    assert result["status"] == "http://publications.europa.eu/resource/authority/distribution-status/COMPLETED"
    assert result["issued"] == "2023-01-15T00:00:00"
    assert result["modified"] == "2024-05-20T00:00:00"


def test_parse_distribution_nested_rights():
    """parse_distribution should resolve 'rights' into a parsed dct:RightsStatement dict when
    typed accordingly."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    distribution_ref = URIRef("http://example.com/distribution1")

    result = profile.parse_distribution({}, distribution_ref)

    assert result["rights"]["uri"] == "http://example.com/distribution1/rights"
    assert result["rights"]["label"] == "Access restricted to authorised researchers"


def test_parse_distribution_nested_checksum():
    """parse_distribution should resolve 'checksum' into a parsed spdx:Checksum dict when typed
    accordingly."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    distribution_ref = URIRef("http://example.com/distribution1")

    result = profile.parse_distribution({}, distribution_ref)

    assert result["checksum"]["uri"] == "http://example.com/distribution1/checksum"
    assert result["checksum"]["algorithm"] == "http://spdx.org/rdf/terms#checksumAlgorithm_sha256"
    assert result["checksum"]["checksumValue"] == "abc123def456"


def test_parse_distribution_nested_policy():
    """parse_distribution should resolve 'hasPolicy' into a parsed odrl:Policy dict when the
    referenced resource is typed odrl:Policy. Requires extraction_distribution_full.ttl's
    <.../policy> resource to gain `a odrl:Policy` plus permission/prohibition/obligation
    triples (see tests/test_data/extraction_policy.ttl for the expected shape)."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    distribution_ref = URIRef("http://example.com/distribution1")

    result = profile.parse_distribution({}, distribution_ref)

    assert result["hasPolicy"]["uri"] == "http://example.com/distribution1/policy"
    assert "permission" in result["hasPolicy"]


def test_parse_distribution_nested_dataservice():
    """parse_distribution should resolve 'accessService' into a parsed dcat:DataService dict
    when the referenced resource is typed dcat:DataService. Requires
    extraction_distribution_full.ttl's <.../service> resource to gain `a dcat:DataService`
    plus its own descriptive fields (see tests/test_data/extraction_dataservice.ttl for the
    expected shape)."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    distribution_ref = URIRef("http://example.com/distribution1")

    result = profile.parse_distribution({}, distribution_ref)

    assert result["accessService"]["uri"] == "http://example.com/distribution1/service"


def test_extract_policy_valid():
    """hasPolicy pointing to an odrl:Policy resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_policy.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"hasPolicy": "http://example.com/policy1"}
    result = profile._extract_policy(dataset_dict, "hasPolicy")

    assert result["hasPolicy"]["uri"] == "http://example.com/policy1"
    assert result["hasPolicy"]["permission"]["action"] == "http://www.w3.org/ns/odrl/2/use"


def test_extract_policy_wrong_type_left_as_iri():
    """hasPolicy pointing to a resource that is not an odrl:Policy should remain a plain IRI
    string."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_policy_wrong_type.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"hasPolicy": "http://example.com/policy_wrong"}
    result = profile._extract_policy(dataset_dict, "hasPolicy")

    assert result["hasPolicy"] == "http://example.com/policy_wrong"


def test_parse_policy_nested_permission_prohibition_obligation():
    """parse_policy should resolve permission/prohibition/obligation into parsed dicts when
    their resources are typed odrl:Permission/Prohibition/Duty respectively."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_policy.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    policy_ref = URIRef("http://example.com/policy1")

    result = profile.parse_policy({}, policy_ref)

    assert result["uri"] == "http://example.com/policy1"
    assert result["permission"]["uri"] == "http://example.com/policy1/permission1"
    assert result["permission"]["action"] == "http://www.w3.org/ns/odrl/2/use"
    assert result["prohibition"]["uri"] == "http://example.com/policy1/prohibition1"
    assert result["prohibition"]["action"] == "http://www.w3.org/ns/odrl/2/distribute"
    assert result["obligation"]["uri"] == "http://example.com/policy1/obligation1"
    assert result["obligation"]["action"] == "http://www.w3.org/ns/odrl/2/attribute"


def test_parse_permission_fields():
    """parse_permission should extract 'action'."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_policy.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    permission_ref = URIRef("http://example.com/policy1/permission1")

    result = profile.parse_permission({}, permission_ref)

    assert result["uri"] == "http://example.com/policy1/permission1"
    assert result["action"] == "http://www.w3.org/ns/odrl/2/use"


def test_parse_prohibition_fields():
    """parse_prohibition should extract 'action'."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_policy.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    prohibition_ref = URIRef("http://example.com/policy1/prohibition1")

    result = profile.parse_prohibition({}, prohibition_ref)

    assert result["uri"] == "http://example.com/policy1/prohibition1"
    assert result["action"] == "http://www.w3.org/ns/odrl/2/distribute"


def test_parse_obligation_fields():
    """parse_obligation should extract 'action'."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_policy.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    obligation_ref = URIRef("http://example.com/policy1/obligation1")

    result = profile.parse_obligation({}, obligation_ref)

    assert result["uri"] == "http://example.com/policy1/obligation1"
    assert result["action"] == "http://www.w3.org/ns/odrl/2/attribute"


def test_extract_obligation_accepts_molgenis_obligation_class():
    """The Molgenis metadata model's own semantics annotation for the 'obligation' table uses
    odrl:Obligation rather than the real ODRL 2.2 class odrl:Duty. _extract_obligation must
    resolve resources typed either way, since we can't be sure which one Molgenis actually
    emits."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_obligation_molgenis_class.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"obligation": "http://example.com/obligation_molgenis1"}
    result = profile._extract_obligation(dataset_dict, "obligation")

    assert result["obligation"]["uri"] == "http://example.com/obligation_molgenis1"
    assert result["obligation"]["action"] == "http://www.w3.org/ns/odrl/2/attribute"


def test_extract_checksum_valid():
    """checksum pointing to an spdx:Checksum resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"checksum": "http://example.com/distribution1/checksum"}
    result = profile._extract_checksum(dataset_dict, "checksum")

    assert result["checksum"]["uri"] == "http://example.com/distribution1/checksum"
    assert result["checksum"]["checksumValue"] == "abc123def456"


def test_parse_checksum_fields():
    """parse_checksum should extract algorithm and checksumValue."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    checksum_ref = URIRef("http://example.com/distribution1/checksum")

    result = profile.parse_checksum({}, checksum_ref)

    assert result["uri"] == "http://example.com/distribution1/checksum"
    assert result["algorithm"] == "http://spdx.org/rdf/terms#checksumAlgorithm_sha256"
    assert result["checksumValue"] == "abc123def456"


def test_extract_rightsstatement_valid():
    """rights pointing to a dct:RightsStatement resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"rights": "http://example.com/distribution1/rights"}
    result = profile._extract_rightsstatement(dataset_dict, "rights")

    assert result["rights"]["uri"] == "http://example.com/distribution1/rights"
    assert result["rights"]["label"] == "Access restricted to authorised researchers"


def test_parse_rightsstatement_fields():
    """parse_rightsstatement should extract label."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_distribution_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    rights_ref = URIRef("http://example.com/distribution1/rights")

    result = profile.parse_rightsstatement({}, rights_ref)

    assert result["uri"] == "http://example.com/distribution1/rights"
    assert result["label"] == "Access restricted to authorised researchers"


def test_extract_dataservice_valid():
    """accessService pointing to a dcat:DataService resource should be replaced with a parsed
    dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_dataservice.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"accessService": "http://example.com/dataservice1"}
    result = profile._extract_dataservice(dataset_dict, "accessService")

    assert result["accessService"]["uri"] == "http://example.com/dataservice1"
    assert result["accessService"]["title"] == "Test Data Service"


def test_parse_dataservice_fields():
    """parse_dataservice should extract accessRights, applicableLegislation, conformsTo,
    contactPoint, description, endpointDescription, endPointURL, format, keyword,
    landingPage, license, publisher, theme, title. contactPoint and publisher should be
    resolved to their assigned UUIDv4 reference id, mirroring the equivalent wiring in
    parse_dataset/parse_datasetseries."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_dataservice.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    dataservice_ref = URIRef("http://example.com/dataservice1")

    result = profile.parse_dataservice({}, dataservice_ref)

    assert result["uri"] == "http://example.com/dataservice1"
    assert result["title"] == "Test Data Service"
    assert result["description"] == "A test data service"
    assert result["endPointURL"] == "http://example.com/dataservice1/api"
    assert result["endpointDescription"] == "OpenAPI specification available for this service"
    assert result["landingPage"] == "http://example.com/dataservice1/landing"
    assert result["conformsTo"] == "http://example.com/standards/fhir"
    assert result["keyword"] == "test"
    assert result["contactPoint"] == profile._get_or_create_reference_id("http://example.com/dataservice1/contact")
    assert result["publisher"] == profile._get_or_create_reference_id("http://example.com/dataservice1/publisher")


def test_extract_legalbasis_valid():
    """hasLegalBasis pointing to a dpv:LegalBasis resource should be replaced with a parsed dict."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_legal_basis.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"hasLegalBasis": "http://example.com/legalbasis1"}
    result = profile._extract_legalbasis(dataset_dict, "hasLegalBasis")

    assert result["hasLegalBasis"]["uri"] == "http://example.com/legalbasis1"
    assert result["hasLegalBasis"]["description"] == "GDPR Art. 9(2)(j)"


def test_extract_legalbasis_wrong_type_left_as_iri():
    """hasLegalBasis pointing to a resource that is not a dpv:LegalBasis should remain a plain
    IRI string."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_attribution_wrong_type.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)

    dataset_dict = {"hasLegalBasis": "http://example.com/attribution_wrong"}
    result = profile._extract_legalbasis(dataset_dict, "hasLegalBasis")

    assert result["hasLegalBasis"] == "http://example.com/attribution_wrong"


def test_parse_legal_basis_fields():
    """parse_legal_basis should extract description."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_legal_basis.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    legal_basis_ref = URIRef("http://example.com/legalbasis1")

    result = profile.parse_legal_basis({}, legal_basis_ref)

    assert result["uri"] == "http://example.com/legalbasis1"
    assert result["description"] == "GDPR Art. 9(2)(j)"


def test_parse_kind_hasemail_as_list_not_stripped():
    """When hasEmail resolves to a list (multiple vcard:hasEmail values), the mailto: prefix
    stripping should be skipped rather than raising AttributeError."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_kind_multiple_emails.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    kind_ref = URIRef("http://example.com/kind_multi")

    result = profile.parse_kind({}, kind_ref)

    assert isinstance(result["hasEmail"], list)
    assert "mailto:one@example.com" in result["hasEmail"]
    assert "mailto:two@example.com" in result["hasEmail"]


def test_parse_publisher_mbox_and_phone_fields():
    """parse_publisher should extract mbox and phone in addition to name/description/type/homepage."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_foaf_organization.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    publisher_ref = URIRef("http://example.com/org1")

    result = profile.parse_publisher({}, publisher_ref)

    assert result["name"] == "Test Publisher Org"
    assert result["mbox"] == "org1@example.com"
    assert result["phone"] == "tel:+31201234567"


def test_parse_datasetseries_pid_field_from_identifier():
    """dct:identifier on a DatasetSeries should be mapped to 'pid' (not 'id').

    The biobanks.pid column is typed 'hyperlink' in the Molgenis model, so a real PID is a
    URI, not a bare string.
    """
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_datasetseries_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    series_ref = URIRef("http://example.com/series_full2")

    result = profile.parse_datasetseries({}, series_ref)

    assert result["pid"] == "https://pid.example.com/series-pid-001"
    assert "identifier" not in result


def test_parse_datasetseries_contactpoint_resolved_to_reference_id():
    """contactPoint pointing to a vcard:Kind resource should be resolved to its assigned
    UUIDv4 reference id."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_datasetseries_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    series_ref = URIRef("http://example.com/series_full2")

    result = profile.parse_datasetseries({}, series_ref)

    assert result["contactPoint"] == profile._get_or_create_reference_id("http://example.com/series_full2/contact")


def test_parse_datasetseries_temporal_extracted_as_periodoftime():
    """temporal pointing to a dct:PeriodOfTime resource should be resolved into a parsed dict,
    mirroring the equivalent wiring already present in parse_dataset."""
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_datasetseries_full.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    series_ref = URIRef("http://example.com/series_full2")

    result = profile.parse_datasetseries({}, series_ref)

    assert result["temporal"]["startDate"] == "2015-01-01T00:00:00"
    assert result["temporal"]["endDate"] == "2019-12-31T00:00:00"


def test_parse_datasetseries_default_id_fallback_to_title(graph_datasetseries_no_id):
    """When no 'id' is otherwise present, id should fall back to the munged title (mirrors
    existing parse_datasetseries fallback behavior)."""
    profile = MolgenisEUCAIMDCATAPProfile(graph_datasetseries_no_id)
    series_ref = URIRef("http://example.com/series2")

    result = profile.parse_datasetseries({}, series_ref)

    assert result["id"] == "biobank-without-id"


def _parse_wired_dataset():
    g = rdflib.Dataset()
    g.parse("tests/test_data/extraction_dataset_wired_fields.ttl", format="turtle")
    profile = MolgenisEUCAIMDCATAPProfile(g)
    profile.config = {"pid_service_url": "https://pid.example.com", "fdp_id_prefix": "testorg"}
    dataset_ref = URIRef("http://example.com/dataset_wired")
    return profile.parse_dataset({}, dataset_ref)


def test_parse_dataset_purpose_wired_as_nested_object():
    """parse_dataset should resolve hasPurpose into hasPurpose_obj via _extract_purpose when
    the target resource is typed dpv:Purpose."""
    result = _parse_wired_dataset()

    assert result["hasPurpose_obj"]["description"] == "Wired purpose"
    assert "hasPurpose_IRI" not in result


def test_parse_dataset_creator_wired_as_object():
    """parse_dataset should resolve 'creator' into a parsed foaf:Agent dict."""
    result = _parse_wired_dataset()

    assert result["creator"]["name"] == "Wired Creator"


def test_parse_dataset_temporal_wired_as_periodoftime():
    """parse_dataset should resolve 'temporal' into a parsed dct:PeriodOfTime dict."""
    result = _parse_wired_dataset()

    assert result["temporal"]["startDate"] == "2020-01-01T00:00:00"
    assert result["temporal"]["endDate"] == "2021-01-01T00:00:00"


def test_parse_dataset_retentionperiod_wired_as_periodoftime():
    """parse_dataset should resolve 'retentionPeriod' into a parsed dct:PeriodOfTime dict."""
    result = _parse_wired_dataset()

    assert result["retentionPeriod"]["startDate"] == "2022-01-01T00:00:00"
    assert result["retentionPeriod"]["endDate"] == "2032-01-01T00:00:00"


def test_parse_dataset_qualifiedattribution_wired():
    """parse_dataset should resolve 'qualifiedAttribution' into a parsed prov:Attribution dict."""
    result = _parse_wired_dataset()

    assert result["qualifiedAttribution"]["agent"]["name"] == "Wired Attribution Agent"


def test_parse_dataset_other_identifier_wired():
    """parse_dataset should resolve 'other_identifier' into a parsed adms:Identifier dict."""
    result = _parse_wired_dataset()

    assert result["other_identifier"]["notation"] == "WIRED-001"


def test_parse_dataset_legalbasis_wired_as_object():
    """parse_dataset should resolve 'hasLegalBasis' into a parsed dpv:LegalBasis dict."""
    result = _parse_wired_dataset()

    assert result["hasLegalBasis"]["description"] == "Wired legal basis"


def test_parse_dataset_sample_wired_as_distribution():
    """parse_dataset should resolve 'sample' into a parsed dcat:Distribution dict."""
    result = _parse_wired_dataset()

    assert result["sample"]["title"] == "Wired Sample Distribution"


def test_parse_dataset_analytics_wired_as_distribution():
    """parse_dataset should resolve 'analytics' into a parsed dcat:Distribution dict."""
    result = _parse_wired_dataset()

    assert result["analytics"]["title"] == "Wired Analytics Distribution"
