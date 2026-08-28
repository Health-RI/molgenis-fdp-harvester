# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI
# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://raw.githubusercontent.com/ckan/ckanext-dcat/master/ckanext/dcat/profiles/euro_dcat_ap.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN
import logging
import uuid

from rdflib import FOAF, PROV, RDF, RDFS, URIRef

from molgenis_fdp_harvester.utils import HarvesterException

from .baseharvester import munge_title_to_name
from .baseparser import ADMS, DCAT, DCATAP, DCT, DPV, EUCAIM, HEALTHDCATAP, ODRL, SKOS, SPDX, VCARD, RDFProfile

log = logging.getLogger(__name__)

# This is an entry that needs to be removed from the language list. We're not using it so the http protocol unsafety can
# be ignored.
DEFAULT_LANGUAGE = "http://id.loc.gov/vocabulary/iso639-1/en"  # NOSONAR


class MolgenisEUCAIMDCATAPProfile(RDFProfile):
    """RDF profile for EUCAIM DCAT-AP data mapping to Molgenis."""

    def __init__(self, graph, compatibility_mode=False):
        super().__init__(graph, compatibility_mode=compatibility_mode)
        # Supplementary classes (Agents, contact Kinds, ProvenanceStatements, ...) are
        # created with a UUIDv4 internal ID. The same RDF resource can be referenced by
        # multiple datasets, so this cache ensures it resolves to the same ID everywhere
        # it's referenced, for as long as this profile instance lives (one harvest run).
        # Harvesting is single-threaded; this cache is not safe for concurrent access.
        self._reference_ids: dict[str, str] = {}

    def _get_or_create_reference_id(self, uri: str) -> str:
        if uri not in self._reference_ids:
            self._reference_ids[uri] = str(uuid.uuid4())
        return self._reference_ids[uri]

    def _resolve_reference_id(self, uri_ref, _dataset_dict=None):
        return self._get_or_create_reference_id(str(uri_ref))

    @staticmethod
    def _strip_mailto_prefix(value):
        """Strip a 'mailto:' prefix from an email value for Molgenis's plain-string email columns."""
        if isinstance(value, str) and value.startswith("mailto:"):
            return value.removeprefix("mailto:")
        return value

    def _extract_concept_dict(self, concept_ref, concept_dict: dict, field_mappings: tuple) -> dict:
        """Extract RDF properties into a concept dictionary."""
        for field_name, predicate in field_mappings:
            value = self._object_value(concept_ref, predicate)
            if not value:
                continue

            # Handle single-item lists
            if isinstance(value, list) and len(value) == 1:
                value = value[0]

            concept_dict[field_name] = value
        return concept_dict

    def _extract_and_transform_by_type(
        self,
        dataset_dict: dict,
        key: str,
        expected_types: list | URIRef,
        extraction_fn,
    ) -> dict:
        """
        Generic method to extract and transform RDF properties based on type checking.

        Many Molgenis columns backing `key` are ref_array (e.g. contactPoint, creator,
        qualifiedAttribution, sample/analytics, other_identifier, temporal), so `value` may be
        either a single IRI string or a list of IRI strings. Each referenced resource is
        type-checked and transformed independently; resources that don't match
        `expected_types` are left as plain IRI strings, same as the single-value case.

        Args:
            dataset_dict: Dictionary to modify
            key: Key to look up and modify in the dictionary
            expected_types: List of RDF types to check against
            extraction_fn: Function to extract and transform the value
                          Takes (uri_ref, dataset_dict) and returns the transformed value

        Returns:
            Modified dataset_dict
        """
        value = dataset_dict.get(key)
        if not value:
            return dataset_dict

        if not isinstance(expected_types, list):
            expected_types = [expected_types]
        expected_types = [str(exp_type) for exp_type in expected_types]

        def resolve(single_value):
            uri_ref = URIRef(single_value)
            rdf_type = self._object_value(uri_ref, RDF.type)
            # Normalize to list
            if not isinstance(rdf_type, list):
                rdf_type = [rdf_type]

            # Simple membership check
            if any(t in expected_types for t in rdf_type):
                return extraction_fn(uri_ref, dataset_dict)
            return single_value

        if isinstance(value, list):
            dataset_dict[key] = [resolve(v) for v in value]
        else:
            dataset_dict[key] = resolve(value)

        return dataset_dict

    def _get_dataset_field_mappings(self):
        """Get field mappings for dataset parsing."""
        return (
            ("title", DCT.title),
            ("description", DCT.description),
            ("theme", DCAT.theme),
            ("provenance", DCT.provenance),
            ("keyword", DCAT.keyword),
            ("hasPurpose_obj", DPV.hasPurpose),
            ("hasPurpose_IRI", DPV.hasPurpose),
            ("accessRights", DCT.accessRights),
            ("healthCategory", HEALTHDCATAP.healthCategory),
            ("healthTheme", HEALTHDCATAP.healthTheme),
            ("spatial", DCT.spatial),
            ("applicableLegislation", DCATAP.applicableLegislation),
            ("contactPoint", DCAT.contactPoint),
            ("publisher", DCT.publisher),
            ("type", DCT.type),
            ("maxTypicalAge", HEALTHDCATAP.maxTypicalAge),
            ("minTypicalAge", HEALTHDCATAP.minTypicalAge),
            ("hasBirthSex", EUCAIM.hasBirthSex),
            ("numberOfRecords", HEALTHDCATAP.numberOfRecords),
            ("numberOfUniqueIndividuals", HEALTHDCATAP.numberOfUniqueIndividuals),
            ("collectionMethod", EUCAIM.collectionMethod),
            ("temporal", DCT.temporal),
            ("hasCondition", EUCAIM.hasCondition),
            ("hasImageModality", EUCAIM.hasImageModality),
            ("hasEquipmentManufacturer", EUCAIM.hasEquipmentManufacturer),
            ("hasImageBodyPart", EUCAIM.hasImageBodyPart),
            ("hasAnnotationLabel", EUCAIM.hasAnnotationLabel),
            ("hasAlgorithmType", EUCAIM.hasAlgorithmType),
            ("nbrOfSegmentations", EUCAIM.nbrOfSegmentations),
            ("identifier", DCT.identifier),
            ("version", DCAT.version),
            ("interoperabilityLevel", ADMS.interoperabilityLevel),
            ("language", DCT.language),
            ("populationCoverage", HEALTHDCATAP.populationCoverage),
            ("hasPersonalData", DPV.hasPersonalData),
            ("temporalResolution", DCAT.temporalResolution),
            ("accrualPeriodicity", DCT.accrualPeriodicity),
            ("hasLegalBasis", DPV.hasLegalBasis),
            ("retentionPeriod", HEALTHDCATAP.retentionPeriod),
            ("conformsTo", DCT.conformsTo),
            ("hasCodingSystem", HEALTHDCATAP.hasCodingSystem),
            ("hasCodeValues", HEALTHDCATAP.hasCodeValues),
            ("relation", DCT.relation),
            ("isReferencedBy", DCT.isReferencedBy),
            ("landingPage", DCAT.landingPage),
            ("page", FOAF.page),
            ("sample", ADMS.sample),
            ("analytics", HEALTHDCATAP.analytics),
            ("creator", DCT.creator),
            ("wasGeneratedBy", PROV.wasGeneratedBy),
            ("spatialResolutionInMeters", DCAT.spatialResolutionInMeters),
            ("qualifiedAttribution", PROV.qualifiedAttribution),
            ("other_identifier", ADMS.identifier),
            ("versionNotes", ADMS.versionNotes),
            ("issued", DCT.issued),
            ("modified", DCT.modified),
            ("in_series", DCAT.inSeries),
        )

    def _extract_datasetseries_id(self, dataset_dict: dict):
        if dataset_dict.get("in_series"):
            original_value = URIRef(dataset_dict["in_series"])
            retrieved_class = self._object_value(original_value, RDF.type)
            if any(val in [str(DCAT.DatasetSeries)] for val in retrieved_class):
                dataset_dict["in_series"] = str(self._object_value(original_value, DCT.identifier))
                if dataset_dict["in_series"] == "":
                    dataset_dict["in_series"] = munge_title_to_name(str(self._object_value(original_value, DCT.title)))
        return dataset_dict

    def _extract_creator(self, dataset_dict: dict, key: str):
        return self._extract_and_transform_by_type(dataset_dict, key, FOAF.Agent, self._resolve_reference_id)

    def _extract_periodoftime(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_periodoftime({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, DCT.PeriodOfTime, extraction)

    def _extract_attribution(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_attribution({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, PROV.Attribution, extraction)

    def _extract_attribution_agent(self, dataset_dict: dict, key: str):
        return self._extract_and_transform_by_type(dataset_dict, key, FOAF.Agent, self._resolve_reference_id)

    def _extract_other_identifier(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_other_identifier({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, ADMS.Identifier, extraction)

    def _extract_distribution(self, dataset_dict: dict, key: str):
        return self._extract_and_transform_by_type(dataset_dict, key, DCAT.Distribution, self._resolve_reference_id)

    def _extract_policy(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_policy({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, ODRL.Policy, extraction)

    def _extract_checksum(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_checksum({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, SPDX.Checksum, extraction)

    def _extract_rightsstatement(self, dataset_dict: dict, key: str):
        return self._extract_and_transform_by_type(dataset_dict, key, DCT.RightsStatement, self._resolve_reference_id)

    def _extract_dataservice(self, dataset_dict: dict, key: str):
        return self._extract_and_transform_by_type(dataset_dict, key, DCAT.DataService, self._resolve_reference_id)

    def _extract_permission(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_permission({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, ODRL.Permission, extraction)

    def _extract_prohibition(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_prohibition({}, uri_ref)

        return self._extract_and_transform_by_type(dataset_dict, key, ODRL.Prohibition, extraction)

    def _extract_obligation(self, dataset_dict: dict, key: str):
        def extraction(uri_ref, _):
            return self.parse_obligation({}, uri_ref)

        # The real ODRL 2.2 vocabulary names this class Duty, but the Molgenis metadata model's
        # own semantics annotation for the 'obligation' table uses odrl:Obligation instead.
        # Accept either so we resolve regardless of which one Molgenis actually emits.
        return self._extract_and_transform_by_type(dataset_dict, key, [ODRL.Duty, ODRL.Obligation], extraction)

    def _extract_legalbasis(self, dataset_dict: dict, key: str):
        return self._extract_and_transform_by_type(dataset_dict, key, DPV.LegalBasis, self._resolve_reference_id)

    def _extract_purpose(self, dataset_dict: dict):
        """Resolve hasPurpose to nested Purpose object(s) and/or plain vocabulary IRI(s).

        dpv:hasPurpose is a Molgenis ref_array, so a dataset may declare more than one
        value; each is independently resolved into either hasPurpose_obj (dpv:Purpose
        resources) or hasPurpose_IRI (plain vocabulary terms).
        """
        purpose_value = dataset_dict.get("hasPurpose_obj")
        if not purpose_value:
            dataset_dict.pop("hasPurpose_obj", None)
            return dataset_dict

        purpose_values = purpose_value if isinstance(purpose_value, list) else [purpose_value]
        purpose_objects, purpose_iris = self._split_purpose_values(purpose_values)

        self._set_or_pop_value(dataset_dict, "hasPurpose_obj", purpose_objects)
        self._set_or_pop_value(dataset_dict, "hasPurpose_IRI", purpose_iris)
        return dataset_dict

    def _split_purpose_values(self, purpose_values: list) -> tuple[list, list]:
        """Split hasPurpose values into resolved dpv:Purpose objects and plain vocabulary IRIs.

        dpv:Purpose resources are independently harvested and upserted into their own
        MOLGENIS table (see `RDFParser.purpose`), so a dataset only needs to reference
        them by id here, the same way contactPoint/publisher/provenance are resolved to
        their cached UUIDv4 rather than the full nested object.
        """
        purpose_objects = []
        purpose_iris = []
        for value in purpose_values:
            purpose_ref = URIRef(value)
            purpose_type = self._object_value(purpose_ref, RDF.type)
            if not isinstance(purpose_type, list):
                purpose_type = [purpose_type]

            if str(DPV.Purpose) in purpose_type:
                purpose_objects.append(self._resolve_reference_id(purpose_ref))
            else:
                purpose_iris.append(value)
        return purpose_objects, purpose_iris

    @staticmethod
    def _set_or_pop_value(dataset_dict: dict, key: str, values: list) -> None:
        """Set `key` to the single value or list of `values`, or drop `key` if empty."""
        if values:
            dataset_dict[key] = values[0] if len(values) == 1 else values
        else:
            dataset_dict.pop(key, None)

    def _remove_default_language(self, dataset_dict: dict):
        if dataset_dict.get("language"):
            language_list = dataset_dict["language"]
            if not isinstance(language_list, list):
                language_list = [language_list]
            try:
                language_list.remove(DEFAULT_LANGUAGE)
                if not language_list:
                    # If removing the default language makes language_list empty, remove the dictionary entry.
                    del dataset_dict["language"]
                else:
                    dataset_dict["language"] = language_list
            except ValueError:
                pass
        return dataset_dict

    def handle_pids(self, dataset_dict: dict):
        original_identifier = dataset_dict.get("identifier")
        if not original_identifier or not str(original_identifier).strip():
            raise ValueError("dataset_dict is missing a non-empty 'identifier'")

        pid_service_url = self.config.get("pid_service_url")
        if not pid_service_url or not str(pid_service_url).strip():
            raise ValueError("pid_service_url is not configured")

        original_identifier = str(original_identifier)
        molgenis_id = str(uuid.uuid4())

        dataset_dict["id"] = molgenis_id
        other_identifier = dataset_dict.get("other_identifier")
        if not other_identifier:
            dataset_dict["other_identifier"] = original_identifier
        elif isinstance(other_identifier, list):
            dataset_dict["other_identifier"] = [*other_identifier, original_identifier]
        else:
            dataset_dict["other_identifier"] = [other_identifier, original_identifier]
        dataset_dict["identifier"] = f"{pid_service_url}/{molgenis_id}"

        return dataset_dict

    def parse_dataset(self, dataset_dict: dict, dataset_ref: URIRef) -> dict:
        """Parse dataset from RDF reference into dictionary."""
        dataset_dict["uri"] = str(dataset_ref)

        field_mappings = self._get_dataset_field_mappings()

        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, field_mappings)
        dataset_dict = self.handle_pids(dataset_dict)
        dataset_dict = self._remove_default_language(dataset_dict)
        dataset_dict = self._extract_and_transform_by_type(
            dataset_dict, "contactPoint", VCARD.Kind, self._resolve_reference_id
        )
        dataset_dict = self._extract_and_transform_by_type(
            dataset_dict, "publisher", FOAF.Organization, self._resolve_reference_id
        )
        dataset_dict = self._extract_and_transform_by_type(
            dataset_dict, "provenance", DCT.ProvenanceStatement, self._resolve_reference_id
        )
        dataset_dict = self._extract_datasetseries_id(dataset_dict)
        dataset_dict = self._extract_purpose(dataset_dict)
        dataset_dict = self._extract_legalbasis(dataset_dict, "hasLegalBasis")
        dataset_dict = self._extract_creator(dataset_dict, "creator")
        dataset_dict = self._extract_periodoftime(dataset_dict, "temporal")
        dataset_dict = self._extract_periodoftime(dataset_dict, "retentionPeriod")
        dataset_dict = self._extract_attribution(dataset_dict, "qualifiedAttribution")
        dataset_dict = self._extract_other_identifier(dataset_dict, "other_identifier")
        dataset_dict = self._extract_distribution(dataset_dict, "sample")
        return self._extract_distribution(dataset_dict, "analytics")

    def parse_datasetseries(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("title", DCT.title),
            ("description", DCT.description),
            ("temporal", DCT.temporal),
            ("applicableLegislation", DCATAP.applicableLegislation),
            ("contactPoint", DCAT.contactPoint),
            ("accrualPeriodicity", DCT.accrualPeriodicity),
            ("spatial", DCT.spatial),
            ("modified", DCT.modified),
            ("publisher", DCT.publisher),
            ("issued", DCT.issued),
            ("pid", DCT.identifier),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

        dataset_dict = self._extract_and_transform_by_type(
            dataset_dict, "contactPoint", VCARD.Kind, self._resolve_reference_id
        )
        dataset_dict = self._extract_and_transform_by_type(
            dataset_dict, "publisher", FOAF.Organization, self._resolve_reference_id
        )
        dataset_dict = self._extract_periodoftime(dataset_dict, "temporal")

        if not dataset_dict.get("id", False):
            dataset_dict["id"] = munge_title_to_name(dataset_dict["title"])

        return dataset_dict

    def parse_publisher(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        key_predicate_tuple = (
            ("name", FOAF.name),
            ("description", DCT.description),
            ("type", DCT.type),
            ("mbox", FOAF.mbox),
            ("homepage", FOAF.homepage),
            ("phone", FOAF.phone),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        if "mbox" in dataset_dict:
            dataset_dict["mbox"] = self._strip_mailto_prefix(dataset_dict["mbox"])
        return dataset_dict

    def _parse_agent_like(self, dataset_dict: dict, dataset_ref: URIRef):
        """Shared body for FOAF.Agent-shaped concepts (name/description/type/mbox/homepage)."""
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("name", FOAF.name),
            ("description", DCT.description),
            ("type", DCT.type),
            ("mbox", FOAF.mbox),
            ("homepage", FOAF.homepage),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        if "mbox" in dataset_dict:
            dataset_dict["mbox"] = self._strip_mailto_prefix(dataset_dict["mbox"])
        return dataset_dict

    def _parse_single_field_concept(self, dataset_dict: dict, dataset_ref: URIRef, field_name: str, predicate):
        """Shared body for concepts with a single field plus a generated id."""
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = ((field_name, predicate),)
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict["id"] = str(uuid.uuid4())
        return dataset_dict

    def parse_creator(self, dataset_dict: dict, dataset_ref: URIRef):
        return self._parse_agent_like(dataset_dict, dataset_ref)

    def parse_legal_basis(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        key_predicate_tuple = (("description", DCT.description),)
        return self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

    def parse_kind(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        key_predicate_tuple = (
            ("fn", VCARD.fn),
            ("hasEmail", VCARD.hasEmail),
            ("hasURL", VCARD.hasURL),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        if "hasEmail" in dataset_dict:
            dataset_dict["hasEmail"] = self._strip_mailto_prefix(dataset_dict["hasEmail"])
        return dataset_dict

    def parse_distribution(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("status", ADMS.status),
            ("accessService", DCAT.accessService),
            ("accessURL", DCAT.accessURL),
            ("byteSize", DCAT.byteSize),
            ("compressFormat", DCAT.compressFormat),
            ("downloadURL", DCAT.downloadURL),
            ("mediaType", DCAT.mediaType),
            ("packageFormat", DCAT.packageFormat),
            ("spatialResolutionInMeters", DCAT.spatialResolutionInMeters),
            ("applicableLegislation", DCATAP.applicableLegislation),
            ("availability", DCATAP.availability),
            ("conformsTo", DCT.conformsTo),
            ("description", DCT.description),
            ("format", DCT.format),
            ("issued", DCT.issued),
            ("language", DCT.language),
            ("license", DCT.license),
            ("modified", DCT.modified),
            ("rights", DCT.rights),
            ("temporal", DCT.temporal),
            ("title", DCT.title),
            ("page", FOAF.page),
            ("hasPolicy", ODRL.hasPolicy),
            ("checksum", SPDX.checksum),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        dataset_dict = self._extract_policy(dataset_dict, "hasPolicy")
        dataset_dict = self._extract_checksum(dataset_dict, "checksum")
        dataset_dict = self._extract_rightsstatement(dataset_dict, "rights")
        return self._extract_dataservice(dataset_dict, "accessService")

    def parse_dataservice(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("accessRights", DCT.accessRights),
            ("applicableLegislation", DCATAP.applicableLegislation),
            ("conformsTo", DCT.conformsTo),
            ("contactPoint", DCAT.contactPoint),
            ("description", DCT.description),
            ("endpointDescription", DCAT.endpointDescription),
            ("endpointURL", DCAT.endpointURL),
            ("format", DCT.format),
            ("keyword", DCAT.keyword),
            ("landingPage", DCAT.landingPage),
            ("license", DCT.license),
            ("publisher", DCT.publisher),
            ("theme", DCAT.theme),
            ("title", DCT.title),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        dataset_dict = self._extract_and_transform_by_type(
            dataset_dict, "contactPoint", VCARD.Kind, self._resolve_reference_id
        )
        return self._extract_and_transform_by_type(
            dataset_dict, "publisher", FOAF.Organization, self._resolve_reference_id
        )

    def parse_provenancestatement(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        key_predicate_tuple = (("label", RDFS.label),)
        return self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

    def parse_purpose(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        key_predicate_tuple = (("description", DCT.description),)
        return self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

    def parse_other_identifier(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("notation", SKOS.notation),
            ("schemaAgency", ADMS.schemaAgency),
        )
        return self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

    def parse_periodoftime(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("startDate", DCAT.startDate),
            ("endDate", DCAT.endDate),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

        start_date = dataset_dict.get("startDate")
        if start_date is None:
            raise HarvesterException(f"No start date provided for {dataset_dict}")

        end_date = dataset_dict.get("endDate")
        if end_date is None:
            raise HarvesterException(f"No end date provided for {dataset_dict}")

        dataset_dict["id"] = f"{start_date}/{end_date}"
        return dataset_dict

    def parse_attribution(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("agent", PROV.agent),
            ("hadRole", DCAT.hadRole),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict["id"] = str(uuid.uuid4())
        return self._extract_attribution_agent(dataset_dict, "agent")

    def parse_attribution_agent(self, dataset_dict: dict, dataset_ref: URIRef):
        return self._parse_agent_like(dataset_dict, dataset_ref)

    def parse_checksum(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("algorithm", SPDX.algorithm),
            ("checksumValue", SPDX.checksumValue),
        )
        return self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

    def parse_rightsstatement(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        dataset_dict["id"] = self._get_or_create_reference_id(dataset_dict["uri"])
        key_predicate_tuple = (("label", RDFS.label),)
        return self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

    def parse_policy(self, dataset_dict: dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("permission", ODRL.permission),
            ("prohibition", ODRL.prohibition),
            ("obligation", ODRL.obligation),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict["id"] = str(uuid.uuid4())
        dataset_dict = self._extract_permission(dataset_dict, "permission")
        dataset_dict = self._extract_prohibition(dataset_dict, "prohibition")
        return self._extract_obligation(dataset_dict, "obligation")

    def parse_permission(self, dataset_dict: dict, dataset_ref: URIRef):
        return self._parse_single_field_concept(dataset_dict, dataset_ref, "action", ODRL.action)

    # ODRL's Permission, Prohibition and Obligation rules all carry a single odrl:action;
    # parse_prohibition/parse_obligation alias parse_permission rather than redefine it.
    parse_prohibition = parse_permission
    parse_obligation = parse_permission

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")
