# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI
# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://raw.githubusercontent.com/ckan/ckanext-dcat/master/ckanext/dcat/profiles/euro_dcat_ap.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN
from datetime import datetime
from typing import Dict, Union
from urllib.parse import urlparse

from rdflib import URIRef, FOAF, RDF, RDFS, PROV

import logging

from .baseharvester import munge_title_to_name
from .baseparser import (
    RDFProfile, VCARD, EUCAIM, HEALTHDCATAP, DCT, DCAT, ADMS, DCATAP, DPV
)

log = logging.getLogger(__name__)

def generate_id(arr):
    """
    To link any auxiliary classes to the properties the IDs of these classes need to be calculated in the same way
    as is done on the Molgenis side. Currently this pseudo-hashing function is used.
    """
    h = 5381
    items = [str(x) for x in arr]
    for c in '\0'.join(sorted(items)):
        h = (h * 33) ^ ord(c)
    return h % (2**32)

def check_url(input_string: str):
    """
    Check if the string is a URL.
    """
    parsed = urlparse(input_string)
    return bool(parsed.scheme and parsed.netloc)


class MolgenisEUCAIMDCATAPProfile(RDFProfile):
    """RDF profile for EUCAIM DCAT-AP data mapping to Molgenis."""

    def _extract_concept_dict(self, concept_ref, concept_dict: Dict, 
                            field_mappings: tuple) -> Dict:
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
            dataset_dict: Dict,
            key: str,
            expected_types: Union[list, URIRef],
            extraction_fn
    ) -> Dict:
        """
        Generic method to extract and transform RDF properties based on type checking.

        Args:
            dataset_dict: Dictionary to modify
            key: Key to look up and modify in the dictionary
            expected_types: List of RDF types to check against
            extraction_fn: Function to extract and transform the value
                          Takes (self, uri_ref, dataset_dict) and returns the transformed value

        Returns:
            Modified dataset_dict
        """
        value = dataset_dict.get(key)
        if not value:
            return dataset_dict

        uri_ref = URIRef(value)
        rdf_type = self._object_value(uri_ref, RDF.type)
        # Normalize to list
        if not isinstance(rdf_type, list):
            rdf_type = [rdf_type]
        if not isinstance(expected_types, list):
            expected_types = [expected_types]
        expected_types = [str(exp_type) for exp_type in expected_types]

        # Simple membership check
        if any(t in expected_types for t in rdf_type):
            dataset_dict[key] = extraction_fn(uri_ref, dataset_dict)

        return dataset_dict

    def _get_dataset_field_mappings(self):
        """Get field mappings for dataset parsing."""
        return (
            ("title", DCT.title),
            ("description", DCT.description),
            ("in_series", DCAT.inSeries),
            ("theme", DCAT.theme),
            ("provenance", DCT.provenance),
            ("keyword", DCAT.keyword),
            ("hasPurpose", DPV.hasPurpose),
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
            # ("hasQualityAnnotation", DPV.hasQualityAnnotation),
            ("creator", DCT.creator),
            ("spatialResolutionInMeters", DCAT.spatialResolutionInMeters),
            # ("qualifiedAttribution", PROV.qualifiedAttribution),
            # ("other_identifier", ADMS.identifier),
            ("versionNotes", ADMS.versionNotes),
            ("issued", DCT.issued),
            ("modified", DCT.modified),
        )

    def _extract_name_vcard(self, dataset_dict: Dict, key: str):
        def extraction(uri_ref, _):
            return self._object_value(uri_ref, VCARD.fn).lower().replace(' ', '')
        return self._extract_and_transform_by_type(dataset_dict, key, VCARD.Kind, extraction)

    def _extract_name_publisher(self, dataset_dict: Dict, key: str):
        def extraction(uri_ref, _):
            return self._object_value(uri_ref, FOAF.name).lower().replace(' ', '')
        return self._extract_and_transform_by_type(dataset_dict, key, FOAF.Organization, extraction)

    def _extract_provenancestatement_label(self, dataset_dict: Dict, key: str):
        def extraction(uri_ref, _):
            label_list = self._object_value(uri_ref, RDFS.label)
            if not isinstance(label_list, list):
                label_list = [label_list]
            return generate_id(label_list)
        return self._extract_and_transform_by_type(dataset_dict, key, DCT.ProvenanceStatement, extraction)

    def _extract_purpose(self, dataset_dict: Dict, key: str):
        def extraction(uri_ref, _):
            label_list = self._object_value(uri_ref, DCT.description)
            if not isinstance(label_list, list):
                label_list = [label_list]
            return generate_id(label_list)
        dataset_dict = self._extract_and_transform_by_type(dataset_dict, key, DPV.Purpose, extraction)
        if dataset_dict.get(key):
            if not check_url(str(dataset_dict[key])):
                dataset_dict[f'{key}_obj'] = dataset_dict[key]
            else:
                dataset_dict[f'{key}_IRI'] = dataset_dict[key]
            del dataset_dict[key]
        return dataset_dict

    def _extract_datasetseries_id(self, dataset_dict: Dict):
        if dataset_dict.get('in_series'):
            original_value = URIRef(dataset_dict['in_series'])
            retrieved_class = self._object_value(original_value, RDF.type)
            if any([val in [str(DCAT.DatasetSeries)] for val in retrieved_class]):
                dataset_dict['in_series'] = str(self._object_value(original_value, DCT.identifier))
                if dataset_dict['in_series'] == '':
                    dataset_dict['in_series'] = munge_title_to_name(str(self._object_value(original_value, DCT.title)))
        return dataset_dict

    def _remove_default_language(self, dataset_dict: Dict):
        if dataset_dict.get('language'):
            language_list = dataset_dict['language']
            if not isinstance(language_list, list):
                language_list = [language_list]
            try:
                language_list.remove('http://id.loc.gov/vocabulary/iso639-1/en')
                if not language_list:
                    # If removing the default language makes language_list empty, remove the dictionary entry.
                    del dataset_dict['language']
                else:
                    dataset_dict['language'] = language_list
            except ValueError:
                pass
        return dataset_dict

    def handle_pids(self, dataset_dict: Dict):
        # This method is only used for datasets
        # For datasets it is required that there is an 'id' and 'identifier' column.
        # If the source contains dct:identifier, it is mapped to 'identifier'.
        # If 'identifier' is a hyperlink it is assumed to be a proper PID.
        # If 'identifier' is not a hyperlink, the EUCAIM PID service will be used.
        pid_bool = check_url(dataset_dict['identifier'])

        pid_service_url = self.config.get('pid_service_url')
        if pid_bool:
            ## The source contains a PID
            # The PID will be provided as dct:identifier, and mapped to 'identifier'
            # If 'identifier' was previously generated by the PID service, recover the stable
            # 'id' that was assigned in the first pass rather than re-deriving it from the URL.
            # Otherwise treat it as an external PID and sanitize it into 'id'.
            identifier = str(dataset_dict['identifier'])
            if identifier.startswith(pid_service_url + '/'):
                dataset_dict['id'] = identifier[len(pid_service_url) + 1:]
            else:
                dataset_dict['id'] = munge_title_to_name(identifier)
        else:
            ## The source does not contain a PID
            # 'identifier' will be a string; it needs to be prefixed with the organization code.
            # The original ID with the prefix will be 'id'
            # 'identifier' will be replaced by the EUCAIM PID service URL + org prefix + original dataset.
            fdp_id_prefix = self.config.get('fdp_id_prefix')
            if fdp_id_prefix:
                dataset_dict['id'] = f"{fdp_id_prefix}-{dataset_dict['identifier']}"
            else:
                dataset_dict['id'] = dataset_dict['identifier']
            dataset_dict['identifier'] = f"{pid_service_url}/{dataset_dict['id']}"

        return dataset_dict

    def parse_dataset(self, dataset_dict: Dict, dataset_ref: URIRef) -> Dict:
        """Parse dataset from RDF reference into dictionary."""
        dataset_dict["uri"] = str(dataset_ref)

        field_mappings = self._get_dataset_field_mappings()

        ### Extract hasPurpose to hasPurpose_obj or hasPurpose_IRI

        dataset_dict = self._extract_concept_dict(
            dataset_ref, dataset_dict, field_mappings
        )
        dataset_dict = self.handle_pids(dataset_dict)
        dataset_dict = self._remove_default_language(dataset_dict)
        dataset_dict = self._extract_name_vcard(dataset_dict, 'contactPoint')
        dataset_dict = self._extract_name_publisher(dataset_dict, 'publisher')
        dataset_dict = self._extract_provenancestatement_label(dataset_dict, 'provenance')
        dataset_dict = self._extract_datasetseries_id(dataset_dict)
        dataset_dict = self._extract_purpose(dataset_dict, 'hasPurpose')

        return dataset_dict

    def parse_datasetseries(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        # Basic fields
        key_predicate_tuple = (
            ("id", DCT.identifier),
            ("title", DCT.title),
            ("description", DCT.description),
            ("temporal", DCT.temporal),
            ("applicableLegislation", DCATAP.applicableLegislation),
            ("accrualPeriodicity", DCT.accrualPeriodicity),
            ("spatial", DCT.spatial),
            ("publisher", DCT.publisher),
            ("modified", DCT.modified),
            ("issued", DCT.issued),
            ("contactPoint", DCAT.contactPoint),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

        dataset_dict = self._extract_name_vcard(dataset_dict, 'contactPoint')
        dataset_dict = self._extract_name_publisher(dataset_dict, 'publisher')

        if not dataset_dict.get('id', False):
            dataset_dict['id'] = munge_title_to_name(dataset_dict["title"])

        return dataset_dict

    def parse_publisher(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("name", FOAF.name),
            ("description", DCT.description),
            ("publishertype", HEALTHDCATAP.publishertype),
            ("homepage", FOAF.homepage),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        return dataset_dict

    def parse_kind(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("fn", VCARD.fn),
            ("hasEmail", VCARD.hasEmail),
            ("hasURL", VCARD.hasURL),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

        if dataset_dict.get("hasEmail") and dataset_dict["hasEmail"].startswith("mailto:"):
            dataset_dict["hasEmail"] = dataset_dict["hasEmail"].removeprefix("mailto:")
        return dataset_dict

    def parse_provenancestatement(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("label", RDFS.label),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        return dataset_dict

    def parse_purpose(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        key_predicate_tuple = (
            ("description", DCT.description),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        print(dataset_dict)
        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")