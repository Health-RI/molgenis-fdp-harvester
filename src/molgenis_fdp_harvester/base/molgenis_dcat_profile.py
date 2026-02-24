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
from typing import Dict

from rdflib import URIRef, FOAF, RDF, RDFS

import logging

from .baseharvester import munge_title_to_name
from .baseparser import RDFProfile, VCARD, EUCAIM, HEALTHDCATAP
from .baseparser import (
    DCT, DCAT, ADMS
)

log = logging.getLogger(__name__)

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

    def _get_dataset_field_mappings(self):
        """Get field mappings for dataset parsing."""
        return (
            ("id", DCT.identifier),
            ("name", DCT.title),
            ("acronym", DCT.alternative),
            ("description", DCT.description),
            ("biobank", DCAT.inSeries),
            ("provider", DCT.publisher),
            ("imaging_modality", EUCAIM.hasImageModality),
            ("geographical_coverage", DCT.spatial),
            ("type", DCT.type),
            ("intended_purpose", URIRef("https://w3id.org/dpv#hasPurpose")),
            ("image_access_type", DCT.accessRights),
            ("collection_method", EUCAIM.collectionMethod),
            ("number_of_subjects", HEALTHDCATAP.numberOfUniqueIndividuals),
            ("number_of_records", HEALTHDCATAP.numberOfRecords),
            ("body_part_examined", EUCAIM.hasImageBodyPart),
            ("condition", EUCAIM.hasCondition),
            ("vendor", EUCAIM.hasImageVendor),
            ("image_year_range", DCT.temporal),
            ("sex", EUCAIM.hasBirthSex),
            ("age_high", HEALTHDCATAP.maxTypicalAge),
            ("age_low", HEALTHDCATAP.minTypicalAge),
            ("theme", DCAT.theme),
            ("interoperability_tier", ADMS.interoperabilityLevel),
            ("provenance", DCT.provenance),
            ("intented_purpose", URIRef("https://w3id.org/dpv/dpv-skos#hasPurpose")),
            ("applicable_legislation", URIRef("http://data.europa.eu/r5r/applicableLegislation")),
            ("legal_basis", URIRef("https://w3id.org/dpv/dpv-skos#hasLegalBasis")),
            ("retention_period", URIRef("http://www.healthdcatap.org/retentionPeriod")),
            ("rights", DCT.rights),
            ("quality_label", URIRef("http://www.w3.org/ns/dqv#hasQualityAnnotation")),
            ("coding_systems", URIRef("http://www.healthdcatap.org/hasCodingSystem")),
            ("last_modified", DCT.modified),
            ("version", DCAT.version),
            ("publisherType", HEALTHDCATAP.publishertype),
            ("format", DCT.format),
            ("contact", DCAT.contactPoint)
        )

    def _extract_name_vcard(self, dataset_dict: Dict, key: str):
        if dataset_dict.get(key):
            contact_uri = URIRef(dataset_dict[key])
            contact_point_class = self._object_value(contact_uri, RDF.type)
            if any([val == str(VCARD.Kind) for val in contact_point_class]):
                dataset_dict[key] = self._object_value(contact_uri, VCARD.fn).lower().replace(' ','-')
        return dataset_dict

    def _extract_name_agent(self, dataset_dict: Dict, key: str):
        if dataset_dict.get(key):
            provider_uri = URIRef(dataset_dict[key])
            provider_class = self._object_value(provider_uri, RDF.type)
            if any([val in [str(FOAF.Agent), str(FOAF.Person), str(FOAF.Organization)] for val in provider_class]):
                dataset_dict[key] = self._object_value(provider_uri, FOAF.name)
        return dataset_dict

    def _convert_image_year_range(self, dataset_dict: Dict):
        if dataset_dict.get('image_year_range'):
            original_value = URIRef(dataset_dict['image_year_range'])
            retrieved_class = self._object_value(original_value, RDF.type)
            if any([val in [str(DCT.PeriodOfTime)] for val in retrieved_class]):
                start_date = datetime.fromisoformat(self._object_value(original_value, DCAT.startDate)).date()
                end_date = datetime.fromisoformat(self._object_value(original_value, DCAT.endDate)).date()
                dataset_dict['image_year_range'] = f"{start_date} - {end_date}"
        return dataset_dict

    def _extract_provenancestatement_label(self, dataset_dict: Dict):
        if dataset_dict.get('provenance'):
            original_value = URIRef(dataset_dict['provenance'])
            retrieved_class = self._object_value(original_value, RDF.type)
            if any([val in [str(DCT.ProvenanceStatement)] for val in retrieved_class]):
                dataset_dict['provenance'] = str(self._object_value(original_value, RDFS.label))
        return dataset_dict

    def _extract_datasetseries_id(self, dataset_dict: Dict):
        if dataset_dict.get('biobank'):
            original_value = URIRef(dataset_dict['biobank'])
            retrieved_class = self._object_value(original_value, RDF.type)
            if any([val in [str(DCAT.DatasetSeries)] for val in retrieved_class]):
                dataset_dict['biobank'] = str(self._object_value(original_value, DCT.identifier))
                if dataset_dict['biobank'] == '':
                    dataset_dict['biobank'] = munge_title_to_name(str(self._object_value(original_value, DCT.title)))
        return dataset_dict

    def handle_pids(self, dataset_dict: Dict, key: str):
        return dataset_dict

    def parse_dataset(self, dataset_dict: Dict, dataset_ref: URIRef) -> Dict:
        """Parse dataset from RDF reference into dictionary."""
        dataset_dict["uri"] = str(dataset_ref)

        field_mappings = self._get_dataset_field_mappings()

        dataset_dict = self._extract_concept_dict(
            dataset_ref, dataset_dict, field_mappings
        )
        dataset_dict = self._extract_name_vcard(dataset_dict, 'contact')
        dataset_dict = self._extract_name_agent(dataset_dict, 'provider')
        dataset_dict = self._convert_image_year_range(dataset_dict)
        dataset_dict = self._extract_provenancestatement_label(dataset_dict)
        dataset_dict = self._extract_datasetseries_id(dataset_dict)

        return dataset_dict

    def parse_datasetseries(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        # Basic fields
        key_predicate_tuple = (
            ("id", DCT.identifier),
            ("name", DCT.title),
            ("acronym", DCT.alternative),
            ("description", DCT.description),
            ("geographical_coverage", DCT.spatial),
            ("juridical_person", DCT.publisher),
            ("url", DCAT.landingPage),
            ("contact", DCAT.contactPoint),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)

        dataset_dict = self.handle_pids(dataset_dict, "id")
        dataset_dict = self._extract_name_vcard(dataset_dict, 'contact')
        dataset_dict = self._extract_name_agent(dataset_dict, 'juridical_person')

        if not dataset_dict.get('id', False):
            dataset_dict['id'] = munge_title_to_name(dataset_dict["name"])

        return dataset_dict

    def parse_person(self, dataset_dict: Dict, dataset_ref: URIRef):
        dataset_dict["uri"] = str(dataset_ref)
        value = self._object_value(dataset_ref, RDF.type)
        key_predicate_tuple = ()
        if any([val in [str(FOAF.Agent), str(FOAF.Person), str(FOAF.Organization)] for val in value]):
            # Basic fields
            key_predicate_tuple = (
                ("id", DCT.identifier),
                ("name", FOAF.name),
                ("email", FOAF.mbox),
                ("last_name", FOAF.name),
            )
        elif any([val == str(VCARD.Kind) for val in value]):
            key_predicate_tuple = (
                ("id", DCT.identifier),
                ("name", VCARD.fn),
                ("email", VCARD.hasEmail),
                ("last_name", VCARD.fn),
            )

        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple)
        dataset_dict['first_name'] = " "

        if dataset_dict["email"].startswith("mailto:"):
            dataset_dict["email"] = dataset_dict["email"].removeprefix("mailto:")
        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")