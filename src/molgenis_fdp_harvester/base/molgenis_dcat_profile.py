# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI
# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://raw.githubusercontent.com/ckan/ckanext-dcat/master/ckanext/dcat/profiles/euro_dcat_ap.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

from typing import Dict, Union

from rdflib import URIRef, FOAF
from yarl import URL

import logging

from molgenis_fdp_harvester.base.baseharvester import munge_title_to_name
from molgenis_fdp_harvester.base.baseparser import RDFProfile, munge_tag
from molgenis_fdp_harvester.base.baseparser import (
    DCT, DCAT, ADMS
)

log = logging.getLogger(__name__)

# config = toolkit.config
DCAT_CLEAN_TAGS = False
NORMALIZE_CKAN_FORMAT = True


DISTRIBUTION_LICENSE_FALLBACK_CONFIG = "ckanext.dcat.resource.inherit.license"


class MolgenisEUCAIMDCATAPProfile(RDFProfile):
    """
    An RDF profile based on the DCAT-AP for data portals in Europe

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    """
    def _extract_name_from_query(self, value: Union[list, str]):
        if isinstance(value, list):
            value = [URL(val).query.get('name') for val in value]
        else:
            value = URL(value).query.get('name')
        return value

    def _extract_concept_dict(self, concept_ref, concept_dict: Dict, key_predicate_tuple: tuple, query_property_list: list):
        for key, predicate in key_predicate_tuple:
            value = self._object_value(concept_ref, predicate)
            if len(value) == 1:
                value = value[0]
            if value:
                if key in query_property_list:
                    value = self._extract_name_from_query(value)
                if isinstance(value, list):
                    value = ",".join(value)
                concept_dict[key] = value
        return concept_dict

    def parse_dataset(self, dataset_dict: Dict, dataset_ref: URIRef):
        # dataset_dict["extras"] = []
        # dataset_dict["resources"] = []
        dataset_dict["uri"] = str(dataset_ref)
        dataset_url = URL(str(dataset_ref))
        catalogue_base_url = URL.build(scheme=dataset_url.scheme, host=dataset_url.host, path=dataset_url.path)
        # Basic fields
        query_property_list = ['order_of_magnitude', 'imaging_modality','geographical_coverage','type',
                               'image_access_type', 'collection_method', 'body_part_examined',
                               'condition', 'topography', 'vendor', 'sex', 'interoperability_tier',
                               'terms_of_use', 'rights', 'hdab', 'coding_systems', 'theme']
        key_predicate_tuple = (
            # ("id", DCT.identifier),
            ("name", DCT.title),
            ("acronym", DCT.alternative),
            ("description", DCT.description),
            ("biobank", DCAT.inSeries),
            ("provider", DCT.publisher),
            ("order_of_magnitude", URIRef(f"{catalogue_base_url}/column/order_of_magnitude")),
            ("imaging_modality", URIRef("https:/www.eucaim.org/hasImageModality")),
            ("geographical_coverage", DCT.spatial),
            ("type", DCT.type),
            ("intended_purpose", URIRef(f"{catalogue_base_url}/column/intended_purpose")),
            ("image_access_type", URIRef(f"{catalogue_base_url}/column/image_access_type")),
            ("collection_method", URIRef("http://www.healthdcatap.org/healthCategory")),
            ("head", URIRef(f"{catalogue_base_url}/column/head")),
            ("contact", URIRef(f"{catalogue_base_url}/column/contact")),
            ("number_of_subjects", URIRef("http://www.healthdcatap.org/numberofUniqueIndividuals")),
            ("number_of_records", URIRef("http://www.healthdcatap.org/numberofRecords")),
            ("number_of_series", URIRef("https:/www.eucaim.org/nbrofSeries")),
            ("body_part_examined", URIRef("https:/www.eucaim.org/hasImageBodyPart")),
            ("condition", URIRef("https:/www.eucaim.org/hasCondition")),
            ("topography", URIRef(f"{catalogue_base_url}/column/topography")),
            ("vendor", URIRef("https:/www.eucaim.org/hasImageVendor")),
            ("image_year_range", URIRef(f"{catalogue_base_url}/column/image_year_range")),
            ("image_size", URIRef(f"{catalogue_base_url}/column/image_size")),
            ("sex", URIRef("https:/www.eucaim.org/hasAssociatedSex")),
            ("age_high", URIRef("http://www.healthdcatap.org/maxTypicalAge")),
            ("age_low", URIRef("http://www.healthdcatap.org/minTypicalAge")),
            ("age_median", URIRef("https:/www.eucaim.org/ageMedian")),
            ("theme", DCAT.theme),
            ("interoperability_tier", ADMS.interoperabilityLevel),
            ("provenance", DCT.provenance),
            ("intented_purpose", URIRef("https://w3id.org/dpv/dpv-skos#hasPurpose")),
            ("terms_of_use", URIRef(f"{catalogue_base_url}/column/terms_of_use")),
            ("commercial_use", URIRef(f"{catalogue_base_url}/column/commercial_use")),
            ("image_access_description", URIRef(f"{catalogue_base_url}/column/image_access_description")),
            ("image_access_fee", URIRef(f"{catalogue_base_url}/column/image_access_fee")),
            ("image_access_uri", URIRef(f"{catalogue_base_url}/column/image_access_uri")),
            ("publication_uri", URIRef(f"{catalogue_base_url}/column/publication_uri")),
            ("applicable_legislation", URIRef("http://data.europa.eu/r5r/applicableLegislation")),
            ("legal_basis", URIRef("https://w3id.org/dpv/dpv-skos#hasLegalBasis")),
            ("retention_period", URIRef("http://www.healthdcatap.org/retentionPeriod")),
            ("rights", DCT.rights),
            ("hdab", URIRef(f"{catalogue_base_url}/column/health_data_access_body")),
            ("quality_label", URIRef("http://www.w3.org/ns/dqv#hasQualityAnnotation")),
            ("coding_systems", URIRef("http://www.healthdcatap.org/hasCodingSystem")),
            ("metadata_issued", URIRef(f"{catalogue_base_url}/column/metadata_issued")),
            ("last_modified", DCT.modified),
            ("version", DCAT.version),
            ("withdrawn", URIRef(f"{catalogue_base_url}/column/withdrawn")),
        )
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple, query_property_list)

        # TODO store keywords somewhere
        # replace munge_tag to noop if there's no need to clean tags
        do_clean = DCAT_CLEAN_TAGS
        tags_val = [
            munge_tag(tag) if do_clean else tag for tag in self._keywords(dataset_ref)
        ]
        tags = [{"name": tag} for tag in tags_val]
        # dataset_dict["tags"] = tags

        # These values are fake. They need to be made "real"
        # log.warning("Filling in fake values")

        # dataset_dict["biobank"] = URL(dataset_dict["biobank"]).query.get('id')
        dataset_dict["biobank"] = munge_title_to_name(dataset_dict["biobank"])
        try:
            dataset_dict["head"] = URL(dataset_dict["head"]).query.get('id')
        except KeyError:
            pass
        try:
            dataset_dict["contact"] = URL(dataset_dict["contact"]).query.get('id')
        except KeyError:
            pass

        return dataset_dict

    def parse_datasetseries(self, dataset_dict: Dict, dataset_ref: URIRef):
        # dataset_dict["extras"] = []
        # dataset_dict["resources"] = []
        dataset_dict["uri"] = str(dataset_ref)
        dataset_url = URL(str(dataset_ref))
        catalogue_base_url = URL.build(scheme=dataset_url.scheme, host=dataset_url.host, path=dataset_url.path)
        # Basic fields
        key_predicate_tuple = (
            # ("id", DCT.identifier),
            ("name", DCT.title),
            ("acronym", DCT.alternative),
            ("description", DCT.description),
            ("geographical_coverage", DCT.spatial),
            ("juridical_person", DCT.publisher),
            ("url", DCAT.landingPage),
            ("contact", DCAT.contactPoint),
            ("head", URIRef(f"{catalogue_base_url}/column/head")),
            ("role", URIRef(f"{catalogue_base_url}/column/role")),
            ("network", URIRef(f"{catalogue_base_url}/column/network")),
            ("withdrawn", URIRef(f"{catalogue_base_url}/column/withdrawn")),
        )
        query_property_list = ['geographical_coverage']
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple, query_property_list)

        # # TODO store keywords somewhere
        # # replace munge_tag to noop if there's no need to clean tags
        # do_clean = DCAT_CLEAN_TAGS
        # tags_val = [
        #     munge_tag(tag) if do_clean else tag for tag in self._keywords(dataset_ref)
        # ]
        # tags = [{"name": tag} for tag in tags_val]
        # # dataset_dict["tags"] = tags

        # # These values are fake. They need to be made "real"
        # # log.warning("Filling in fake values")

        try:
            dataset_dict["contact"] = URL(dataset_dict["contact"]).query.get('id')
        except KeyError:
            pass
        try:
            dataset_dict["head"] = URL(dataset_dict["head"]).query.get('id')
        except KeyError:
            pass
        dataset_dict["network"] = URL(dataset_dict["network"]).query.get('id')

        return dataset_dict

    def parse_person(self, dataset_dict: Dict, dataset_ref: URIRef):
        # dataset_dict["extras"] = []
        # dataset_dict["resources"] = []
        dataset_dict["uri"] = str(dataset_ref)
        dataset_url = URL(str(dataset_ref))
        catalogue_base_url = URL.build(scheme=dataset_url.scheme, host=dataset_url.host, path=dataset_url.path)
        # Basic fields
        key_predicate_tuple = (
            ("id", FOAF.openid),
            ("name", FOAF.openid),
            ("email", FOAF.mbox),
            ("title_before_name", URIRef(f"{catalogue_base_url}/column/title_before_name")),
            ("title_after_name", URIRef(f"{catalogue_base_url}/column/title_after_name")),
            ("first_name", FOAF.firstName),
            ("last_name", FOAF.lastName),
            ("phone", FOAF.phone),
            ("address", URIRef(f"{catalogue_base_url}/column/address")),
            ("zip", URIRef(f"{catalogue_base_url}/column/zip")),
            ("city", URIRef(f"{catalogue_base_url}/column/city")),
            ("country", URIRef(f"{catalogue_base_url}/column/country")),
            ("role", URIRef(f"{catalogue_base_url}/column/role")),
        )
        query_property_list = ['country']
        dataset_dict = self._extract_concept_dict(dataset_ref, dataset_dict, key_predicate_tuple, query_property_list)

        dataset_dict["email"] = dataset_dict["email"].removeprefix("mailto:")
        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")
