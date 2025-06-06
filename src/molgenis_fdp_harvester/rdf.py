import hashlib
import json
import logging
import traceback
from typing import List, Dict

from molgenis_emx2_pyclient import Client
from rdflib import URIRef

from molgenis_fdp_harvester.base.baseharvester import HarvestObject, munge_title_to_name
from molgenis_fdp_harvester.base.processor import RDFParser
from molgenis_fdp_harvester.rdf_harvester.dcatharvester import DCATHarvester
from molgenis_fdp_harvester.utils import HarvesterException

log = logging.getLogger(__name__)

class DCATRDFHarvester(DCATHarvester):
    _names_taken = []

    def __init__(self, profiles: List, concept_table_dict: Dict[str, str], molgenis_client: Client):
        super().__init__()
        self._profiles = profiles
        self.concept_table_link = concept_table_dict
        self.concept_types = [concept for concept in self.concept_table_link.keys()]
        self.molgenis_client = molgenis_client

        self.parser = RDFParser(self._profiles)

        self.guids_in_harvest = {concept : list() for concept in self.concept_types}
        self.guids_in_db = {concept : list() for concept in self.concept_types}
        self._names_taken = {concept : list() for concept in self.concept_types}

    def info(self):
        return {
            "name": "dcat_rdf",
            "title": "Generic DCAT RDF Harvester",
            "description": "Harvester for DCAT datasets from an RDF graph",
        }

    def gather_stage(self, harvest_root_uri):

        log.debug("In DCATRDFHarvester gather_stage")

        # Load RDF into the parser
        self._get_paginated_rdf(harvest_root_uri)

        # Extract the GUIDs from the harvested RDF in the parser
        try:
            for person in self.parser.persons():
                self._gather_concept_guid(person, concept_type='person')
            for dataset_series in self.parser.datasetseries():
                self._gather_concept_guid(dataset_series, concept_type='datasetseries')
            for dataset in self.parser.datasets():
                self._gather_concept_guid(dataset, concept_type='dataset')
        except Exception as e:
            log.error(
                "Error when processsing dataset: %r / %s" % (e, traceback.format_exc()),
            )
            return []

        # Extract the existing GUIDs
        for concept_type in self.concept_types:
            entity_name = self.concept_table_link[concept_type]
            try:
                existing_ids = self.molgenis_client.get(entity_name)
                self.guids_in_db[concept_type] = [x["id"] for x in existing_ids]
            except Exception as e:
                log.error(
                    "fetch_stage: Error getting list of uids %s: %r / %s",
                    (entity_name, e, traceback.format_exc()),
                )
                self.guids_in_db[concept_type] = []

        # Compare GUIDs from the harvest and those already existing, and create the HarvestObjects
        for concept_type in self.concept_types:
            guids_in_harvest = set(self.guids_in_harvest[concept_type])
            # guids_in_db = set(self.guids_in_db[concept_type])
            if guids_in_harvest:
                # new = guids_in_harvest - guids_in_db
                # delete = guids_in_db - guids_in_harvest
                # change = guids_in_db & guids_in_harvest
                for guid in guids_in_harvest:
                    self._harvest_objects.append(HarvestObject(guid=guid, status="new", concept_type=concept_type))
                # for guid in new:
                #     self._harvest_objects.append(HarvestObject(guid=guid, status="new", concept_type=concept_type))
                # for guid in change:
                #     self._harvest_objects.append(HarvestObject(guid=guid, status="change", concept_type=concept_type))
                # for guid in delete:
                #     self._harvest_objects.append(
                #         HarvestObject(guid=guid, status="delete", concept_type=concept_type))

        return self._harvest_objects

    def _gather_concept_guid(self, concept_dict: Dict, concept_type: str):
        guid = self._get_guid(concept_dict, source_url=concept_dict["uri"])
        if not guid:
            self._save_gather_error(
                "Could not get a unique identifier for {0}: {1}".format(
                    concept_type,
                    concept_dict
                ),
            )
        else:
            self.guids_in_harvest[concept_type].append(guid)

    def fetch_stage(self, harvest_object: HarvestObject):
        return self._gather_concept(harvest_object)

    def _gather_concept(self, harvest_object: HarvestObject):
        concept_type = harvest_object.concept_type
        concept_dict = self.parser.get_concept(URIRef(harvest_object.guid), concept_type)
        if not concept_dict.get("name"):
            concept_dict["name"] = self._gen_new_name(concept_dict["title"])
        if concept_dict["name"] in self._names_taken[concept_type]:
            suffix = (
                    len(
                        [
                            i
                            for i in self._names_taken
                            if i.startswith(concept_dict["name"] + "-")
                        ]
                    )
                    + 1
            )
            concept_dict["name"] = "{}-{}".format(concept_dict["name"], suffix)
        self._names_taken[concept_type].append(concept_dict["name"])

        # If there already is an identifier, don't add another identifier
        if not concept_dict.get("id"):
            concept_dict["id"] = munge_title_to_name(harvest_object.guid)

        harvest_object.content = json.dumps(concept_dict)
        return harvest_object

    def import_stage(self, harvest_object: HarvestObject):
        """
        Import HarvestObjects into Molgenis
        """
        log.debug("In DCATRDFHarvester import_stage")

        status = harvest_object.status
        if status == "delete":
            log.warning("import_stage: deleting datasets is currently not supported")
            return True

        if harvest_object.content is None:
            log.error(
                "import_stage: Empty content for object {0}".format(harvest_object.guid),
            )
            return False

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.error(
                "import_stage: Could not parse content for object {0}".format(
                    harvest_object.guid
                ),
            )
            return False

        concept_type = dataset['concept_type']
        entity_name = self.concept_table_link[concept_type]

        try:
            if harvest_object.status == "new":
                log.info("Adding dataset %s" % dataset["name"])
            else: # harvest_object.status == "change"
                log.info("Updating dataset %s" % dataset["name"])
            self.molgenis_client.save_schema(table=entity_name, data=[dataset])
        except Exception as e:
            log.error(
                "import_stage: Error importing dataset %s: %r / %s"
                % (dataset.get("name", ""), e, traceback.format_exc()),
                )
            return False

    def _get_paginated_rdf(self, harvest_root_uri):
        next_page_url = harvest_root_uri
        last_content_hash = None
        rdf_format = None

        content, rdf_format = self._get_content_and_type(
            next_page_url, 1, content_type=rdf_format
        )

        try:
            self.parser.parse(content, _format=rdf_format)
        except HarvesterException as e:
            self._save_gather_error(
                "Error parsing the RDF file: {0}".format(e), next_page_url
            )
            # return []

        # while next_page_url:
        #     if not next_page_url:
        #         # return []
        #         break

        #     content, rdf_format = self._get_content_and_type(
        #         next_page_url, 1, content_type=rdf_format
        #     )

        #     # MD5 is not cryptographically secure anymore, but this is not a security function.
        #     # It is used as a fast hash function to make sure no duplicate data is received
        #     content_hash = hashlib.md5()
        #     if content:
        #         content_hash.update(content.encode("utf8"))

        #     if last_content_hash:
        #         if content_hash.digest() == last_content_hash.digest():
        #             log.warning(
        #                 "Remote content was the same even when using a paginated URL, skipping"
        #             )
        #             break
        #     else:
        #         last_content_hash = content_hash

        #     if not content:
        #         break
        #         # return []

        #     try:
        #         self.parser.parse(content, _format=rdf_format)
        #     except HarvesterException as e:
        #         self._save_gather_error(
        #             "Error parsing the RDF file: {0}".format(e), next_page_url
        #         )
        #         # return []
        #         break

        #     if not self.parser:
        #         return []
        #     next_page_url = self.parser.next_page()

    def _get_dict_value(self, _dict, key, default=None):
        """
        Returns the value for the given key on a CKAN dict

        By default a key on the root level is checked. If not found, extras
        are checked, both with the key provided and with `dcat_` prepended to
        support legacy fields.

        If not found, returns the default value, which defaults to None
        """

        if key in _dict:
            return _dict[key]

        for extra in _dict.get("extras", []):
            if extra["key"] == key or extra["key"] == "dcat_" + key:
                return extra["value"]

        return default

    def _get_guid(self, dataset_dict, source_url=None):
        """
        Try to get a unique identifier for a harvested dataset

        It will be the first found of:
         * URI (rdf:about)
         * dcat:identifier
         * Source URL + Dataset name
         * Dataset name

         The last two are obviously not optimal, as depend on title, which
         might change.

         Returns None if no guid could be decided.
        """
        guid = None

        guid = self._get_dict_value(dataset_dict, "uri") or self._get_dict_value(
            dataset_dict, "identifier"
        )
        if guid:
            return guid

        if dataset_dict.get("name"):
            guid = dataset_dict["name"]
            if source_url:
                guid = source_url.rstrip("/") + "/" + guid
        return guid
