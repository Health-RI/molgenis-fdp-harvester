import json
import logging
import traceback
import warnings
from typing import Dict, List

from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.base.baseharvester import HarvestObject, munge_title_to_name
from molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point_record_provider import FairDataPointRecordProvider
from molgenis_fdp_harvester.fdp_harvester.domain.identifier import Identifier
from molgenis_fdp_harvester.rdf import DCATRDFHarvester

log = logging.getLogger(__name__)


# def text_traceback():
#     with warnings.catch_warnings():
#         warnings.simplefilter("ignore")
#         res = "the original traceback:".join(
#             cgitb.text(sys.exc_info()).split("the original traceback:")[1:]
#         ).strip()
#     return res

class FDPHarvester(DCATRDFHarvester):
    record_provider = None

    def __init__(self, profiles: List, concept_table_dict: Dict[str, str], molgenis_client: Client, harvester_config: Dict = None):
        super().__init__(profiles, concept_table_dict, molgenis_client, harvester_config)

    def gather_stage(self, harvest_root_uri):
        result = []

        self.setup_record_provider(harvest_root_uri)

        self._get_guids_in_harvest()
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

        for concept_type in self.concept_types:
            guids_in_harvest = set(self.guids_in_harvest[concept_type])
            guids_in_db = set(self.guids_in_db[concept_type])
            if guids_in_harvest:
                new = guids_in_harvest - guids_in_db
                delete = guids_in_db - guids_in_harvest
                change = guids_in_db & guids_in_harvest

                for guid in new:
                    self._harvest_objects.append(HarvestObject(guid=guid, status="new", concept_type=concept_type))
                for guid in change:
                    self._harvest_objects.append(HarvestObject(guid=guid, status="change", concept_type=concept_type))
                for guid in delete:
                    self._harvest_objects.append(
                        HarvestObject(guid=guid, status="delete", concept_type=concept_type))

        return self._harvest_objects

    def _get_guids_in_harvest(self):
        """
        Get identifiers of records in harvest source. These should be present in CKAN once all imports have
        finished.
        :param harvest_job:
        :return:
        """

        try:
            for concept_type in self.concept_types:
                for identifier in self.record_provider.get_record_ids(concept_type=concept_type):
                    try:
                        log.info("Got identifier [%s] from RecordProvider", identifier)
                        if identifier is None:
                            log.error(
                                "RecordProvider returned empty identifier [%r], skipping..."
                                % identifier
                            )
                            continue

                        self.guids_in_harvest[concept_type].append(identifier)
                    except Exception as e:
                        log.error(
                            "Error for identifier [%s] in gather phase: [%r]"
                            % (identifier, e)
                        )
                        continue
        except Exception as e:
            # log.error("Exception: %s" % text_traceback())
            log.error(
                "Error gathering the identifiers from the RecordProvider: [%s]"
                % str(e)
            )

    def fetch_stage(self, harvest_object: HarvestObject):
        logger = logging.getLogger(__name__ + ".fetch_stage")

        logger.debug("Starting fetch_stage for harvest object [%s]", harvest_object.guid)

        result = False

        # Check harvest object status
        status = harvest_object.status

        if status == "delete":
            # No need to fetch anything, just pass to the import stage
            result = True

        else:
            identifier = harvest_object.guid
            try:
                record = self.record_provider.get_record_by_id(identifier)
                harvest_object.guid = Identifier(identifier).get_id_value()

                if record:
                    try:
                        # Save the fetch contents in the HarvestObject
                        self.parser.parse(record, _format="ttl")
                        harvest_object = self._fetch_concept(harvest_object)
                        # harvest_object.content = record  # TODO move JSON stuff to record provider for Gisweb harvester
                    except Exception as e:
                        log.error(
                            "Error saving harvest object for identifier [%s] [%r]"
                            %(identifier, e))
                        return False

                    result = True
                else:
                    log.error(
                        "Empty record for identifier %s" % identifier, harvest_object
                    )
                    result = False

            except Exception as e:  # Broad exception because of unpredictability of Exceptions
                log.error(
                    "Error getting the record with identifier [%s] from record provider"
                    % identifier
                )
                result = False

        return harvest_object

    def setup_record_provider(self, harvest_url):
        # Harvest catalog config can be set on global CKAN level, but can be overriden by harvest config

        self.record_provider = FairDataPointRecordProvider(harvest_url)
