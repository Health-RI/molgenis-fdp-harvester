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

class FDPHarvester(DCATRDFHarvester):
    record_provider = None

    def __init__(self, profiles: List, concept_table_dict: Dict[str, str], molgenis_client: Client, harvester_config: Dict = None):
        super().__init__(profiles, concept_table_dict, molgenis_client, harvester_config)

    def gather_stage(self, harvest_root_uri):
        result = []

        self.setup_record_provider(harvest_root_uri)

        self._get_guids_in_harvest()
        self._get_guids_in_db()

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
                        log.info(f"Got identifier {str(identifier)} from RecordProvider")
                        if identifier is None:
                            log.error(f"RecordProvider returned empty identifier {repr(identifier)}, skipping...")
                            continue

                        self.guids_in_harvest[concept_type].append(identifier)
                    except Exception as e:
                        log.error(f"Error for identifier {str(identifier)} in gather phase: {str(e)}")
                        continue
        except Exception as e:
            # log.error("Exception: %s" % text_traceback())
            log.error(f"Error gathering the identifiers from the RecordProvider: [{str(e)}]")


    def fetch_stage(self, harvest_object: HarvestObject):
        logger = logging.getLogger(f"{__name__}.fetch_stage")

        logger.debug(f"Starting fetch_stage for harvest object [{harvest_object.guid}]")

        # Check harvest object status
        status = harvest_object.status

        if status == "delete":
            # No need to fetch anything, just pass to the import stage
            pass

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
                    except Exception as e:
                        log.error(
                            "Error saving harvest object for identifier [%s] [%r]"
                            %(identifier, e))

                else:
                    log.error(
                        "Empty record for identifier %s" % identifier, harvest_object
                    )

            except Exception as e:  # Broad exception because of unpredictability of Exceptions
                log.error(f"Error getting the record with identifier [{identifier}] from record provider")

        return harvest_object

    def setup_record_provider(self, harvest_url):
        # Harvest catalog config can be set on global CKAN level, but can be overriden by harvest config

        self.record_provider = FairDataPointRecordProvider(harvest_url)
