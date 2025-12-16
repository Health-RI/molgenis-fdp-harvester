import logging
from typing import Dict, List

from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.fdp_harvester.domain.fair_data_point_record_provider import FairDataPointRecordProvider
from molgenis_fdp_harvester.fdp_harvester.domain.identifier import Identifier
from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester
from molgenis_fdp_harvester.utils import HarvesterException

log = logging.getLogger(__name__)

class FDPHarvester(DCATRDFHarvester):
    record_provider = None

    def __init__(self, profiles: List, concept_table_dict: Dict[str, str], molgenis_client: Client, harvester_config: Dict = None):
        super().__init__(profiles, concept_table_dict, molgenis_client, harvester_config)

    def gather_stage(self, harvest_root_uri):
        self.setup_record_provider(harvest_root_uri)
        try:
            # Flatten FDP to RDF
            self._convert_fdp_to_rdf()

            # Run the gather stage as if it is just RDF
            self._gather_stage()

        except Exception as e:
            log.error(f"Error in gather stage: {e}")
            raise HarvesterException(f"Failed to gather objects: {e}") from e
        return self._harvest_objects

    def _convert_fdp_to_rdf(self):
        for concept_type in self.concept_types:
            for identifier in self.record_provider.get_record_ids(concept_type=concept_type):
                log.info(f"Got identifier {str(identifier)} from RecordProvider")

                try:
                    self.guids_in_harvest[concept_type].append(Identifier(identifier).get_id_value())
                except Exception as e:
                    log.error(f"Error for identifier {str(identifier)} in gather phase: {str(e)}")
                    continue

                record = self.record_provider.get_record_by_id(identifier)
                if record:
                    try:
                        # Save the fetch contents in the HarvestObject
                        self.parser.parse(record, _format="ttl")
                    except Exception as e:
                        log.error(
                            "Error saving harvest object for identifier [%s] [%r]"
                            % (identifier, e))
                else:
                    log.error(
                        "Empty record for identifier %s" % identifier
                    )


    def setup_record_provider(self, harvest_url):
        # Harvest catalog config can be set on global CKAN level, but can be overriden by harvest config

        self.record_provider = FairDataPointRecordProvider(harvest_url)
