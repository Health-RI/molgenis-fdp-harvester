import logging

from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester
from molgenis_fdp_harvester.utils import HarvesterException

from .domain.fair_data_point_record_provider import FairDataPointRecordProvider
from .domain.identifier import Identifier

log = logging.getLogger(__name__)


class FDPHarvester(DCATRDFHarvester):
    record_provider = None

    def __init__(
        self,
        profiles: list,
        concept_table_dict: dict[str, str],
        molgenis_client: Client,
        harvester_config: dict | None = None,
    ):
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
                log.info(f"Got identifier {identifier!s} from RecordProvider")

                try:
                    self.guids_in_harvest[concept_type].append(
                        Identifier(identifier).get_id_value())
                except Exception as e:
                    log.error(
                        f"Error for identifier {identifier!s} in gather phase: {e!s}")
                    continue

                record = self.record_provider.get_record_by_id(identifier)
                if record:
                    try:
                        # Save the fetch contents in the HarvestObject
                        self.parser.parse(record, _format="ttl")
                    except Exception as e:
                        log.error(
                            f"Error saving harvest object for identifier [{identifier}] [{e!r}]")
                else:
                    log.error(
                        f"Empty record for identifier {identifier}"
                    )

    def setup_record_provider(self, harvest_url):
        # Harvest catalog config can be set on global CKAN level, but can be overriden by harvest config

        self.record_provider = FairDataPointRecordProvider(harvest_url)
