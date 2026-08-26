import json
import logging
import traceback
from urllib.parse import quote

from molgenis_emx2_pyclient import Client
from rdflib import URIRef

from molgenis_fdp_harvester.base.baseharvester import HarvestObject, munge_title_to_name
from molgenis_fdp_harvester.base.processor import RDFParser
from molgenis_fdp_harvester.utils import HarvesterException

from .dcatharvester import DCATHarvester

log = logging.getLogger(__name__)


class DCATRDFHarvester(DCATHarvester):
    """DCAT RDF Harvester for processing RDF data into Molgenis."""

    def __init__(
        self,
        profiles: list,
        concept_table_dict: dict[str, str],
        molgenis_client: Client,
        harvester_config: dict | None = None,
    ):
        super().__init__()
        self._profiles = profiles
        self.concept_table_link = concept_table_dict
        self.concept_types = list(self.concept_table_link.keys())
        self.molgenis_client = molgenis_client
        self.parser = RDFParser(self._profiles)
        self.harvester_config = harvester_config or {}

        # Initialize tracking dictionaries
        self._initialize_tracking_dictionaries()

    def _initialize_tracking_dictionaries(self):
        """Initialize dictionaries for tracking GUIDs and names."""
        self.guids_in_harvest = {concept: [] for concept in self.concept_types}
        self.guids_in_db = {concept: [] for concept in self.concept_types}
        # self._names_taken = {concept: [] for concept in self.concept_types}
        # Track datasets that need auto-generated datasetseries
        self._datasets_without_datasetseries = []

    def info(self):
        return {
            "name": "dcat_rdf",
            "title": "Generic DCAT RDF Harvester",
            "description": "Harvester for DCAT datasets from an RDF graph",
        }

    def gather_stage(self, harvest_root_uri):
        """Gather stage: discover and prepare objects for harvesting."""
        log.info(f"Starting gather stage for URI: {harvest_root_uri}")

        try:
            # Load and parse RDF content
            self._load_rdf_content(harvest_root_uri)

            self._gather_stage()

        except Exception as e:
            log.error(f"Error in gather stage: {e}")
            raise HarvesterException(f"Failed to gather objects: {e}") from e

        return self._harvest_objects

    def _gather_stage(self):
        # Extract concepts from RDF
        self._extract_concepts_from_rdf()

        # Get existing records from database
        self._get_guids_in_db()

        # Create harvest objects
        self._create_harvest_objects()

        log.info(f"Gathered {len(self._harvest_objects)} objects for harvesting")

    def _load_rdf_content(self, harvest_root_uri):
        """Load RDF content from the source URI."""
        try:
            self._get_rdf(harvest_root_uri)
        except Exception as e:
            raise HarvesterException(f"Failed to load RDF from {harvest_root_uri}: {e}") from e

    def _extract_concepts_from_rdf(self):
        """Extract all concept types from the parsed RDF."""
        try:
            extraction_methods = [
                ("provenancestatement", self.parser.provenancestatement),
                ("purpose", self.parser.purpose),
                ("kind", self.parser.kind),
                ("publisher", self.parser.publisher),
                ("datasetseries", self.parser.datasetseries),
                ("dataset", self.parser.datasets),
            ]

            for concept_type, extraction_method in extraction_methods:
                for concept in extraction_method():
                    self._gather_concept_guid(concept, concept_type)

        except Exception as e:
            log.error(f"Error extracting concepts from RDF: {e}")
            raise HarvesterException(f"Failed to extract concepts: {e}") from e

    def _get_guids_in_db(self):
        """Load existing records from the database for comparison."""
        for concept_type in self.concept_types:
            entity_name = self.concept_table_link[concept_type]
            try:
                existing_ids = self.molgenis_client.get(entity_name)
                self.guids_in_db[concept_type] = [x["id"] for x in existing_ids]
            except Exception as e:
                log.exception(
                    f"fetch_stage: Error getting list of uids {entity_name!s}: {e!r} / {traceback.format_exc()!s}"
                )
                self.guids_in_db[concept_type] = []

    def _create_harvest_objects(self):
        """Create harvest objects based on differences between harvested and existing GUIDs."""
        for concept_type in self.concept_types:
            guids_in_harvest = set(self.guids_in_harvest[concept_type])
            if guids_in_harvest:
                for guid in guids_in_harvest:
                    self._harvest_objects.append(HarvestObject(guid=guid, status="new", concept_type=concept_type))

        return self._harvest_objects

    def _gather_concept_guid(self, concept_dict: dict, concept_type: str):
        guid = self._get_guid(concept_dict, source_url=concept_dict["uri"])
        if not guid:
            self._save_gather_error(
                f"Could not get a unique identifier for {concept_type}: {concept_dict}",
            )
        else:
            self.guids_in_harvest[concept_type].append(guid)

    def fetch_stage(self, harvest_object: HarvestObject):
        concept_type = harvest_object.concept_type
        concept_dict = self.parser.get_concept(URIRef(harvest_object.guid), concept_type)

        # Ensure required fields
        if not concept_dict.get("name"):
            concept_dict["name"] = concept_dict.get("title")

        if not concept_dict.get("id"):
            concept_dict["id"] = munge_title_to_name(harvest_object.guid)

        # In Concept dict, go through the properties, look up the table to
        # query, and query Molgenis to get the name attached to the
        # ontologyTermURI. The table to query is configured in the
        # configuration.
        uri_lookup_table = self.harvester_config.get("uri_lookup_config", {}).get(concept_type)
        if uri_lookup_table:
            for property, value in concept_dict.items():
                molgenis_table = uri_lookup_table.get(property)
                if molgenis_table:
                    try:
                        new_property_value = self._resolve_uris_and_labels(value, molgenis_table)
                        if new_property_value:
                            concept_dict[property] = new_property_value
                    except Exception as exc:
                        log.warning(
                            f"Exception when resolving ontology URI or label: table {molgenis_table}; "
                            f"URI {value}; {exc!s}"
                        )

        harvest_object.content = json.dumps(concept_dict)

        # Check if this is a dataset without a datasetseries and auto_create is enabled
        if (
            concept_type == "dataset"
            and self.harvester_config.get("auto_create_datasetseries", False)
            and ("in_series" not in concept_dict or not concept_dict["in_series"])
        ):
            # Track this dataset for later datasetseries creation
            self._datasets_without_datasetseries.append(
                {
                    "dataset_name": concept_dict.get("title"),
                    "dataset_id": concept_dict.get("id"),
                    "dataset_description": concept_dict.get("description", ""),
                    "dataset_guid": harvest_object.guid,
                }
            )

        return harvest_object

    def _resolve_uri(self, value, molgenis_table):
        return self.molgenis_client.get(table=molgenis_table, query_filter=f"ontologyTermURI == '{quote(value)}'")

    def _resolve_label(self, value, molgenis_table):
        return self.molgenis_client.get(table=molgenis_table, query_filter=f"label == '{quote(value)}'")

    def _resolve_uris_and_labels(self, value, molgenis_table):
        new_property_value = None
        if isinstance(value, list):
            returned_value_list = []
            for val in value:
                returned_value = self._resolve_uri(val, molgenis_table)
                if not returned_value:
                    returned_value = self._resolve_label(val, molgenis_table)
                    if not returned_value:
                        continue
                returned_value_list.append(returned_value[0]["name"])
            if returned_value_list:
                new_property_value = ",".join(returned_value_list)
        else:
            returned_value = self._resolve_uri(value, molgenis_table)
            if not returned_value:
                returned_value = self._resolve_label(value, molgenis_table)
            if returned_value:
                new_property_value = returned_value[0]["name"]
        return new_property_value

    def _create_datasetseries_for_dataset(self, dataset_info):
        """Create a datasetseries (biobank) HarvestObject for a dataset."""
        # Use the same name as the dataset
        datasetseries_name = dataset_info["dataset_name"]
        datasetseries_id = dataset_info["dataset_id"]

        # Create minimal datasetseries content
        datasetseries_dict = {
            "id": datasetseries_id,
            "title": datasetseries_name,
            "description": dataset_info.get(
                "dataset_description",
                f"Auto-generated datasetseries for {datasetseries_name}",
            ),
        }

        # Create HarvestObject for the datasetseries
        # Use a synthetic GUID based on the dataset GUID
        datasetseries_guid = f"{dataset_info['dataset_guid']}_datasetseries"

        datasetseries_object = HarvestObject(guid=datasetseries_guid, status="new", concept_type="datasetseries")
        datasetseries_object.content = json.dumps(datasetseries_dict)

        return datasetseries_object, datasetseries_id

    def generate_missing_datasetseries(self):
        """Generate datasetseries for all datasets that need them and update dataset references."""
        if not self._datasets_without_datasetseries:
            return

        log.info(f"Auto-generating {len(self._datasets_without_datasetseries)} datasetseries for datasets without them")

        # Create datasetseries objects and update corresponding datasets
        for dataset_info in self._datasets_without_datasetseries:
            # Create the datasetseries HarvestObject
            datasetseries_object, datasetseries_id = self._create_datasetseries_for_dataset(dataset_info)

            # Add to harvest objects list
            self._harvest_objects.append(datasetseries_object)

            # Update the corresponding dataset to reference this datasetseries
            for harvest_obj in self._harvest_objects:
                if harvest_obj.concept_type == "dataset" and harvest_obj.guid == dataset_info["dataset_guid"]:
                    # Update the dataset's content to include the biobank reference
                    dataset_dict = json.loads(harvest_obj.content)
                    dataset_dict["in_series"] = datasetseries_id
                    harvest_obj.content = json.dumps(dataset_dict)
                    break

        log.info(f"Successfully created {len(self._datasets_without_datasetseries)} auto-generated datasetseries")

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
            log.error(f"import_stage: Empty content for object {harvest_object.guid}")
            return False

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.error(f"import_stage: Could not parse content for object {harvest_object.guid}")
            return False

        entity_name = self.concept_table_link[harvest_object.concept_type]

        dataset_name = dataset.get("title")
        notation = dataset.get("other_identifier")
        agency = self.harvester_config.get("server_url")

        try:
            if entity_name == "collections":
                success = self._upsert_collections(
                    dataset,
                    agency=agency,
                    dataset_name=dataset_name,
                    other_identifier_notation=notation,
                )
            else:
                success = self._upsert_table(
                    dataset,
                    status=harvest_object.status,
                    entity_name=entity_name,
                    dataset_name=dataset_name,
                )
        except Exception as e:
            log.exception(f"import_stage: Error importing dataset {dataset_name}: {e!r}")
            return False

        return bool(success)

    def _upsert_other_identifier_table(self, notation, agency):
        if notation:
            self.molgenis_client.save_table(
                table="other_identifier",
                data=[
                    {
                        "notation": notation,
                        "schemaAgency": agency,
                    }
                ],
            )

    def _check_previous_import(
        self,
        dataset: dict,
        agency: str,
        other_identifier_notation: str,
    ) -> tuple[str, bool]:
        """
        Check if the object has already been imported into Molgenis.

        Returns the entity ID and a boolean indicating if the agency matches.
        """
        try:
            existing_records = self.molgenis_client.get(
                table="collections",
                # The client library is responsible for quoting the value, so we don't quote it here.
                query_filter=f"other_identifier.notation == {other_identifier_notation}",
            )
            if not existing_records:
                return (None, False)

            existing_agency = existing_records[0].get("other_identifier")
            return (existing_records[0].get("id"), existing_agency == agency)
        except Exception as e:
            log.exception(f"Error checking previous import for dataset {dataset.get('title')}: {e!r}")
            return (None, False)

    def _upsert_collections(
        self,
        dataset: dict,
        agency: str,
        dataset_name: str,
        other_identifier_notation: str,
    ) -> bool:
        (existing_id, same_agency) = self._check_previous_import(dataset, agency, other_identifier_notation)
        if existing_id:
            if same_agency:
                log.info(f"Updating dataset '{dataset_name}' with ID '{existing_id}'")
                dataset["id"] = existing_id
                log.info(f"Updating dataset '{dataset_name}'")
            else:
                log.warning(f"Dataset '{dataset_name}' already exists with a different agency. Skipping update.")
                return False
        else:
            log.info(f"Adding dataset '{dataset_name}'")

        self._upsert_other_identifier_table(other_identifier_notation, agency)
        self.molgenis_client.save_table(table="collections", data=[dataset])
        return True

    def _upsert_table(self, dataset: dict, status: str, entity_name: str, dataset_name: str) -> bool:
        if status == "new":
            log.info(f"Adding dataset '{dataset_name}'")
        else:  # status == "change"
            log.info(f"Updating dataset '{dataset_name}'")
        self.molgenis_client.save_table(table=entity_name, data=[dataset])
        return True

    def _get_rdf(self, harvest_root_uri):
        next_page_url = harvest_root_uri
        rdf_format = None

        content, rdf_format = self._get_content_and_type(next_page_url, 1, content_type=rdf_format)

        try:
            self.parser.parse(content, _format=rdf_format)
        except HarvesterException as e:
            self._save_gather_error(f"Error parsing the RDF file: {e}", next_page_url)

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
        guid = self._get_dict_value(dataset_dict, "uri")
        if guid:
            return guid

        if dataset_dict.get("name"):
            guid = dataset_dict["name"]
            if source_url:
                guid = source_url.rstrip("/") + "/" + guid
        return guid
