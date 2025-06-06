# SPDX-FileCopyrightText: 2023 Civity
# SPDX-FileContributor: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-only
import traceback

import cgitb
import json
import logging
import sys
import uuid
import warnings
from abc import abstractmethod

import ckan.plugins.toolkit as toolkit
from ckan import model

# from ckanext.fairdatapoint.harvesters.config import get_harvester_setting
from ckanext.fairdatapoint.labels import resolve_labels
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from molgenis_emx2_pyclient import Client

ID = "id"

log = logging.getLogger(__name__)

RESOLVE_LABELS = "resolve_labels"


class HarvestObject(object):
    def __init__(self, guid, content):
        self.guid = guid
        self.content = content
        self.status = None


def text_traceback():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = "the original traceback:".join(
            cgitb.text(sys.exc_info()).split("the original traceback:")[1:]
        ).strip()
    return res


class CivityHarvesterException(Exception):
    pass


class CivityHarvester(HarvesterBase):
    """
    A Harvester base class for multiple Civity harvesters. This class contains the harvester bookkeeping and delegates
    the harvester specific work to a RecordProvider (to access records from a harvest source) and a
    RecordToPackageConverter to convert proprietary data from the harvest source to CKAN packages.
    """

    record_provider = None

    record_to_package_converter = None

    @abstractmethod
    def setup_record_provider(self, harvest_url):
        pass

    @abstractmethod
    def setup_record_to_package_converter(self, harvest_url, harvest_config_dict):
        pass

    def gather_stage(self, harvest_root_uri):
        """
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its job. The HarvestObjects need a
              reference date with the last modified date for the resource, this
              may need to be set in a different stage depending on the type of
              source.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.
            - to abort the harvest, create a HarvestGatherError and raise an
              exception. Any created HarvestObjects will be deleted.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        """

        logger = logging.getLogger(__name__ + ".gather_stage")

        # logger.debug("Starting gather_stage for job: [%r]", harvest_job)

        #
        result = []

        self.setup_record_provider(harvest_root_uri)

        # TODO: There is no connection with the database
        # guids_to_package_ids = self._get_guids_to_package_ids_from_database(harvest_job)

        # guids_in_db = set(guids_to_package_ids.keys())

        guids_in_harvest = self._get_guids_in_harvest()

        if guids_in_harvest:
            for guid in guids_in_harvest:
                obj = HarvestObject(
                    guid=guid,
                    status="new"
                )
                result.append(obj.guid)

        # Why is this needed? An empty list seems a valid result of this stage. There is simply nothing to do
        # if len(result) == 0:
        #     result = None

        logger.debug("Finished gather_stage for job: [%r]", harvest_root_uri)

        return result

    # TODO In the other harvester the fetch_stage is used to check if a dataset already exists, and with that
    #   determine if the object should be added or updated. Updating and adding is now a function of the Molgenis
    #   library. Also,_get_guids_to_package_ids_from_database seems to do something similar?
    #   What is use is the fetch_stage here?
    def fetch_stage(self, harvest_object):
        """
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything is ok (ie the object should now be
              imported), "unchanged" if the object didn't need harvesting after
              all (ie no error, but don't continue to import stage) or False if
              there were errors.

        :param harvest_object: HarvestObject object
        :returns: True if successful, 'unchanged' if nothing to import after
                  all, False if not successful
        """

        logger = logging.getLogger(__name__ + ".fetch_stage")

        logger.debug("Starting fetch_stage for harvest object [%s]", harvest_object.id)

        self.setup_record_provider(
            harvest_object.source.url,
            self._get_harvest_config(harvest_object.source.config),
        )

        result = False

        # Check harvest object status
        status = self._get_object_extra(harvest_object, "status")

        if status == "delete":
            # No need to fetch anything, just pass to the import stage
            result = True

        else:
            identifier = harvest_object.guid
            try:
                record = self.record_provider.get_record_by_id(identifier)

                if record:
                    try:
                        # Save the fetch contents in the HarvestObject
                        harvest_object.content = record  # TODO move JSON stuff to record provider for Gisweb harvester
                        harvest_object.save()
                    except Exception as e:
                        self._save_object_error(
                            "Error saving harvest object for identifier [%s] [%r]"
                            % (identifier, e),
                            harvest_object,
                        )
                        return False

                    model.Session.commit()

                    logger.debug(
                        "Record content saved for ID [%s], harvest object ID [%s]",
                        harvest_object.guid,
                        harvest_object.id,
                    )

                    result = True
                else:
                    self._save_object_error(
                        "Empty record for identifier %s" % identifier, harvest_object
                    )
                    result = False

            except (
                Exception
            ) as e:  # Broad exception because of unpredictability of Exceptions
                self._save_object_error(
                    "Error getting the record with identifier [%s] from record provider"
                    % identifier,
                    harvest_object,
                )
                result = False

        logger.debug("Finished fetch_stage for harvest object [%s]", harvest_object.id)

        return result

    # TODO This needs to be rewritten to use the Molgenis client
    def import_stage(self, harvest_object: HarvestObject, molgenis_client: Client):
        """
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g.
              create, update or delete a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - setting the HarvestObject.package (if there is one)
            - setting the HarvestObject.current for this harvest:
               - True if successfully created/updated
               - False if successfully deleted
            - setting HarvestObject.current to False for previous harvest
              objects of this harvest source if the action was successful.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - creating the HarvestObject - Package relation (if necessary)
            - returning True if the action was done, "unchanged" if the object
              didn't need harvesting after all or False if there were errors.

        NB You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                  need harvesting after all or False if there were errors.
        """

        logger = logging.getLogger(__name__ + ".import_stage")

        logger.debug("Starting import stage for harvest_object [%s]", harvest_object.id)

        # TODO What does this do?
        self.setup_record_to_package_converter(
            harvest_object.source.url,
            self._get_harvest_config(harvest_object.source.config),
        )

        status = self._get_object_extra(harvest_object, "status")

        if status == "delete":
            # Delete package
            log.warning("import_stage: deleting datasets is currently not supported")
            return True

        if harvest_object.content is None:
            log.error(
                "import_stage: Empty content for object {0}".format(harvest_object.id),
            )
            return False

        try:
            dataset = json.loads(harvest_object.content)
            concept_type = dataset['concept_type']
            entity_name = self.concept_table_link[concept_type]
        except ValueError:
            log.error(
                "import_stage: Could not parse content for object {0}".format(
                    harvest_object.guid
                ),
            )
            return False

        try:
            if status == "new":
                log.info("Adding dataset %s" % dataset["name"])
                molgenis_client.save_schema(table=entity_name, data=[dataset])
            elif status == "change":
                log.info("Updating dataset %s" % dataset["name"])
                molgenis_client.save_schema(table=entity_name, data=[dataset])
        except Exception as e:
            log.error(
                "import_stage: Error importing dataset %s: %r / %s"
                % (dataset.get("name", ""), e, traceback.format_exc()),
            )
            return False

        logger.debug("Finished import stage for harvest_object [%s]", harvest_object.id)

        return True

#     @staticmethod
#     def _get_guids_to_package_ids_from_database(harvest_job):
#         """
#         Read from GUID's and associated package ID's as currently present from database to be able to create
#         the three to do lists
#         :param harvest_job:
#         :return:
#         """
#         query = (
#             model.Session.query(HarvestObject.guid, HarvestObject.package_id)
#             .filter(HarvestObject.current == True)
#             .filter(HarvestObject.harvest_source_id == harvest_job.source.id)
#         )
#
#         guid_to_package_id = {}
#
#         for guid, package_id in query:
#             guid_to_package_id[guid] = package_id
#
#         return guid_to_package_id

    def _get_guids_in_harvest(self):
        """
        Get identifiers of records in harvest source. These should be present in CKAN once all imports have
        finished.
        :param harvest_job:
        :return:
        """
        guids_in_harvest = set()

        try:
            for identifier in self.record_provider.get_record_ids():
                try:
                    log.info("Got identifier [%s] from RecordProvider", identifier)
                    if identifier is None:
                        log.error(
                            "RecordProvider returned empty identifier [%r], skipping..."
                            % identifier
                        )
                        continue

                    guids_in_harvest.add(identifier)
                except Exception as e:
                    log.error(
                        "Error for identifier [%s] in gather phase: [%r]"
                        % (identifier, e)
                    )
                    # self._save_gather_error(
                    #     "Error for identifier [%s] in gather phase: [%r]"
                    #     % (identifier, e),
                    #     harvest_job,
                    # )
                    continue
        except Exception as e:
            log.error("Exception: %s" % text_traceback())
            log.error(
                "Error gathering the identifiers from the RecordProvider: [%s]"
                % str(e)
            )
            # self._save_gather_error(
            #     "Error gathering the identifiers from the RecordProvider: [%s]"
            #     % str(e),
            #     harvest_job,
            # )
            guids_in_harvest = None

        return guids_in_harvest

    @staticmethod
    def _get_harvest_config(config_str):
        """
        Loads the source configuration JSON object into a dict for convenient access
        """
        config_dict = {}

        if config_str:
            config_dict = json.loads(config_str)

        return config_dict

    @staticmethod
    def _get_object_extra(harvest_object, key):
        """
        Helper function for retrieving the value from a harvest object extra,
        given the key
        """
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_template_package_dict(self, harvest_config_dict):
        """
        Get template package dictionary. This approach has the advantage that in case someone modifies the
        schema, the harvester configuration can be left untouched without the harvester breaking. Disadvantage is
        that there is a package which should never be shown to users.
        TODO
          is this the most convenient solution? Are there better alternatives which provide the same functionality
          without the drawback of running the risk of the template showing up somewhere unexpectedly.
        :param harvest_config_dict:
        :return:
        """
        context = {
            "user": self._get_user_name(),
            "return_id_only": True,
            "ignore_auth": True,
        }

        template_package_id = self._get_template_package_id(harvest_config_dict)
        try:
            result = toolkit.get_action("package_show")(
                context.copy(), {"id": template_package_id}
            )
        except toolkit.ObjectNotFound as e:
            log.error(
                "Error looking up template package [%s]: [%s]",
                template_package_id,
                e.message,
            )
            result = None

        return result

    @staticmethod
    def _get_template_package_id(harvest_config_dict):
        """
        Retrieves template package ID from harvest_config dictionary key 'template_package_id_id' if it exists
        ex. {'template_package_id': 'template_for_rotterdam_dataplatform'}
        """

        if "template_package_id" in harvest_config_dict.keys():
            result = harvest_config_dict.get("template_package_id", "template")
        else:
            result = "template"

        return result

    def _get_package_name(self, harvest_object, title):
        package = harvest_object.package
        if package is None or package.title != title:
            name = self._gen_new_name(title)
            if not name:
                raise Exception(
                    "Could not generate a unique name from the title or the GUID. Please choose a more unique title."
                )
        else:
            name = package.name

        return name
