# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI

# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/processors.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN
from typing import List
import xml

import rdflib
import rdflib.parser
from rdflib import FOAF
from rdflib.namespace import Namespace, RDF, DCAT

from .baseparser import VCARD
from ..utils import HarvesterException


HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")

RDF_PROFILES_ENTRY_POINT_GROUP = "ckan.rdf.profiles"
RDF_PROFILES_CONFIG_OPTION = "ckanext.dcat.rdf.profiles"
COMPAT_MODE_CONFIG_OPTION = "ckanext.dcat.compatibility_mode"

DEFAULT_RDF_PROFILES = ["euro_dcat_ap_2"]


def url_to_rdflib_format(_format):
    """
    Translates the RDF formats used on the endpoints to rdflib ones
    """
    if _format == "ttl":
        _format = "turtle"
    elif _format in ("rdf", "xml"):
        _format = "pretty-xml"
    elif _format == "jsonld":
        _format = "json-ld"

    return _format


class RDFProcessor(object):
    def __init__(self):
        """
        Creates a parser or serializer instance
        """

        self.g = rdflib.ConjunctiveGraph()


class RDFParser(RDFProcessor):
    """
    An RDF to CKAN parser based on rdflib

    Supports different profiles which are the ones that will generate
    CKAN dicts from the RDF graph.
    """

    def __init__(self, profiles: List):
        super().__init__()
        self._profiles = profiles

    # FIXME The FDP harvester should do this from catalog root.
    def _datasets(self):
        """
        Generator that returns all DCAT datasets on the graph

        Yields rdflib.term.URIRef objects that can be used on graph lookups
        and queries
        """
        for dataset in self.g.subjects(RDF.type, DCAT.Dataset):
            yield dataset

    def _datasetseries(self):
        """
        Generator that returns all DCAT dataset series on the graph

        Yields rdflib.term.URIRef objects that can be used on graph lookups
        and queries
        """
        for dataset in self.g.subjects(RDF.type, DCAT.DatasetSeries):
            yield dataset

    def _persons(self):
        """
        Generator that returns all FOAF Persons, Organizations and VCARD Kinds from the graph.

        This includes both:
        - Named resources (URIRefs) that are explicitly typed
        - Inline/blank node resources used as property values (e.g., dcat:contactPoint)

        Yields rdflib.term.Node objects (URIRef or BNode) that can be used on graph
        lookups and queries
        """
        query = """
        SELECT DISTINCT ?subject WHERE {
            {
                # Explicitly typed resources (named or blank nodes)
                ?subject a ?type .
                FILTER(?type IN (?FOAFPerson, ?FOAFOrganization, ?VCARDKind))
            }
            UNION
            {
                # Resources used as object values in any triple
                ?s ?p ?subject .
                ?subject a ?type .
                FILTER(?type IN (?FOAFPerson, ?FOAFOrganization, ?VCARDKind))
            }
        }
        """
        initBindings = {
            'FOAFPerson': FOAF.Person,
            'FOAFOrganization': FOAF.Agent,
            'VCARDKind': VCARD.Kind,
        }

        for row in self.g.query(query, initBindings=initBindings):
            yield row.subject

    def _catalogs(self):
        """
        Generator that returns all DCAT catalogs on the graph

        Yields rdflib.term.URIRef objects that can be used on graph lookups
        and queries, or for get requests
        """
        for catalog in self.g.subjects(RDF.type, DCAT.Catalog):
            yield catalog

    def next_page(self):
        """
        Returns the URL of the next page or None if there is no next page
        """
        for pagination_node in self.g.subjects(RDF.type, HYDRA.PagedCollection):
            # Try to find HYDRA.next first
            for o in self.g.objects(pagination_node, HYDRA.next):
                return str(o)

            # If HYDRA.next is not found, try HYDRA.nextPage (deprecated)
            for o in self.g.objects(pagination_node, HYDRA.nextPage):
                return str(o)
        return None

    def parse(self, data=None, _format=None, source=None):
        """
        Parses and RDF graph serialization and into the class graph

        It calls the rdflib parse function with the provided data and format.

        Data is a string with the serialized RDF graph (eg RDF/XML, N3
        ... ). By default RF/XML is expected. The optional parameter _format
        can be used to tell rdflib otherwise.

        It raises a ``RDFParserException`` if there was some error during
        the parsing.

        Returns nothing.
        """

        _format = url_to_rdflib_format(_format)
        if not _format or _format == "pretty-xml":
            # _format = "xml"
            # Let rdflib take care of it
            _format = None

        try:
            self.g.parse(data=data, format=_format)
            self.g = self.g.skolemize()
        # Apparently there is no single way of catching exceptions from all
        # rdflib parsers at once, so if you use a new one and the parsing
        # exceptions are not cached, add them here.
        # PluginException indicates that an unknown format was passed.
        except (
            SyntaxError,
            xml.sax.SAXParseException,
            rdflib.plugin.PluginException,
            TypeError,
        ) as e:
            raise HarvesterException(e)

    def supported_formats(self):
        """
        Returns a list of all formats supported by this processor.
        """
        return sorted(
            [plugin.name for plugin in rdflib.plugin.plugins(kind=rdflib.parser.Parser)]
        )

    def datasets(self):
        """
        Generator that returns CKAN datasets parsed from the RDF graph

        Each dataset is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._datasets():
            dataset_dict = {}
            for profile_class in self._profiles:
                profile = profile_class(self.g)
                profile.parse_dataset(dataset_dict, dataset_ref)

            dataset_dict['concept_type'] = 'dataset'

            yield dataset_dict

    def datasetseries(self):
        """
        Generator that returns dataset series parsed from the RDF graph

        Each dataset series is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._datasetseries():
            dataset_dict = {}
            for profile_class in self._profiles:
                profile = profile_class(self.g)
                profile.parse_datasetseries(dataset_dict, dataset_ref)

            dataset_dict['concept_type'] = 'datasetseries'

            yield dataset_dict

    def persons(self):
        """
        Generator that returns FOAF persons parsed from the RDF graph

        Each person object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._persons():
            dataset_dict = {}
            for profile_class in self._profiles:
                profile = profile_class(self.g)
                profile.parse_person(dataset_dict, dataset_ref)

            dataset_dict['concept_type'] = 'person'

            yield dataset_dict

    def get_concept(self, uri_ref, concept_type):
        concept_dict = {}
        for profile_class in self._profiles:
            profile = profile_class(self.g)
            if concept_type == 'person':
                profile.parse_person(concept_dict, uri_ref)
            elif concept_type == 'dataset':
                profile.parse_dataset(concept_dict, uri_ref)
            elif concept_type == 'datasetseries':
                profile.parse_datasetseries(concept_dict, uri_ref)

        concept_dict['concept_type'] = concept_type

        return concept_dict

    def dataset_in_catalog(self):
        """
        Generator that returns URIRef of all datasets referred to in Catalogs
        """
        for catalog_ref in self._catalogs():
            for object in self.g.objects(catalog_ref, DCAT.dataset):
                yield object
