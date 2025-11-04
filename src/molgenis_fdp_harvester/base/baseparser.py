# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI

# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/profiles/base.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

import json
from urllib.parse import quote
import re
import logging

from rdflib import term, URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, SKOS, RDFS, DCAT, FOAF, TIME, OWL
from rdflib.namespace import DCTERMS as DCT
from geomet import wkt
from unidecode import unidecode

log = logging.getLogger(__name__)

DCATAP = Namespace("http://data.europa.eu/r5r/")
ADMS = Namespace("http://www.w3.org/ns/adms#")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
SCHEMA = Namespace("http://schema.org/")
LOCN = Namespace("http://www.w3.org/ns/locn#")
GSP = Namespace("http://www.opengis.net/ont/geosparql#")
SPDX = Namespace("http://spdx.org/rdf/terms#")

namespaces = {
    "dct": DCT,
    "dcat": DCAT,
    "dcatap": DCATAP,
    "adms": ADMS,
    "vcard": VCARD,
    "foaf": FOAF,
    "schema": SCHEMA,
    "time": TIME,
    "skos": SKOS,
    "locn": LOCN,
    "gsp": GSP,
    "owl": OWL,
    "spdx": SPDX,
}

PREFIX_MAILTO = "mailto:"

GEOJSON_IMT = "https://www.iana.org/assignments/media-types/application/vnd.geo+json"

HARVEST_DEFAULT_LABEL_LANGUAGE = "en"

MIN_TAG_LENGTH = 2
MAX_TAG_LENGTH = 100


# Below taken from https://github.com/ckan/ckan/blob/master/ckan/lib/munge.py
def _munge_to_length(string: str, min_length: int, max_length: int) -> str:
    """Pad/truncates a string"""
    if len(string) < min_length:
        string += "_" * (min_length - len(string))
    if len(string) > max_length:
        string = string[:max_length]
    return string


def munge_tag(tag: str) -> str:
    tag = unidecode(tag)
    tag = tag.lower().strip()
    tag = re.sub(r"[^a-zA-Z0-9\- ]", "", tag).replace(" ", "-")
    tag = _munge_to_length(tag, MIN_TAG_LENGTH, MAX_TAG_LENGTH)
    return tag


class URIRefOrLiteral(object):
    """Helper which creates an URIRef if the value appears to be an http URL,
    or a Literal otherwise. URIRefs are also cleaned using CleanedURIRef.

    Like CleanedURIRef, this is a factory class.
    """

    def __new__(cls, value):
        try:
            stripped_value = value.strip()
            if isinstance(value, str) and (
                stripped_value.startswith("http://")
                or stripped_value.startswith("https://")
            ):
                uri_obj = CleanedURIRef(value)
                # although all invalid chars checked by rdflib should have been quoted, try to serialize
                # the object. If it breaks, use Literal instead.
                uri_obj.n3()
                # URI is fine, return the object
                return uri_obj
            else:
                return Literal(value)
        except Exception:
            # In case something goes wrong: use Literal
            return Literal(value)


class CleanedURIRef(object):
    """Performs some basic URL encoding on value before creating an URIRef object.

    This is a factory for URIRef objects, which allows usage as type in graph.add()
    without affecting the resulting node types. That is,
    g.add(..., URIRef) and g.add(..., CleanedURIRef) will result in the exact same node type.
    """

    @staticmethod
    def _careful_quote(value):
        # only encode this limited subset of characters to avoid more complex URL parsing
        # (e.g. valid ? in query string vs. ? as value).
        # can be applied multiple times, as encoded %xy is left untouched. Therefore, no
        # unquote is necessary beforehand.
        quotechars = " !\"$'()*,;<>[]{|}\\^`"
        for c in quotechars:
            value = value.replace(c, quote(c))
        return value

    def __new__(cls, value):
        if isinstance(value, str):
            value = CleanedURIRef._careful_quote(value.strip())
        return URIRef(value)


class RDFProfile(object):
    """Base class with helper methods for implementing RDF parsing profiles

    This class should not be used directly, but rather extended to create
    custom profiles
    """

    def __init__(self, graph, compatibility_mode=False):
        """Class constructor

        Graph is an rdflib.Graph instance.

        In compatibility mode, some fields are modified to maintain
        compatibility with previous versions of the ckanext-dcat parsers
        (eg adding the `dcat_` prefix or storing comma separated lists instead
        of JSON dumps).
        """

        self.g = graph

        self.compatibility_mode = compatibility_mode

        # Cache for mappings of licenses URL/title to ID built when needed in
        # _license().
        self._licenceregister_cache = None

    def _datasets(self):
        """
        Generator that returns all DCAT datasets on the graph

        Yields term.URIRef objects that can be used on graph lookups
        and queries
        """
        for dataset in self.g.subjects(RDF.type, DCAT.Dataset):
            yield dataset

    def _distributions(self, dataset):
        """
        Generator that returns all DCAT distributions on a particular dataset

        Yields term.URIRef objects that can be used on graph lookups
        and queries
        """
        for distribution in self.g.objects(dataset, DCAT.distribution):
            yield distribution

    def _object(self, subject, predicate):
        """
        Helper for returning the first object for this subject and predicate

        Both subject and predicate must be rdflib URIRef or BNode objects

        Returns an rdflib reference (URIRef or BNode) or None if not found
        """
        for _object in self.g.objects(subject, predicate):
            return _object
        return None

    def _object_value(self, subject, predicate):
        """
        Given a subject and a predicate, returns the value of the object

        Both subject and predicate must be rdflib URIRef or BNode objects

        If found, the string representation is returned, else an empty string
        """
        # FIXME Change language back to be dynamic
        default_lang = HARVEST_DEFAULT_LABEL_LANGUAGE
        fallback = ""
        for o in self.g.objects(subject, predicate):
            if isinstance(o, Literal):
                if o.language and o.language == default_lang:
                    return str(o)
                # Use first object as fallback if no object with the default language is available
                elif fallback == "":
                    fallback = str(o)
            else:
                return str(o)
        return fallback

    def _get_root_catalog_ref(self):
        roots = list(self.g.subjects(DCT.hasPart))
        if not roots:
            roots = list(self.g.subjects(RDF.type, DCAT.Catalog))
        return roots[0]

    # Public methods for profiles to implement

    def parse_dataset(self, dataset_dict, dataset_ref):
        """
        Creates a CKAN dataset dict from the RDF graph

        The `dataset_dict` is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.
        `dataset_ref` is an rdflib URIRef object
        that can be used to reference the dataset when querying the graph.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        return dataset_dict
