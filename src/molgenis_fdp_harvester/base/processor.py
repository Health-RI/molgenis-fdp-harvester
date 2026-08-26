# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI

# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/processors.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN
import xml

import rdflib
import rdflib.parser
from rdflib import FOAF, PROV
from rdflib.namespace import DCAT, RDF

from molgenis_fdp_harvester.utils import HarvesterException

from .baseparser import DCT, DPV, HYDRA, VCARD

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


class RDFProcessor:
    def __init__(self):
        """
        Creates a parser or serializer instance
        """

        self.g = rdflib.Dataset()


class RDFParser(RDFProcessor):
    """
    An RDF to CKAN parser based on rdflib

    Supports different profiles which are the ones that will generate
    CKAN dicts from the RDF graph.
    """

    def __init__(self, profiles: list):
        super().__init__()
        self._profiles = profiles
        self._profile_instances = None

    def _get_profile_instances(self):
        """Build one instance per profile class and reuse it for the parser's lifetime.

        Profiles are stateful for the duration of a harvest run: they cache the
        UUIDv4s assigned to supplementary classes (e.g. Agents) so that the same
        RDF resource resolves to the same internal ID everywhere it is referenced.
        Instances are created lazily so they capture the graph after parsing
        (self.g is replaced by its skolemized copy in parse()).

        This assumes every parse() call happens before this method is first
        called, i.e. before any of datasets()/kind()/publisher()/etc. are used
        - both DCATRDFHarvester and FDPHarvester satisfy this. Calling parse()
        again afterwards would leave the cached instances pointing at a stale
        graph. Harvesting is single-threaded; this cache is not safe for
        concurrent access.
        """
        if self._profile_instances is None:
            self._profile_instances = [profile_class(self.g) for profile_class in self._profiles]
        return self._profile_instances

    # FIXME The FDP harvester should do this from catalog root.
    def _datasets(self):
        """
        Generator that returns all DCAT datasets on the graph

        Yields rdflib.term.URIRef objects that can be used on graph lookups
        and queries
        """
        yield from self.g.subjects(RDF.type, DCAT.Dataset)

    def _datasetseries(self):
        """
        Generator that returns all DCAT dataset series on the graph

        Yields rdflib.term.URIRef objects that can be used on graph lookups
        and queries
        """
        yield from self.g.subjects(RDF.type, DCAT.DatasetSeries)

    def _publisher(self):
        yield from self.g.subjects(RDF.type, FOAF.Organization)

    def _kind(self):
        yield from self.g.subjects(RDF.type, VCARD.Kind)

    def _provenancestatement(self):
        yield from self.g.subjects(RDF.type, DCT.ProvenanceStatement)

    def _purpose(self):
        yield from self.g.subjects(RDF.type, DPV.Purpose)

    def _creator(self):
        """foaf:Agent resources reached via dct:creator on a dataset.

        creator and attribution_agent are both plain foaf:Agent in RDF - there's no
        rdf:type that distinguishes "this agent is a dataset's creator" from "this agent
        is an attribution's agent" - so unlike the whole-graph type scans above, these
        have to be found by walking the specific predicate path from datasets instead.
        """
        seen = set()
        for dataset_ref in self._datasets():
            for obj in self.g.objects(dataset_ref, DCT.creator):
                if obj not in seen and (obj, RDF.type, FOAF.Agent) in self.g:
                    seen.add(obj)
                    yield obj

    def _attribution_agent(self):
        """foaf:Agent resources reached via prov:qualifiedAttribution/prov:agent on a dataset."""
        seen = set()
        for dataset_ref in self._datasets():
            for attribution_ref in self.g.objects(dataset_ref, PROV.qualifiedAttribution):
                for obj in self.g.objects(attribution_ref, PROV.agent):
                    if obj not in seen and (obj, RDF.type, FOAF.Agent) in self.g:
                        seen.add(obj)
                        yield obj

    def _catalogs(self):
        """
        Generator that returns all DCAT catalogs on the graph

        Yields rdflib.term.URIRef objects that can be used on graph lookups
        and queries, or for get requests
        """
        yield from self.g.subjects(RDF.type, DCAT.Catalog)

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

    def parse(self, data=None, _format=None):
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
            raise HarvesterException(e) from e

    def supported_formats(self):
        """
        Returns a list of all formats supported by this processor.
        """
        return sorted([plugin.name for plugin in rdflib.plugin.plugins(kind=rdflib.parser.Parser)])

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
            for profile in self._get_profile_instances():
                profile.parse_dataset(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "dataset"

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
            for profile in self._get_profile_instances():
                profile.parse_datasetseries(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "datasetseries"

            yield dataset_dict

    def publisher(self):
        """
        Generator that returns FOAF persons parsed from the RDF graph

        Each person object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._publisher():
            dataset_dict = {}
            for profile in self._get_profile_instances():
                profile.parse_publisher(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "publisher"

            yield dataset_dict

    def kind(self):
        """
        Generator that returns FOAF persons parsed from the RDF graph

        Each person object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._kind():
            dataset_dict = {}
            for profile in self._get_profile_instances():
                profile.parse_kind(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "kind"

            yield dataset_dict

    def provenancestatement(self):
        """
        Generator that returns FOAF persons parsed from the RDF graph

        Each person object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._provenancestatement():
            dataset_dict = {}
            for profile in self._get_profile_instances():
                profile.parse_provenancestatement(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "provenancestatement"

            yield dataset_dict

    def purpose(self):
        """
        Generator that returns dpv:Purpose concepts parsed from the RDF graph

        Each purpose object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._purpose():
            dataset_dict = {}
            for profile in self._get_profile_instances():
                profile.parse_purpose(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "purpose"

            yield dataset_dict

    def creator(self):
        """
        Generator that returns foaf:Agent concepts (reached via dct:creator) parsed from the RDF graph

        Each creator object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._creator():
            dataset_dict = {}
            for profile in self._get_profile_instances():
                profile.parse_creator(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "creator"

            yield dataset_dict

    def attribution_agent(self):
        """
        Generator that returns foaf:Agent concepts (reached via prov:qualifiedAttribution/prov:agent)
        parsed from the RDF graph

        Each attribution agent object is passed to all the loaded profiles before being
        yielded, so it can be further modified by each one of them.

        Returns a dataset dict that can be passed to eg `package_create`
        or `package_update`
        """
        for dataset_ref in self._attribution_agent():
            dataset_dict = {}
            for profile in self._get_profile_instances():
                profile.parse_attribution_agent(dataset_dict, dataset_ref)

            dataset_dict["concept_type"] = "attribution_agent"

            yield dataset_dict

    def get_concept(self, uri_ref, concept_type):
        concept_dict = {}
        for profile in self._get_profile_instances():
            if concept_type == "publisher":
                profile.parse_publisher(concept_dict, uri_ref)
            elif concept_type == "kind":
                profile.parse_kind(concept_dict, uri_ref)
            elif concept_type == "dataset":
                profile.parse_dataset(concept_dict, uri_ref)
            elif concept_type == "datasetseries":
                profile.parse_datasetseries(concept_dict, uri_ref)
            elif concept_type == "provenancestatement":
                profile.parse_provenancestatement(concept_dict, uri_ref)
            elif concept_type == "purpose":
                profile.parse_purpose(concept_dict, uri_ref)
            elif concept_type == "creator":
                profile.parse_creator(concept_dict, uri_ref)
            elif concept_type == "attribution_agent":
                profile.parse_attribution_agent(concept_dict, uri_ref)

        return concept_dict

    def dataset_in_catalog(self):
        """
        Generator that returns URIRef of all datasets referred to in Catalogs
        """
        for catalog_ref in self._catalogs():
            yield from self.g.objects(catalog_ref, DCAT.dataset)
