# SPDX-FileCopyrightText: 2023 Civity
# SPDX-FileContributor: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-only


import logging
from typing import Dict, Iterable, Union
from collections import deque

import requests
from rdflib import DCAT, DCTERMS, RDF, BNode, Graph, Literal, Namespace, URIRef
from rdflib.term import Node
from requests import HTTPError, JSONDecodeError

from .fair_data_point import FairDataPoint
from .graph_to_fdp_record_mapper import GraphToFdpRecordMapper
from .identifier import Identifier

LDP = Namespace("http://www.w3.org/ns/ldp#")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")

log = logging.getLogger(__name__)


class FairDataPointRecordProvider:

    def __init__(self, fdp_end_point: str):
        self.fair_data_point = FairDataPoint(fdp_end_point)

    def get_record_ids(self, concept_type: str = 'all') -> Dict.keys:
        log.debug(f"FAIR Data Point get_records from {self.fair_data_point.fdp_end_point}")
        result = {}
        for fdp_record in self._breadth_first_search_records(self.fair_data_point.fdp_end_point):
            if fdp_record.is_catalog() and concept_type in {'catalog', 'all'}:
                identifier = Identifier("")
                identifier.add("catalog", str(fdp_record.url))
                result[identifier.guid] = fdp_record.url
            elif fdp_record.is_dataset() and concept_type in {'dataset', 'all'}:
                identifier = Identifier("")
                identifier.add("dataset", str(fdp_record.url))
                result[identifier.guid] = fdp_record.url
            elif fdp_record.is_datasetseries() and concept_type in {'datasetseries', 'all'}:
                identifier = Identifier("")
                identifier.add("datasetseries", str(fdp_record.url))
                result[identifier.guid] = fdp_record.url
            elif fdp_record.is_person() and concept_type in {'person', 'all'}:
                identifier = Identifier("")
                identifier.add("person", str(fdp_record.url))
                result[identifier.guid] = fdp_record.url
        return result.keys()

    def get_record_by_id(self, guid: str) -> str:
        """
        Get additional information for FDP record.
        """
        log.debug(
            f"FAIR data point get_record_by_id from {self.fair_data_point.fdp_end_point} for {guid}"
        )

        identifier = Identifier(guid)

        subject_url = identifier.get_id_value()

        g = self.fair_data_point.get_graph(subject_url)

        subject_uri = URIRef(subject_url)

        self._remove_fdp_defaults(g, subject_uri)

        # Add information from distribution to graph
        for distribution_uri in g.objects(
            subject=subject_uri, predicate=DCAT.distribution
        ):
            distribution_g = self.fair_data_point.get_graph(distribution_uri)

            self._remove_fdp_defaults(g, distribution_uri)

            for predicate in [
                DCTERMS.description,
                DCTERMS.format,
                DCTERMS.license,
                DCTERMS.title,
                DCAT.accessURL,
            ]:
                for distr_attribute_value in self.get_values(
                    distribution_g, distribution_uri, predicate
                ):
                    g.add((distribution_uri, predicate, distr_attribute_value))

        # Look-up contact information
        for contact_point_uri in self.get_values(g, subject_uri, DCAT.contactPoint):
            if isinstance(contact_point_uri, URIRef):
                self._parse_contact_point(
                    g=g, subject_uri=subject_uri, contact_point_uri=contact_point_uri
                )

        return g.serialize(format="ttl")

    @staticmethod
    def _parse_contact_point(g: Graph, subject_uri: URIRef, contact_point_uri: URIRef):
        """
        Replaces contact point URI with a VCard
        """
        g.remove((subject_uri, DCAT.contactPoint, contact_point_uri))
        vcard_node = BNode()
        g.add((subject_uri, DCAT.contactPoint, vcard_node))
        g.add((vcard_node, RDF.type, VCARD.Kind))
        g.add((vcard_node, VCARD.hasUID, contact_point_uri))
        if "orcid" in str(contact_point_uri):
            try:
                orcid_response = requests.get(
                    str(contact_point_uri).rstrip("/") + "/public-record.json"
                )
                json_orcid_response = orcid_response.json()
                name = json_orcid_response["displayName"]
                g.add((vcard_node, VCARD.fn, Literal(name)))
            except (JSONDecodeError, HTTPError) as e:
                log.error(f"Failed to get data from ORCID for {contact_point_uri}: {e}")

    @staticmethod
    def get_values(
        graph: Graph,
        subject: Union[str, URIRef, Node],
        predicate: Union[str, URIRef, Node],
    ) -> Iterable[Node]:
        subject_uri = URIRef(subject)
        predicate_uri = URIRef(predicate)

        yield from graph.objects(subject=subject_uri, predicate=predicate_uri)

    def _map_record(self, url: str):
        mapper = GraphToFdpRecordMapper(url)
        graph = self.fair_data_point.get_graph(url)
        return mapper.map(graph)

    def _breadth_first_search_records(self, start_url: str):
        queue = deque([start_url])
        visited = set()
        while queue:
            url = queue.popleft()
            if url in visited:
                continue
            visited.add(url)
            record = self._map_record(url)
            if record:
                yield record
                queue.extend(record.children())

    @staticmethod
    def _remove_fdp_defaults(g, subject_uri):
        for s, p, o in g.triples((subject_uri, DCTERMS.accessRights, None)):
            access_rights_default = URIRef(f"{subject_uri}#accessRights")
            if o == access_rights_default:
                g.remove((subject_uri, DCTERMS.accessRights, o))
                g.remove((access_rights_default, None, None))
