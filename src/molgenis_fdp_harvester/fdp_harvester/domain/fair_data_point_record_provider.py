# SPDX-FileCopyrightText: 2023 Civity
# SPDX-FileContributor: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-only


import logging
from typing import Dict, Iterable, Union
from collections import deque

from rdflib import DCTERMS, Graph, URIRef
from rdflib.term import Node

from .fair_data_point import FairDataPoint
from .graph_to_fdp_record_mapper import GraphToFdpRecordMapper
from .identifier import Identifier


log = logging.getLogger(__name__)


class FairDataPointRecordProvider:

    def __init__(self, fdp_end_point: str):
        self.fair_data_point = FairDataPoint(fdp_end_point)

    def get_record_ids(self, concept_type: str = 'all') -> Dict.keys:
        log.debug(f"FAIR Data Point get_records from {self.fair_data_point.fdp_end_point}")
        result = {}
        for fdp_record in self._breadth_first_search_records(self.fair_data_point.fdp_end_point):
            record_type = fdp_record.get_type(concept_type)
            if record_type:
                identifier = Identifier("")
                identifier.add(record_type, str(fdp_record.url))
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

        return g.serialize(format="ttl")

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
