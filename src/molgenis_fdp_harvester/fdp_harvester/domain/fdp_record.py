# SPDX-FileCopyrightText: 2023 Civity
# SPDX-FileContributor: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-only

from rdflib import DCAT, RDF, URIRef, FOAF, DCTERMS

from ...base.baseparser import VCARD, DPV


class FdpRecord:
    def __init__(self, url, graph):
        self.url = url
        self._children = set()
        self._graph = graph

    def children(self):
        return self._children

    def get_type(self, concept_type: str) -> str:
        return_type = None
        if self.is_catalog() and concept_type in {'catalog', 'all'}:
            return_type = 'catalog'
        elif self.is_dataset() and concept_type in {'dataset', 'all'}:
            return_type = 'dataset'
        elif self.is_datasetseries() and concept_type in {'datasetseries', 'all'}:
            return_type = 'datasetseries'
        elif self.is_kind() and concept_type in {'kind', 'all'}:
            return_type = 'kind'
        elif self.is_provenancestatement() and concept_type in {'provenancestatement', 'all'}:
            return_type = 'provenancestatement'
        elif self.is_publisher() and concept_type in {'publisher', 'all'}:
            return_type = 'publisher'
        elif self.is_purpose() and concept_type in {'purpose', 'all'}:
            return_type = 'purpose'
        return return_type

    def add_children(self, child_url):
        self._children.add(child_url)

    def is_catalog(self):
        return (URIRef(self.url), RDF.type, DCAT.Catalog) in self._graph

    def is_dataset(self):
        return (URIRef(self.url), RDF.type, DCAT.Dataset) in self._graph

    def is_datasetseries(self):
        return (URIRef(self.url), RDF.type, DCAT.DatasetSeries) in self._graph

    def is_kind(self):
        return (URIRef(self.url), RDF.type, VCARD.Kind) in self._graph

    def is_publisher(self):
        return (URIRef(self.url), RDF.type, FOAF.Organization) in self._graph

    def is_provenancestatement(self):
        return (URIRef(self.url), RDF.type, DCTERMS.ProvenanceStatement) in self._graph

    def is_purpose(self):
        return (URIRef(self.url), RDF.type, DPV.Purpose) in self._graph

    def is_person(self):
        return ((URIRef(self.url), RDF.type, FOAF.Person) in self._graph
                or (URIRef(self.url), RDF.type, FOAF.Organization) in self._graph
                or (URIRef(self.url), RDF.type, VCARD.Kind) in self._graph
                )
