#!/usr/bin/env python3
"""Upload test data to the dev FAIR Data Points.

Pushes a catalog and a dataset, linked to that catalog, to both dev FDPs.

Usage:
    python dev/fdp/upload_data.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from fairclient.fdpclient import FDPClient
from rdflib import DCAT, DCTERMS, RDF, Graph, URIRef

logger = logging.getLogger(__name__)

FDP_USER = "albert.einstein@example.com"
FDP_PASSWORD = "password"

DATA_DIR = Path(__file__).parent / "data"

# Each FDP gets its own catalog and dataset files. A catalog declares dct:isPartOf pointing
# at the repository root of the FDP it lives in, and an FDP rejects a parent URI it does
# not own with "Resource with provided uri prefix is not defined".
TTL_PER_FDP = {
    "http://fdp-1.test": (
        DATA_DIR / "test-catalog-1.ttl",
        (DATA_DIR / "test-data-1.ttl", DATA_DIR / "test-data-3.ttl"),
    ),
    "http://fdp-2.test": (
        DATA_DIR / "test-catalog-2.ttl",
        (DATA_DIR / "test-data-2.ttl", DATA_DIR / "test-data-4.ttl"),
    ),
}


def load_graph(path: Path) -> Graph:
    if not path.is_file():
        msg = f"Template not found at {path}"
        raise FileNotFoundError(msg)
    return Graph().parse(path, format="turtle")


def link_to_catalog(dataset: Graph, catalog_uri: URIRef) -> Graph:
    """Points the dct:isPartOf of every dcat:Dataset at the created catalog.

    This replaces the __CATALOG_UUID__ placeholder that test-data.ttl ships with.
    """
    subjects = list(dataset.subjects(RDF.type, DCAT.Dataset, unique=True))
    if not subjects:
        msg = "No dcat:Dataset subject found in the dataset graph"
        raise ValueError(msg)
    for subject in subjects:
        dataset.remove((subject, DCTERMS.isPartOf, None))
        dataset.add((subject, DCTERMS.isPartOf, catalog_uri))
    return dataset


def upload(fdp_url: str, catalog_ttl: Path, dataset_ttls: tuple[Path, ...]) -> None:
    """Creates and publishes a catalog and linked datasets on a single FDP."""
    catalog_graph = load_graph(catalog_ttl)

    client = FDPClient(fdp_url, FDP_USER, FDP_PASSWORD)

    catalog_uri = client.create_and_publish("catalog", catalog_graph)
    logger.info("%s: created and published catalog %s", fdp_url, catalog_uri)

    for dataset_ttl in dataset_ttls:
        dataset_graph = load_graph(dataset_ttl)
        link_to_catalog(dataset_graph, catalog_uri)
        dataset_uri = client.create_and_publish("dataset", dataset_graph)
        logger.info("%s: created and published dataset %s", fdp_url, dataset_uri)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for fdp_url, (catalog_ttl, dataset_ttl) in TTL_PER_FDP.items():
        upload(fdp_url, catalog_ttl, dataset_ttl)
    return 0


if __name__ == "__main__":
    sys.exit(main())
