# SPDX-FileCopyrightText: 2023 Civity
# SPDX-FileContributor: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-only

import encodings
import logging
import xml.sax

import requests
import rdflib.plugin
from rdflib import Graph, URIRef
from rdflib.exceptions import ParserError
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

log = logging.getLogger(__name__)


class FairDataPoint:
    """Class to connect and get data from FDP"""

    def __init__(self, fdp_end_point: str):
        self.fdp_end_point = fdp_end_point
        # Transport and parse failures, logged and kept here for the harvester to drain, so
        # a run against a broken FDP is not reported as clean.
        self.errors: list[str] = []

    def drain_errors(self) -> list[str]:
        """Return the errors recorded since the last call, and forget them."""
        errors = self.errors
        self.errors = []
        return errors

    def _record_error(self, message: str, *, exc_info: bool = False) -> None:
        log.error("%s", message, exc_info=exc_info)
        self.errors.append(message)

    def get_graph(self, path: str | URIRef) -> Graph:
        """
        Get graph from FDP at specified path. Not using function to load graph from endpoint directly since this
        function fails because of a certificate error. The library it uses probably has no certificates which would
        have to be added to a trust store. But this is inconvenient in case of a new harvester which refers to an
        endpoint whose certificate is not in the trust store yet.
        """
        graph = Graph()
        data = self._get_data(path)
        if data is None:
            # _get_data has already reported why.
            log.debug("No data was received from FDP %s request %s", self.fdp_end_point, path)
        else:
            try:
                graph.parse(data=data)
            # There is no single exception type covering all rdflib parser backends. The same
            # set is caught in RDFParser.parse (base/processor.py) for the same reason.
            except (
                ParserError,
                SyntaxError,
                xml.sax.SAXParseException,
                rdflib.plugin.PluginException,
                TypeError,
            ) as e:
                self._record_error(
                    f"Record from FDP {self.fdp_end_point} at {path} could not be parsed: {e}",
                    exc_info=True,
                )
        return graph

    def _get_data(self, path: str | URIRef) -> str | None:
        headers = {"Accept": "text/turtle"}
        try:
            response = requests.request("GET", path, headers=headers, timeout=30)
            response.encoding = encodings.utf_8.getregentry().name
            response.raise_for_status()
            return response.text
        except (HTTPError, ConnectionError, Timeout, RequestException) as e:
            self._record_error(f"FDP query {path} was not successful: {e}")
            return None
