# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
Create a token on the host MOLGENIS server and store this token as an environment variable named MOLGENIS_TOKEN:
$ export MOLGENIS_TOKEN="..."
"""
import os

import click
from molgenis_emx2_pyclient import Client

from ckan_harvest.dcatrdfharvester import DCATRDFHarvester
from ckan_harvest.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)


@click.command()
@click.option("--fdp", help="FAIR Data Point catalog URL to harvest", required=True)
@click.option("--host", help="MOLGENIS host to harvest to", required=True)
@click.option("--schema", help="Schema on MOLGENIS host to harvest to",
              required=False, default="Eucaim")
@click.option(
    "--table",
    help="Table of MOLGENIS host to harvest to (e.g. EUCAIM_collections)",
    required=False,
    default="collections"
)
@click.option(
    "--token", help="Authentication token of the user harvesting data.",
    required=False, default=os.environ.get("MOLGENIS_TOKEN")
)


def cli(
    fdp: str,
    host: str,
    schema: str,
    table: str,
    token: str,
):
    harvest = DCATRDFHarvester([MolgenisEUCAIMDCATAPProfile], table)

    harvest.gather_stage(fdp)

    with Client(url=host, schema=schema, token=token) as client:
        harvest.fetch_stage(client)
        for object in harvest._harvest_objects:
            harvest.import_stage(object, client)


if __name__ == "__main__":
    cli()
