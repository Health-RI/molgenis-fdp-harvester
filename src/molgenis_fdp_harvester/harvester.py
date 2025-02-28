# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
Create a token on the host MOLGENIS server and store this token as an environment variable named MOLGENIS_TOKEN,
either by creating a .env file in the working directory containing the single line
MOLGENIS_TOKEN="..."
or by directly exporting the token to the working environment
$ export MOLGENIS_TOKEN="..."
The user creating this token requires editing permissions on the host schema.
"""
import logging
import os
from pathlib import Path

# Python < 3.11 does not have tomllib, but tomli provides same functionality
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

import click
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client

from .ckan_harvest.dcatrdfharvester import DCATRDFHarvester
from .ckan_harvest.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)

load_dotenv()
logging.basicConfig(level="INFO")

@click.command()
@click.option("--fdp", help="FAIR Data Point catalog URL to harvest", required=True)
@click.option("--host", help="MOLGENIS host to harvest to", required=True)
@click.option("--schema", help="Schema on MOLGENIS host to harvest to",
              required=False, default="Eucaim")
# @click.option(
#     "--table",
#     help="Table of MOLGENIS host to harvest to.",
#     required=False,
#     default="collections"
# )
@click.option(
    "--config",
    help="Configuration.",
    required=True,
    type=click.Path(exists=True, path_type=Path, readable=True)
)
@click.option(
    "--token", help="Authentication token of the user harvesting data.",
    required=False, default=os.environ.get("MOLGENIS_TOKEN")
)
def cli(
    fdp: str,
    host: str,
    schema: str,
    config: click.Path,
    token: str,
):
    with open(config, "rb") as fname:
        config = tomllib.load(fname)
    concept_table_dict = config['concept_table_link']

    harvest = DCATRDFHarvester([MolgenisEUCAIMDCATAPProfile], concept_table_dict)

    harvest.gather_stage(fdp)

    with Client(url=host, schema=schema, token=token) as client:
        harvest.fetch_stage(client)
        for object in harvest._harvest_objects:
            harvest.import_stage(object, client)


if __name__ == "__main__":
    cli()
