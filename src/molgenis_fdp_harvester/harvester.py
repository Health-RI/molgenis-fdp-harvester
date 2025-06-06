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

from molgenis_fdp_harvester.fdp import FDPHarvester

# Python < 3.11 does not have tomllib, but tomli provides same functionality
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

import click
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client

from .rdf import DCATRDFHarvester
from .base.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)

load_dotenv()
logging.basicConfig(level="INFO")

@click.command()
@click.option("--fdp", help="FAIR Data Point catalog URL to harvest", required=True)
@click.option("--host", help="MOLGENIS host to harvest to", required=True)
@click.option("--schema", help="Schema on MOLGENIS host to harvest to",
              required=False, default="Eucaim")
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
@click.option("--input_type", type=click.Choice(['rdf', 'fdp']), required=True)
def cli(
    fdp: str,
    host: str,
    schema: str,
    config: click.Path,
    token: str,
    input_type: str
):
    with open(config, "rb") as fname:
        config = tomllib.load(fname)
    concept_table_dict = config['concept_table_link']
    concept_type_order = {'person': 0, 'datasetseries': 1, 'dataset': 2}


    with Client(url=host, schema=schema, token=token) as client:
        if input_type == 'rdf':
            harvest = DCATRDFHarvester([MolgenisEUCAIMDCATAPProfile], concept_table_dict, client)
        else:  # input_type == 'fdp'
            harvest = FDPHarvester([MolgenisEUCAIMDCATAPProfile], concept_table_dict, client)

        harvest.gather_stage(fdp)
        harvest._harvest_objects.sort(key=lambda obj: concept_type_order[obj.concept_type])
        for object in harvest._harvest_objects:
            object = harvest.fetch_stage(object)
            print(object.content)
        #     harvest.import_stage(object)


if __name__ == "__main__":
    cli()
