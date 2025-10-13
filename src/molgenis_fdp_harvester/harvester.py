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
@click.option("--input_type", type=click.Choice(['rdf']), required=True)
def cli(
    fdp: str,
    host: str,
    schema: str,
    config: click.Path,
    token: str,
    input_type: str
):
    """Run the harvester with the specified configuration."""
    # Load configuration
    config_data = load_config(config)
    concept_table_dict = config_data['concept_table_link']
    
    # Define processing order for concept types
    CONCEPT_TYPE_ORDER = {'person': 0, 'datasetseries': 1, 'dataset': 2}

    with Client(url=host, schema=schema, token=token) as client:
        # Create appropriate harvester
        harvester = create_harvester(input_type, concept_table_dict, client)
        
        # Execute harvesting process
        execute_harvest(harvester, fdp, CONCEPT_TYPE_ORDER)

def load_config(config_path):
    """Load and parse the configuration file."""
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def create_harvester(input_type, concept_table_dict, client):
    """Create the appropriate harvester based on input type."""
    profiles = [MolgenisEUCAIMDCATAPProfile]
    
    if input_type == 'rdf':
        return DCATRDFHarvester(profiles, concept_table_dict, client)
    else:
        raise ValueError(f"Unknown input_type: {input_type}")

def execute_harvest(harvester, source_url, concept_type_order):
    """Execute the complete harvesting process."""
    # Gather objects to harvest
    harvester.gather_stage(source_url)
    
    # Sort by dependency order
    harvester._harvest_objects.sort(
        key=lambda obj: concept_type_order[obj.concept_type]
    )
    
    # Process each object
    for harvest_object in harvester._harvest_objects:
        harvest_object = harvester.fetch_stage(harvest_object)
        harvester.import_stage(harvest_object)

if __name__ == "__main__":
    cli()