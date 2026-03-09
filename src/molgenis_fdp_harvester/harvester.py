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
import csv
import logging
import os
from pathlib import Path

from .fdp_harvester.fdp import FDPHarvester

# Python < 3.11 does not have tomllib, but tomli provides same functionality
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

import click
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester
from .base.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)
from .config import load_config

# Environment variables:
# MOLGENIS_TOKEN
load_dotenv()
logging.basicConfig(level="INFO")


def read_fdp_list(csv_path: Path, has_header: bool) -> list[tuple[str, str | None]]:
    """Read FDP entries from a CSV file.

    Expects columns: fdp_url, fdp_id_prefix (prefix column is optional/can be blank).
    Rows that are entirely blank are skipped.
    """
    entries = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        if has_header:
            next(reader, None)  # consume header row
        for row in reader:
            if not row or not row[0].strip():
                continue  # skip blank rows
            fdp_url = row[0].strip()
            fdp_id_prefix = row[1].strip() if len(row) > 1 and row[1].strip() else None
            entries.append((fdp_url, fdp_id_prefix))
    return entries


@click.command()
@click.option("--fdp", help="FAIR Data Point catalog URL to harvest", required=False, default=None)
@click.option(
    "--fdp-list",
    help="Path to CSV file with columns fdp_url and fdp_id_prefix (one FDP per row)",
    required=False,
    default=None,
    type=click.Path(exists=True, path_type=Path, readable=True)
)
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
    required=False, default=lambda: os.environ.get("MOLGENIS_TOKEN")
)
@click.option("--input_type", type=click.Choice(['rdf', 'fdp']), required=True)
@click.option(
    "--fdp-id-prefix",
    help="FDP ID prefix used for PID construction. Only used with --fdp.",
    required=False,
    default=None
)
def cli(
    fdp: str,
    fdp_list: Path,
    host: str,
    schema: str,
    config: click.Path,
    token: str,
    input_type: str,
    fdp_id_prefix: str
):
    """Run the harvester with the specified configuration."""
    # Check that token is provided
    if not token:
        raise click.ClickException(
            "Authentication token is required. Either set the MOLGENIS_TOKEN environment "
            "variable or provide the --token option."
        )

    # Validate mutual exclusivity of --fdp and --fdp-list
    if fdp and fdp_list:
        raise click.UsageError("--fdp and --fdp-list are mutually exclusive. Provide only one.")
    if not fdp and not fdp_list:
        raise click.UsageError("One of --fdp or --fdp-list is required.")

    # Load configuration
    config_data = load_config(config)
    concept_table_dict = config_data['concept_table_link']
    harvester_config = config_data.get('harvester_config', {})

    # Build uniform list of (fdp_url, fdp_id_prefix) entries
    if fdp:
        fdp_entries = [(fdp, fdp_id_prefix)]
    else:
        has_header = harvester_config.get('fdp_list_has_header', True)
        fdp_entries = read_fdp_list(fdp_list, has_header)

    # Define processing order for concept types
    CONCEPT_TYPE_ORDER = {
        'provenancestatement': 0,
        'kind': 1,
        'publisher': 2,
        'datasetseries': 3,
        'dataset': 4
    }

    with Client(url=host, schema=schema, token=token) as client:
        for entry_fdp_url, entry_fdp_id_prefix in fdp_entries:
            entry_config = dict(harvester_config)
            if entry_fdp_id_prefix is not None:
                entry_config['fdp_id_prefix'] = entry_fdp_id_prefix

            harvester = create_harvester(input_type, concept_table_dict, client, entry_config)
            execute_harvest(harvester, entry_fdp_url, CONCEPT_TYPE_ORDER)


def create_harvester(input_type, concept_table_dict, client, harvester_config):
    """Create the appropriate harvester based on input type."""
    profiles = [MolgenisEUCAIMDCATAPProfile]
    for profile in profiles:
        profile.config = harvester_config

    if input_type == 'rdf':
        return DCATRDFHarvester(profiles, concept_table_dict, client, harvester_config)
    elif input_type == 'fdp':
        return FDPHarvester(profiles, concept_table_dict, client, harvester_config)
    else:
        raise ValueError(f"Unknown input_type: {input_type}")

def execute_harvest(harvester, source_url, concept_type_order):
    """Execute the complete harvesting process."""
    # Gather objects to harvest
    harvester.gather_stage(source_url)

    # Process fetch stage for all objects to identify datasets without datasetseries
    for harvest_object in harvester._harvest_objects:
        harvest_object = harvester.fetch_stage(harvest_object)

    # Generate missing datasetseries and update dataset references
    harvester.generate_missing_datasetseries()

    # Sort by dependency order (now including auto-generated datasetseries)
    harvester._harvest_objects.sort(
        key=lambda obj: concept_type_order[obj.concept_type]
    )

    # Import all objects in dependency order
    for harvest_object in harvester._harvest_objects:
        harvester.import_stage(harvest_object)

if __name__ == "__main__":
    cli()
