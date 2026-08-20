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
from pathlib import Path

import click
import yaml
from dotenv import find_dotenv, load_dotenv
from molgenis_emx2_pyclient import Client

from molgenis_fdp_harvester.rdf_harvester.rdf import DCATRDFHarvester

from .base.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)
from .config import load_config
from .fdp_harvester.fdp import FDPHarvester
from .logging_config import configure_logging

# See .env.example for every variable this reads.
# Loaded at import time because click resolves option envvars before cli() runs. usecwd
# keeps .env relative to where the harvester is run from; the default resolves it relative
# to this file, which in an installed deployment is somewhere in site-packages.
load_dotenv(find_dotenv(usecwd=True))

log = logging.getLogger(__name__)


def read_fdp_list(yaml_path: Path) -> list[tuple[str, str | None]]:
    """Read FDP entries from a YAML file.

    Expects a top-level list under ``fdps`` where each item contains
    ``fdp_url`` and optional ``fdp_id_prefix`` values. Blank or missing prefixes
    are normalized to ``None``.
    """
    with Path(yaml_path).open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    entries = []
    raw_entries = data.get("fdps", []) if isinstance(data, dict) else []
    fdp_entries = raw_entries if isinstance(raw_entries, list) else []
    for entry in fdp_entries:
        if not isinstance(entry, dict):
            continue
        fdp_url = str(entry.get("fdp_url", "")).strip()
        if not fdp_url:
            continue
        fdp_id_prefix = entry.get("fdp_id_prefix")
        prefix_value = None if fdp_id_prefix is None else str(fdp_id_prefix).strip() or None
        entries.append((fdp_url, prefix_value))
    return entries


@click.command()
@click.option("--fdp", envvar="FDP_URL", help="FAIR Data Point catalog URL to harvest", required=False, default=None)
@click.option(
    "--fdp-list",
    envvar="FDP_LIST_PATH",
    help="Path to YML file with columns fdp_url and fdp_id_prefix (one FDP per row)",
    required=False,
    default=None,
    type=click.Path(exists=True, path_type=Path, readable=True),
)
@click.option("--host", envvar="MOLGENIS_HOST", help="MOLGENIS host to harvest to", required=False, default=None)
@click.option(
    "--schema", envvar="MOLGENIS_SCHEMA", help="Schema on MOLGENIS host to harvest to", required=False, default="Eucaim"
)
@click.option(
    "--config",
    envvar="HARVEST_CONFIG",
    help="Configuration.",
    required=False,
    default=None,
    type=click.Path(exists=True, path_type=Path, readable=True),
)
@click.option(
    "--token",
    envvar="MOLGENIS_TOKEN",
    help="Authentication token of the user harvesting data.",
    required=False,
    default=None,
)
@click.option("--input_type", envvar="INPUT_TYPE", type=click.Choice(["rdf", "fdp"]), required=False, default=None)
@click.option(
    "--fdp-id-prefix",
    envvar="FDP_ID_PREFIX",
    help="FDP ID prefix used for PID construction. Only used with --fdp.",
    required=False,
    default=None,
)
# ruff: noqa: PLR0913, PLR0917
def cli(
    fdp: str,
    fdp_list: Path,
    host: str,
    schema: str,
    config: click.Path,
    token: str,
    input_type: str,
    fdp_id_prefix: str,
):
    """Run the harvester with the specified configuration."""
    # Not at import time: importing this module must not take over logging for its importer.
    configure_logging()

    validate_options(fdp, fdp_list, host, config, token, input_type)

    # Load configuration
    config_data = load_config(config)
    concept_table_dict = config_data["concept_table_link"]
    harvester_config = config_data.get("harvester_config", {})

    # Build uniform list of (fdp_url, fdp_id_prefix) entries
    if fdp:
        fdp_entries = [(fdp, fdp_id_prefix)]
    else:
        fdp_entries = read_fdp_list(fdp_list)
        if not fdp_entries:
            raise click.ClickException(f"FDP list file '{fdp_list}' contains no valid entries.")

    # Define processing order for concept types
    concept_type_order = {
        "provenancestatement": 0,
        "kind": 1,
        "publisher": 2,
        "datasetseries": 3,
        "dataset": 4,
    }

    # An unreachable MOLGENIS is an expected operational failure, so it gets a logged
    # reason and an exit code rather than a traceback. Client.__enter__ does no I/O.
    try:
        molgenis_client = Client(url=host, schema=schema, token=token)
    except Exception as exc:
        log.exception("Could not connect to MOLGENIS at %s: %s", host, exc)
        raise click.ClickException(
            f"Could not connect to MOLGENIS at {host}. See the log output above for details."
        ) from exc

    had_errors = False
    with molgenis_client as client:
        for entry_fdp_url, entry_fdp_id_prefix in fdp_entries:
            entry_config = dict(harvester_config)
            if entry_fdp_id_prefix is not None:
                entry_config["fdp_id_prefix"] = entry_fdp_id_prefix

            harvester = create_harvester(input_type, concept_table_dict, client, entry_config)
            try:
                success = execute_harvest(harvester, entry_fdp_url, concept_type_order)
            except Exception as exc:
                # One failing FDP must not abort the rest of the list.
                log.exception("Unexpected error while harvesting %s: %s", entry_fdp_url, exc)
                had_errors = True
                continue

            if not success:
                had_errors = True

    if had_errors:
        raise click.ClickException(
            "One or more FAIR Data Points failed to harvest cleanly. See the log output above for details."
        )


def validate_options(fdp, fdp_list, host, config, token, input_type):
    """Check the options Click cannot enforce itself, because they allow an env var fallback."""
    missing_required = [
        (token, "Authentication token is required. Either set the MOLGENIS_TOKEN environment "
                "variable or provide the --token option."),
        (host, "MOLGENIS host is required. Set MOLGENIS_HOST or provide --host."),
        (config, "Configuration file is required. Set HARVEST_CONFIG or provide --config."),
        (input_type, "Input type is required. Set INPUT_TYPE or provide --input_type."),
    ]
    for value, message in missing_required:
        if not value:
            raise click.ClickException(message)

    if fdp and fdp_list:
        raise click.UsageError("--fdp and --fdp-list are mutually exclusive. Provide only one.")
    if not fdp and not fdp_list:
        raise click.UsageError("One of --fdp or --fdp-list is required.")


def create_harvester(input_type, concept_table_dict, client, harvester_config):
    """Create the appropriate harvester based on input type."""
    profiles = [MolgenisEUCAIMDCATAPProfile]
    for profile in profiles:
        profile.config = harvester_config

    if input_type == "rdf":
        return DCATRDFHarvester(profiles, concept_table_dict, client, harvester_config)
    if input_type == "fdp":
        return FDPHarvester(profiles, concept_table_dict, client, harvester_config)
    raise ValueError(f"Unknown input_type: {input_type}")


def execute_harvest(harvester, source_url, concept_type_order):
    """Execute the complete harvesting process for a single FDP.

    Individual object failures are recorded by the harvester itself and do not stop the
    run. Returns whether the harvest was clean, so a scheduler can tell a good run from
    one that needs attention.
    """
    # Gather objects to harvest
    harvester.gather_stage(source_url)

    # Process fetch stage for all objects to identify datasets without datasetseries
    for harvest_object in harvester._harvest_objects:
        harvester.fetch_stage(harvest_object)

    # Generate missing datasetseries and update dataset references
    harvester.generate_missing_datasetseries()

    # Sort by dependency order (now including auto-generated datasetseries)
    harvester._harvest_objects.sort(key=lambda obj: concept_type_order[obj.concept_type])

    # Import all objects in dependency order. A failing object never stops the rest.
    for harvest_object in harvester._harvest_objects:
        harvester.import_stage(harvest_object)

    total_objects = len(harvester._harvest_objects)

    if harvester.has_errors:
        log.warning(
            "Harvest for %s completed with errors: %d gather/validation error(s), "
            "%d import error(s), %d object(s) processed",
            source_url, harvester.gather_error_count, harvester.import_error_count, total_objects,
        )
        return False

    if not total_objects:
        # An FDP is allowed to be empty, but harvesting nothing usually means a
        # misconfigured or unreachable endpoint.
        log.warning("Harvest for %s completed without errors but produced no objects", source_url)
        return True

    log.info("Harvest for %s completed successfully: %d object(s) processed", source_url, total_objects)
    return True


if __name__ == "__main__":
    cli()
