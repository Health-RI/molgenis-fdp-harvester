# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import click
from molgenis_emx2_pyclient import Client

from ckan_harvest.dcatrdfharvester import DCATRDFHarvester
from ckan_harvest.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)


@click.command()
@click.option("--fdp", help="FAIR Data Point catalog to harvest", required=True)
@click.option("--host", help="MOLGENIS host to harvest to", required=False)
@click.option(
    "--entity",
    help="Entity of MOLGENIS host to harvest to (e.g. EUCAIM_collections)",
    required=False,
)
@click.option(
    "--username", help="Username of MOLGENIS host to harvest to", required=False
)
@click.password_option(confirmation_prompt=False, required=False)
def cli(
    fdp: str,
    host: str,
    entity: str,
    username: str,
    password: str,
):
    harvest = DCATRDFHarvester([MolgenisEUCAIMDCATAPProfile], entity)

    harvest.gather_stage(fdp)

    with Client(url=host, schema="Eucaim") as client:
        client.signin(username, password)
        harvest.fetch_stage(client)
        for object in harvest._harvest_objects:
            harvest.import_stage(object, client)


if __name__ == "__main__":
    cli()
