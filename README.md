# MOLGENIS FDP Harvester

[![PyPI - Version](https://img.shields.io/pypi/v/molgenis-fdp-harvester.svg)](https://pypi.org/project/molgenis-fdp-harvester)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/molgenis-fdp-harvester.svg)](https://pypi.org/project/molgenis-fdp-harvester)

-----

## Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
git clone https://github.com/Health-RI/molgenis-fdp-harvester.git 
cd molgenis-fdp-harvester
pip install .
```

Authentication for submitting catalogue entries to the receiving catalogue is done through access tokens.
In the Molgens EMX2 catalogue generate an access token by clicking 'Hi {username}' in the top right corner,
and entering a token name under 'Create a token'. Store this token in an environment file `.env`, by writing
`MOLGENIS_TOKEN={token}`. This will be loaded when starting the harvester. 

## License

This program is open and licensed under the GNU Affero General Public License (AGPL) v3.0.
Its full text may be found at:

<http://www.fsf.org/licensing/licenses/agpl-3.0.html>

## Process documentation

The entry point of the application is the CLI command `harvest`, which executes `cli` in `./molgenis_fdp_harvester/harvester.py`.
In `cli`, the configuration is loaded, and a connection with the receiving Molgenis catalogue is set up.
With this connection client, a harvester object is created in `create_harvester()` based on the type of endpoint to be harvested.
This returns either a `DCATRDFHarvester` object for an RDF endpoint or an `FDPHarvester` object for harvesting from a FAIR Data Point from the FDP reference implementation,
as the `harvester` object.
In `create_harvester()` the profile, in this case `MolgenisEUCAIMDCATAPProfile` is given as input to the harvester object.
After creating the harvester, the harvest is executed in `execute_harvest()`. 

This starts with `harvest.gather_stage()`, which retrieves all the items to harvest from the supplied endpoint.
The harvested RDF gets converted to a Graph, from which the objects are subsequently extracted. In this process 
`RDFParser` is used for the conversion and the extraction.

To make sure to adhere to all dependencies between the harvested objects, the list of HarvestObjects are sorted.
After sorting, the method `harvester.fetch_stage()` is called per HarvestObject. 
In this stage the contents of the HarvestObject, e.g., a Dataset, which were harvested from the endpoint, are 
renamed to key-value combinations that are compatible with the receiving Molgenis catalogue. The methods 
for this renaming can be found in the profile, in our case `MolgenisEUCAIMDCATAPProfile`.

Following this conversion, the `harvester.import_stage()` is called per HarvestObject. The `import_stage()` method
takes the information and performs the Molgenis API calls to submit the objects to the EUCAIM catalogue.

