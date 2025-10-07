# MOLGENIS FDP Harvester

-----

## Installation

```console
git clone https://github.com/Health-RI/molgenis-fdp-harvester.git
cd molgenis-fdp-harvester
pip install .
```

## Usage

```console
Usage: harvest [OPTIONS]

Options:
  --fdp TEXT     FAIR Data Point catalog URL to harvest  [required]
  --host TEXT    MOLGENIS host to harvest to  [required]
  --schema TEXT  Schema on MOLGENIS host to harvest to
  --config PATH  Configuration.  [required]
  --token TEXT   Authentication token of the user harvesting data.
  --help         Show this message and exit.
```
The configuration contains a linking table between the concept types, used internally in the script to separate the
handling of the different concepts, and the table in the harvesting MOLGENIS catalogue.

## Workings

This harvester is an adaptation of the [CKAN DCAT harvester](https://github.com/ckan/ckanext-dcat). 
In this harvester the method `DCATRDFHarvester.gather_stage()` is used to first load the RDF data from the FAIR Data 
Point into a graph using `rdflib` and an `RDFParser` object. After that the datasets, datasetseries and persons are 
retrieved from the graph and parsed by the profile `MolgenisEUCAIMDCATAPProfile`, using the methods 
`RDFParser.datatsets()`, `RDFParser.datasetseries()`, and `RDFParser.persons()` respectively. The gather stage
ends with the creation of a list of `HarvestObject`s. With `DCATRDFHarvester.fetch_stage()`, it is checked if 
the IDs of the concepts are unique. In the `DCATRDFHarvester.import_stage()` method the API calls to the harvesting
MOLGENIS catalogue are made.

This harvester is tested on https://catalogue-eucaim.grycap.i3m.upv.es/Eucaim/api/rdf, where the metadata is shown
on one big page. In other FAIR Data Points multiple links need to be traversed to for example get the dataset metadata. 
The harvester has not been tested with those kinds of FAIR Data Points.



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