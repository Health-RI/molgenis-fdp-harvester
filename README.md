# MOLGENIS FDP Harvester

-----

## Installation

```console
git clone https://github.com/Health-RI/molgenis-fdp-harvester.git
cd molgenis-fdp-harvester
pip install .
```

Authentication for submitting catalogue entries to the receiving catalogue is done through access tokens.
In the Molgenis EMX2 catalogue generate an access token by clicking 'Hi {username}' in the top right corner,
and entering a token name under 'Create a token'. Store this token in an environment file `.env`, by writing
`MOLGENIS_TOKEN={token}`. This will be loaded when starting the harvester. 


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

The structure of every harvester is as follows:
- `harvester.gather_stage()`: Collect all IDs of the classes to harvest and compare these IDs with the existing ones in the catalogue. This step creates `HarvestObject`s with just the ID (`guid`) and the class/concept type.
- `harvester.fetch_stage()`: Iterate over all `HarvestObject`s and retrieve the content of the classes. 
- `harvester.generate_missing_datasetseries()`: The current EUCAIM Molgenis model requires a DatasetSeries, so for Datasets that do not have DatasetSeries, one will be created.
- Sorting: There is a dependency between the classes, e.g., when a Dataset is submitted any referenced Agents already need to be known in the catalogue. To allow for this, the `HarvestObject`s are sorted based on concept type.
- `harvester.import_stage()`: Submit the `HarvestObject`s to the Molgenis catalogue through the APIs. 

The most straightforward of the two harvesters is the `DCATRDFHarvester`. 
This harvester is an adaptation of the [CKAN DCAT harvester](https://github.com/ckan/ckanext-dcat).
In  `harvest.gather_stage()`, the entire RDF is retrieved from the endpoint and converted to a graph.
The harvested RDF gets converted to a Graph, from which the objects are subsequently extracted. In this process 
`RDFParser` is used for the conversion and the extraction.

In the `harvester.fetch_stage()` method the concepts are 
retrieved from the graph and parsed by the profile `MolgenisEUCAIMDCATAPProfile`, using the 
respective methods per concept. 
In this stage the contents of the `HarvestObject`, e.g., a Dataset, which were harvested from the endpoint, are 
renamed to key-value combinations that are compatible with the receiving Molgenis catalogue. 

Following this conversion, the `harvester.import_stage()` is called per `HarvestObject`. The `import_stage()` method
takes the information and performs the Molgenis API calls to submit the objects to the EUCAIM catalogue.

The `FDPHarvester` follows the same stages. 
It is based on the [GDI FAIR Data Point harvester for CKAN](https://github.com/GenomicDataInfrastructure/gdi-userportal-ckanext-fairdatapoint).
The major difference between an FDP endpoint and an RDF endpoint, is that in an FDP endpoint not all metadata is presented at once.
You start at a central point, and through LDP (Linked Data Platform) structures all classes, e.g., Datasets, are linked.
To harvest the endpoint, first all linked classes are collected.
For the traversal of the endpoint, the `FairDataPointRecordProvider` class is used. This will handle the exploration
of the linked LDP structures and return the record, e.g., Dataset, IDs, through the `FairDataPointRecordProvider.get_record_ids()`
method. In `FDPHarvester.gather_stage()` these record IDs are converted into `HarvestObject`s.

In `FDPHarvester.fetch_stage()` the RDF data related to the ID in the `HarvestObject` is retrieved and converted to a graph.
Similar to the `DCATRDFHarvester` the `fetch_stage()` method then extracts the classes from this graph.
This content is then added to the `HarvestObject`.
After sorting the `HarvestObject`s are then submitted to the Molgenis catalogue using the `import_stage()` method
which is inherited from the `DCATRDFHarvester` class.

