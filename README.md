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

No authentication is needed to read a public FAIR Data Point.


## Usage

```console
Usage: harvest [OPTIONS]

Options:
  --fdp TEXT            FAIR Data Point catalog URL to harvest
  --fdp-list PATH       Path to CSV file with columns fdp_url and fdp_id_prefix (one FDP per row)
  --host TEXT           MOLGENIS host to harvest to
  --schema TEXT         Schema on MOLGENIS host to harvest to
  --config PATH         Configuration.
  --token TEXT          Authentication token for the Molgenis catalogue of the user harvesting data.
  --input_type TEXT     Type of endpoint to harvest from: 'rdf' or 'fdp'.
  --fdp-id-prefix TEXT  FDP ID prefix used for PID construction. Optional.
  --help                Show this message and exit.
```

Either `--fdp` (single URL) or `--fdp-list` (CSV file) must be provided; they are mutually exclusive.

The `--fdp-list` CSV file must have columns `fdp_url` and `fdp_id_prefix` (one FDP per row). Whether the
file has a header row is controlled by `fdp_list_has_header` in the TOML configuration (default: `true`).

The configuration contains a linking table between the concept types, used internally in the script to separate the
handling of the different concepts, and the table in the harvesting MOLGENIS catalogue.

When `--fdp-id-prefix` is provided, it is prepended to plain-string identifiers to form the record `id`
(e.g. `myprefix-mydataset`), and the PID service URL is used to construct the full `identifier`.
When omitted, the plain-string identifier is used as-is for `id`, and the PID service URL is still applied.

Every CLI option can also be set via an environment variable, which is the recommended approach when running
in Docker or Kubernetes:

| Environment variable | CLI option        | Required               |
|----------------------|-------------------|------------------------|
| `MOLGENIS_TOKEN`     | `--token`         | Yes                    |
| `MOLGENIS_HOST`      | `--host`          | Yes                    |
| `INPUT_TYPE`         | `--input_type`    | Yes                    |
| `HARVEST_CONFIG`     | `--config`        | Yes                    |
| `MOLGENIS_SCHEMA`    | `--schema`        | No (default: `Eucaim`) |
| `FDP_URL`            | `--fdp`           | One of                 |
| `FDP_LIST_PATH`      | `--fdp-list`      | One of                 |
| `FDP_ID_PREFIX`      | `--fdp-id-prefix` | No                     |

## Docker

Pre-built images are published to the GitHub Container Registry and can be pulled with:

```console
docker pull ghcr.io/health-ri/molgenis-fdp-harvester:<tag>
```

### Running with a single FDP

Mount your TOML configuration file and pass all settings via environment variables:

```console
docker run --rm \
  -e MOLGENIS_TOKEN=<your-token> \
  -e MOLGENIS_HOST=https://your-molgenis-host \
  -e INPUT_TYPE=fdp \
  -e FDP_URL=https://fdp.example.com \
  -e HARVEST_CONFIG=/app/config.toml \
  -v /path/to/your/config.toml:/app/config.toml \
  ghcr.io/health-ri/molgenis-fdp-harvester:<tag>
```

### Running with a CSV list of FDPs

```console
docker run --rm \
  -e MOLGENIS_TOKEN=<your-token> \
  -e MOLGENIS_HOST=https://your-molgenis-host \
  -e INPUT_TYPE=fdp \
  -e FDP_LIST_PATH=/app/fdps.csv \
  -e HARVEST_CONFIG=/app/config.toml \
  -v /path/to/your/config.toml:/app/config.toml \
  -v /path/to/your/fdps.csv:/app/fdps.csv \
  ghcr.io/health-ri/molgenis-fdp-harvester:<tag>
```

The CSV file format:

```csv
fdp_url,fdp_id_prefix
https://fdp1.example.com,prefix1
https://fdp2.example.com,prefix2
https://fdp3.example.com,
```

### Building the image locally

```console
docker build -t molgenis-fdp-harvester:local .
```


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
method. In `FDPHarvester.gather_stage()` the metadata from the FDP is flattened to RDF, after which the `gather_stage()` 
and the stages after that from `DCATRDFHarvester` are used. 
