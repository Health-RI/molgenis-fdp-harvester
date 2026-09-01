# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A FAIR Data Point (FDP) harvester for MOLGENIS EMX2. It reads DCAT-AP metadata either from a plain RDF endpoint or from a FAIR Data Point's linked-data (LDP) structure, maps it via a MOLGENIS/EUCAIM DCAT-AP profile, and upserts the result into a MOLGENIS EMX2 catalogue schema. Distributed as a pip package (entry point `harvest`) and as a Docker image (`ghcr.io/health-ri/molgenis-fdp-harvester`).

## Development commands

Dependency/venv management is via `uv` (lockfile `uv.lock` — don't hand-edit, regenerate with `uv lock`).

```console
uv sync                     # install project + dev deps (test, lint, types groups)
uv run pytest               # run the test suite
uv run pytest tests/test_config.py                        # single file
uv run pytest tests/test_config.py::test_validate_config_valid  # single test
uv run pytest -k <expr>     # run tests matching a keyword expression
uv run coverage run -m pytest && uv run coverage report    # tests with coverage
uv run ruff check .         # lint
uv run ruff format .        # format
uv run mypy src             # type-check
```

CI (`.github/workflows/test-python.yml`) runs the same pytest command across Python 3.10/3.11/3.12 with `--doctest-modules`; `test-lint.yml` runs `ruff check .` on PRs. Match these before pushing.

## Running the harvester

```console
harvest --fdp <url> --host <molgenis-host> --schema <schema> --config config.toml --token <token> --input_type fdp|rdf
```

`--fdp` and `--fdp-list` (a YAML file with a `fdps:` list of `fdp_url` entries) are mutually exclusive; exactly one is required. Every CLI option has an equivalent env var (`MOLGENIS_TOKEN`, `MOLGENIS_HOST`, `MOLGENIS_SCHEMA`, `INPUT_TYPE`, `HARVEST_CONFIG`, `FDP_URL`, `FDP_LIST_PATH`) — these are how the Docker image and the `dev/` Compose stack configure it.

## Architecture

Entry point: `cli()` in `src/molgenis_fdp_harvester/harvester.py`. Per configured FDP URL, it builds a harvester object (`create_harvester()`) and runs `execute_harvest()`, which drives a 4-stage pipeline defined in `base/baseharvester.py::HarvesterBase` (adapted from CKAN's `ckanext-harvest`, AGPL-3.0):

1. **gather_stage** — discover which RDF/FDP resources ("concepts": dataset, datasetseries, publisher, creator, distribution, etc.) exist upstream, diff their GUIDs against what's already in MOLGENIS (via `molgenis_client.get()`), and produce `HarvestObject`s only for new ones.
2. **fetch_stage** — parse each object's RDF into a plain dict via the DCAT-AP profile, resolve ontology term URIs to human-readable MOLGENIS lookup-table values (`_resolve_uris_and_labels`, driven by `harvester_config.uri_lookup_config` in the TOML config), and serialize to JSON on the `HarvestObject`.
3. A dependency-order sort (provenancestatement → purpose → legalbasis → rightsstatement → kind → publisher → creator → attribution_agent → dataservice → distribution → datasetseries → dataset) ensures referenced rows exist before the rows that reference them.
4. **import_stage** — upsert each object into MOLGENIS via `molgenis_client.save_table()` (dataset rows go through `_upsert_collections()`, which dedupes on `other_identifier.notation` + agency; everything else goes through the generic `_upsert_table()`).

Two harvester implementations share this pipeline (`rdf_harvester/rdf.py::DCATRDFHarvester` is the base; `fdp_harvester/fdp.py::FDPHarvester` extends it):
- `DCATRDFHarvester` — fetches a single RDF document from an HTTP endpoint and parses it directly.
- `FDPHarvester` — first crawls the FAIR Data Point's LDP structure breadth-first (`fdp_harvester/domain/fair_data_point_record_provider.py`), fetching each linked record's Turtle and flattening it into one RDF graph, then hands off to the same gather/fetch/import machinery as `DCATRDFHarvester`.

Key modules:
- `base/processor.py::RDFProcessor`/`RDFParser` — parses RDF into an `rdflib.Dataset`, skolemizes blank nodes, and exposes per-concept-type generators (`datasets()`, `publisher()`, `distribution()`, etc.) that delegate to a profile's `parse_*` methods. Watch for the documented edge cases here: `_rightsstatement()` walks `_distribution()`'s whole-graph scan and pulls each distribution's `dct:rights` — it deliberately does *not* walk `dataset -> adms:sample/healthdcatap:analytics -> distribution -> dct:rights` the way `_creator()`/`_attribution_agent()` walk their dataset-borne predicate paths, because at least one real FDP implementation models `adms:sample`/`healthdcatap:analytics` as an LDP DirectContainer membership relation that gets silently dropped on a plain-triple submission, even though the distribution's own description round-trips fine elsewhere in the same document (see "Dev FDP fixtures" below). A naive whole-graph type scan for `dct:RightsStatement` isn't safe either, since `dct:accessRights` on a dataset is also commonly typed `dct:RightsStatement`.
- `base/molgenis_dcat_profile.py::MolgenisEUCAIMDCATAPProfile` — the actual DCAT→MOLGENIS field-mapping logic (adapted from `ckanext-dcat`'s `euro_dcat_ap.py`), e.g. `parse_creator()`/`parse_rightsstatement()` turning a resolved RDF ref into a MOLGENIS row dict.
- `config.py` — TOML config loading/validation (`HarvesterConfig`, `ConceptTableLink` dataclasses). `concept_table_link` maps internal concept names to MOLGENIS table names; `harvester_config.uri_lookup_config` maps dataset fields to MOLGENIS ontology lookup tables.

## Configuration

`config.toml` (root) is the reference example config: `[concept_table_link]` (e.g. `dataset = "collections"`, `datasetseries = "biobanks"`) and `[harvester_config]` (`auto_create_datasetseries`, `pid_service_url`, `uri_lookup_config.dataset.*`). Validation lives in `config.py::validate_config()` — required `concept_table_link` fields raise `ValueError` if missing (see `tests/test_config.py` for the exact list).

## Local dev stack (`dev/docker-compose.yml`)

Spins up two reference FAIR Data Points (`fairdata/fairdatapoint`, each backed by MongoDB + GraphDB and preloaded via `schema-tool-{1,2}`), a MOLGENIS EMX2 instance + Postgres, and the harvester itself, wired together to exercise a full FDP→MOLGENIS harvest locally. `fdp-init` seeds the FDPs using `Health-RI/fairclient`, installed from a **local checkout** (not PyPI) — clone `https://github.com/Health-RI/fairclient` separately and point `FAIRCLIENT_PATH` at it (defaults to `~/Documents/Repositories/fairclient`; override per-machine, don't commit a personal path). See the README's "Local FDP/Molgenis stack" section for the exact startup order (`fdp-client-* → schema-tool-* → fdp-init → molgenis → molgenis-init`) and the manual step of uploading the EUCAIM metadata model Excel file (`dev/molgenis/D5.3 - EUCAIM - Molgenis metadata model - <month year>.xlsx`, one dated copy per profile revision) into MOLGENIS.

**MOLGENIS id columns must have their computed/visible formula cleared for every table the harvester upserts a UUID into.** If a table's `id` column still carries the default server-computed hash formula, MOLGENIS silently overrides the UUID the harvester sends, and any other row that references it by that id fails on upload with a foreign-key violation (`... is not present in table "<table>", column(s)("id")`) — this bit both the tables made independent on this branch *and*, separately, older tables (`kind`, `publisher`, `provenance_statement`, `purpose`) whose Python-side fix had shipped without a matching Excel-model change. When adding a new independently-upserted table, clear this in the Excel model at the same time as the Python change, not after.

### Dev FDP fixtures (`dev/fdp/data/*.ttl`)

These are hand-written Turtle files, not RDF captured from a real system, so two things can silently make them unrepresentative of a live FDP:

- **Validate against the EUCAIM SHACL shapes before wiring a new/changed fixture into `upload_data.py`** — `fdp-init` rejects anything that fails: `pyshacl` the file against `dev/fdp/shacl-eucaim-<date>` (matching the properties file's date, e.g. `properties-2026-08-03.yaml` ↔ `shacl-eucaim-2026-08-03`). Shapes that have bitten fixtures before: `adms:sample`, `healthdcatap:analytics`, `dcat:accessService`, and `dpv:hasLegalBasis` are all `sh:nodeKind sh:IRI` — they must reference a separate top-level resource, never an inline blank node; `dcat:Distribution` requires `dcatap:applicableLegislation` and `dcat:accessService` (min 1 each); `dcat:DataService` requires `dct:publisher` (min 1); and there is no shape at all for a locally-typed `dpv:LegalBasis` node in this model — `hasLegalBasis` is only ever a plain external vocabulary IRI.
- **Passing SHACL isn't enough — a real FDP's store/serve round-trip can silently drop or orphan things SHACL permits.** Observed against a live FDP (`docker compose up harvester`, then GET the dataset back from the FDP host to compare against what was submitted): resources nested more than one level below the dataset's own subject (e.g. `dataset -> distribution (own IRI) -> dct:rights` as a blank node) can lose their typing/label triples on the way back out even though SHACL's `BlankNodeEditor` allows the blank node. Give any such nested resource its own top-level IRI instead of an inline blank node — this is *in addition to*, not replaced by, the `sh:nodeKind sh:IRI` requirement above, since it also affects properties SHACL doesn't force to be IRIs.
- After editing a fixture, re-verify it end to end: `pyshacl` for shape conformance, then exercise `RDFParser` directly (or run the dev stack) to confirm the concept generators you intended to exercise (`creator()`, `legalbasis()`, `attribution_agent()`, `dataservice()`, `distribution()`, `rightsstatement()`, etc.) still pick up the expected values.
