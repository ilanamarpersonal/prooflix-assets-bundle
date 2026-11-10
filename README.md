# prooflix-assets-bundle

Pipelines and code for [Prooflix](../prooflix-core). This folder is a Prooflix
workspace: point the server or CLI at it to validate and run the pipelines.

```text
pipelines/               one YAML file per pipeline; the file name is the id
  e2e.yaml               end-to-end run: triggers bronze
  bronze.yaml            converts source files newer than the checkpoint to Parquet
src/                     Python code the pipelines call
  bronze/
    ingest.py            used by bronze (entrypoint bronze.ingest:run)
    avro_reader.py       pure-Python Avro reader (the server has no Avro library)
data/
  tenant1/
    source/                raw input files, any folder tree (.avro, .csv, .json, .parquet)
      checkpoint/
        last_checkpoint.txt  date of the last bronze run (ISO 8601)
    destination/           written by bronze: same tree as source, files as .parquet
```

## How the pipelines fit together

```text
e2e
  └─ run_bronze      pipeline: bronze
        └─ ingest    python: bronze.ingest:run   -> {checkpoint, files, rows, written}
```

- A pipeline has one or more **tasks**. A task runs Python code (`type: python`),
  a shell command (`type: shell`), or another pipeline (`type: pipeline`).
- **Order** comes from `depends_on`: a task starts only after the tasks it
  lists have succeeded. Tasks with no dependency between them run in parallel.
- A `python` task's `entrypoint` is `module:function`, looked up in `src/`.
- A called pipeline's `outputs` become the output of the task that called it.

Add later layers (silver, gold) as pipelines and call them from `e2e` with
`depends_on: [run_bronze]`.

The full format is in [prooflix-core/docs/pipelines.md](../prooflix-core/docs/pipelines.md).

## Bronze and the checkpoint

1. Reads the date in `data/tenant1/source/checkpoint/last_checkpoint.txt` (a missing or
   empty file means "from the beginning").
2. Picks every file under `data/tenant1/source/` modified after that date, skipping the
   `checkpoint/` folder and hidden files.
3. Writes each one to the same relative path under `data/tenant1/destination/` with a
   `.parquet` extension, for example
   `source/customers/customers.avro` -> `destination/customers/customers.parquet`.
4. Saves the time the run started as the new checkpoint, so a file changed during
   the run is picked up by the next one.

The folders and checkpoint file are parameters of `bronze` (`source_dir`,
`destination_dir`, `checkpoint_file`); another tenant can run with its own
folders by passing them. To reprocess everything, set the
checkpoint back to `1970-01-01T00:00:00+00:00`. Avro files must use the `null` or
`deflate` codec.

The Catalog and SQL Editor read the Parquet output: a folder of Parquet files is one
table, so `data/tenant1/destination/customers/` is the table
`tenant1_destination_customers` (schema `main`).

## Run it

From `prooflix-core/server`:

```bash
uv run prooflix --workspace ../../prooflix-assets-bundle validate
uv run prooflix --workspace ../../prooflix-assets-bundle run e2e
```

Or open the bundle in the web UI:

```bash
uv run prooflix --workspace ../../prooflix-assets-bundle serve --reload
```

## Add a pipeline

1. Add `pipelines/<id>.yaml` (lowercase letters, digits, `_` or `-`).
2. Put any Python code it calls under `src/`.
3. Run `validate` to check references, dependencies and sub-pipeline calls.
