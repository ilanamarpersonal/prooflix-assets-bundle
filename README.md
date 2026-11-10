# prooflix-assets-bundle

Pipelines and code for [Prooflix](../prooflix-core). This folder is a Prooflix
workspace: point the server or CLI at it to validate and run the pipelines.

```text
pipelines/               one YAML file per pipeline; the file name is the id
  e2e.yaml               end-to-end run: bronze, then gold
  bronze.yaml            converts source files created after the checkpoint to Parquet
  gold.yaml              aggregates bronze into gold tables
src/                     Python code the pipelines call
  bronze/
    ingest.py            used by bronze (entrypoint bronze.ingest:run)
    avro_reader.py       pure-Python Avro reader (the server has no Avro library)
  gold/
    aggregate.py         used by gold (entrypoint gold.aggregate:transactions_per_date)
data/
  tenant1/                 one folder per tenant: the catalog "tenant1"
    source/                raw input files, any folder tree (.avro, .csv, .json, .parquet)
    checkpoint/
      last_checkpoint.txt  JSON: {"last_checkpoint": "<ISO 8601 date>", ...}
    bronze/                schema tenant1.bronze, written by bronze: same tree as source,
      accounts/              files as .parquet (one folder per table)
      customers/
      transactions/
    gold/                  schema tenant1.gold, written by gold
      agg_transactions_per_date/
```

## How the pipelines fit together

```text
e2e
  ├─ run_bronze      pipeline: bronze
  │     └─ ingest    python: bronze.ingest:run   -> {checkpoint, files, rows, written}
  └─ run_gold        pipeline: gold   (depends_on: run_bronze)
        └─ agg_transactions_per_date
                     python: gold.aggregate:transactions_per_date -> {days, transactions, ...}
```

- A pipeline has one or more **tasks**. A task runs Python code (`type: python`),
  a shell command (`type: shell`), or another pipeline (`type: pipeline`).
- **Order** comes from `depends_on`: a task starts only after the tasks it
  lists have succeeded. Tasks with no dependency between them run in parallel.
- A `python` task's `entrypoint` is `module:function`, looked up in `src/`.
- A called pipeline's `outputs` become the output of the task that called it.

Gold starts only after bronze succeeded; if bronze fails, gold is skipped. Add more
layers (such as silver) the same way, as pipelines called from `e2e` with `depends_on`.

The full format is in [prooflix-core/docs/pipelines.md](../prooflix-core/docs/pipelines.md).

## Bronze and the checkpoint

1. Reads `last_checkpoint` from `data/tenant1/checkpoint/last_checkpoint.txt`, a JSON
   file (a missing or empty file means "from the beginning").
2. Picks every file under `data/tenant1/source/` **created** after that date, skipping
   hidden files. Editing or touching a file doesn't change its creation time, so a
   file is converted once; add new data as new files. (On Linux, where the creation
   time isn't available, the modification time is used.)
3. Writes each one to the same relative path under `data/tenant1/bronze/` with a
   `.parquet` extension, for example
   `source/customers/customers.avro` -> `bronze/customers/customers.parquet`, which is
   the table `tenant1.bronze.customers`.
4. Saves the time the run started as the new checkpoint, so a file created during
   the run is picked up by the next one:

   ```json
   {
     "last_checkpoint": "2026-11-10T18:02:24.797382+00:00",
     "previous_checkpoint": "2026-11-10T17:55:36.850493+00:00",
     "run_id": "2fef9f0e81f1",
     "files": 3,
     "rows": 15856
   }
   ```

   Only `last_checkpoint` is read back; the other fields describe the run.

The folders and checkpoint file are parameters of `bronze` (`source_dir`,
`destination_dir`, `checkpoint_file`); another tenant can run with its own
folders by passing them. To reprocess everything, delete the
checkpoint file or set `last_checkpoint` to `1970-01-01T00:00:00+00:00`. Avro files must use the `null` or
`deflate` codec.

The Catalog and SQL Editor read the Parquet output with three-level names,
`catalog.schema.table`: each folder in `data/` is a catalog (the tenant ID), each folder
in it a schema (the layer), and a folder of Parquet files is one table. So
`data/tenant1/bronze/customers/` is `tenant1.bronze.customers`. `source/` and
`checkpoint/` hold no Parquet, so they add no tables.

## Gold

`gold` reads every Parquet file of `tenant1.bronze.transactions`
(`data/tenant1/bronze/transactions/`) and counts the transactions of each day (UTC
date of `transaction_ts`). It writes `tenant1.gold.agg_transactions_per_date`
(`data/tenant1/gold/agg_transactions_per_date/`):

| Column | Type | Meaning |
|---|---|---|
| `transaction_date` | date | the day |
| `transaction_count` | int64 | number of transactions that day |

The table is rebuilt from all of bronze on every run, so it always matches bronze.

```sql
SELECT * FROM tenant1.gold.agg_transactions_per_date ORDER BY transaction_date DESC;
```

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
