# vanilla (prooflix-assets-bundle/workspace/vanilla)

Pipelines and code for [Prooflix](../../../prooflix-core). This folder is a Prooflix
workspace: point the server or CLI at it to validate and run the pipelines.

**Prooflix orchestrates, Spark executes.** Prooflix runs the pipelines (order,
sub-pipelines, retries, schedules, logs); the code of each task does its work with
Spark. Every table is a Delta table registered in Unity Catalog, read and written
through Spark.

```text
pipelines/               one YAML file per pipeline; the file name is the id
  e2e.yaml               end-to-end run: bronze, then silver, then gold
  bronze.yaml            streams new source files into the bronze Delta tables
  silver.yaml            streams new bronze rows into silver (dates as date-times), loads seeds
  gold.yaml              aggregates silver into gold tables
src/                     Python code the pipelines call
  runtime/
    spark_session.py     the Spark session every task uses (one per server process)
    catalog.py           resolves the tenant catalog in Unity Catalog: table names, locations
  infra/                 shared helpers
    config.py            reads the Spark configuration (conf/spark.yaml, environment)
    streaming.py         Structured Streaming plumbing: file and table streams, run once
    materialization.py   creating, overwriting and appending Delta tables
    transformations.py   common DataFrame transforms (long ms -> timestamp, daily counts, ...)
    csv_seed.py          loads the CSV dimension seeds
  bronze/ingest.py       used by bronze (entrypoint bronze.ingest:run)
  silver/conform.py      used by silver (entrypoint silver.conform:run)
  gold/aggregate.py      used by gold (entrypoint gold.aggregate:transactions_per_date)
seeds/                   CSV dimension seeds: countries, currencies, account and transaction types
conf/spark.yaml          Spark settings for local runs (environment variables override them)
scripts/
  generate_banking_data.py  writes large synthetic source files (see "Large test data")
  extend_banking_data.py    grows customers/accounts files to a target size
  grow_transactions.py      adds transaction files, splits accounts/customers into parts
  dates_to_long.py          stores dates and timestamps in the Avro files as longs (epoch ms)
source/                  raw input files, one folder per table (.avro); the workspace's
                         source data folder, set in the Workspace tab
data/                    the data lake (the workspace's data lake folder)
  tenant1/
    lakehouse/<schema>/<table>/   Delta files of tenant1.<schema>.<table>
    checkpoint/bronze/<table>/    Spark streaming checkpoints: what each stream has read
    checkpoint/silver/<table>/
```

## Spark, Delta and Unity Catalog

- `runtime/spark_session.py` creates one Spark session, the first time a task asks
  for it (`get_spark(ctx)`), with Delta Lake, the Unity Catalog connector and Avro.
  Tasks run in the Prooflix server process, so every task after that reuses it.
- `runtime/catalog.py` resolves the tenant catalog (`tenant1`, `PROOFLIX_UC_CATALOG`):
  it creates the catalog and the schemas in Unity Catalog when missing and registers
  the catalog in the session, so tables are `tenant1.bronze.accounts` and so on.
- Tables are external Delta tables: Unity Catalog holds their names and schemas,
  their files are in `data/tenant1/lakehouse/<schema>/<table>/`. Tasks read and write
  them by name, through Unity Catalog.
- Settings (`infra/config.py`): `conf/spark.yaml`, overridden by environment
  variables (`PROOFLIX_SPARK_MASTER`, `PROOFLIX_SPARK_DRIVER_MEMORY`, `PROOFLIX_UC_URI`,
  `PROOFLIX_UC_CATALOG`, ...), which the Helm chart sets in Kubernetes. Locally,
  Unity Catalog runs in Docker on port 8080 (see "Run it").

## How the pipelines fit together

```text
e2e
  ├─ run_bronze      pipeline: bronze
  │     └─ ingest    python: bronze.ingest:run   -> {tables, rows}
  ├─ run_silver      pipeline: silver (depends_on: run_bronze)
  │     └─ conform   python: silver.conform:run  -> {tables, rows, dimensions}
  └─ run_gold        pipeline: gold   (depends_on: run_silver)
        └─ agg_transactions_per_date
                     python: gold.aggregate:transactions_per_date -> {days, transactions, ...}
```

The order is bronze, silver, gold: each starts only after the one before succeeded,
and if one fails the rest are skipped.

## Bronze

Each folder of `source/` is a table: `source/accounts/*.avro` ->
`tenant1.bronze.accounts`. For each, a Structured Streaming query (trigger
`availableNow`) reads the files it hasn't read yet and appends their rows as they
are, plus `_source_file` and `_ingested_at`. Spark's checkpoint
(`data/tenant1/checkpoint/bronze/<table>/`) remembers the files, so each file is
ingested once: add data as new files. Delete a table's checkpoint folder to read
all its files again.

When a table has no checkpoint yet (the system is being initialized), only files
changed after the system start date (`PROOFLIX_START_DATE`, System tab) are read.

## Silver

For each bronze table, a stream reads the rows added since the last run and appends
them to the silver table of the same name, with every date as a date-time
(`timestamp`, UTC):

| Column | Table | In silver |
|---|---|---|
| `date_of_birth`, `joined_date` | customers | date-time, 00:00 UTC of the day |
| `opened_date` | accounts | date-time, 00:00 UTC of the day |
| `transaction_ts` | transactions | date-time, to the millisecond |

In the source files every date is a plain long, milliseconds since 1970-01-01 UTC.
The tables and columns are parameters (`tables`, `datetime_columns`).

Silver also loads the CSV seeds (`seeds/*.csv`) as dimension tables
`tenant1.silver.dim_<name>`, replaced on every run: `dim_countries`,
`dim_currencies`, `dim_account_types`, `dim_transaction_types`.

## Gold

`gold` counts the transactions of each day (UTC date of `transaction_ts`) in
`tenant1.silver.transactions` and writes `tenant1.gold.agg_transactions_per_date`:

| Column | Type | Meaning |
|---|---|---|
| `transaction_date` | date | the day |
| `transaction_count` | long | number of transactions that day |

The table is rebuilt from all of silver on every run, so it always matches silver.

## Large test data

`scripts/generate_banking_data.py` writes synthetic `customers`, `accounts` and
`transactions` Avro files of a given size, with the same schemas as the samples and
consistent data (every account has a customer, every transaction an account, running
balances add up). It needs fastavro and numpy, so run it with uv:

```bash
uv run --no-project --with fastavro --with numpy python scripts/generate_banking_data.py \
    --out source --customers-gb 1 --accounts-gb 1 --transactions-gb 5
```

That makes about 13M customers, 25M accounts and 104M transactions (7.3 GB) in under a
minute. Data files (`data/**/*.avro`, `data/**/*.parquet`, checkpoints) are in
`.gitignore`: they stay on disk and out of git, since GitHub refuses files over 100 MB.

## Run it

The tasks need Java 17, PySpark and a Unity Catalog server. In Kubernetes the
Prooflix image and Helm chart provide all three. Locally:

```bash
brew install openjdk@17
docker run -d --name prooflix-uc -p 8080:8080 -v prooflix-uc-db:/home/unitycatalog/etc/db \
    unitycatalog/unitycatalog:v0.7.0
```

Then, from `prooflix-core/server`:

```bash
uv run --extra spark prooflix --workspace ../../prooflix-assets-bundle/workspace/vanilla validate
uv run --extra spark prooflix --workspace ../../prooflix-assets-bundle/workspace/vanilla run e2e
```

Or open the bundle in the web UI:

```bash
uv run --extra spark prooflix --workspace ../../prooflix-assets-bundle/workspace/vanilla serve --reload
```

## Add a pipeline

1. Add `pipelines/<id>.yaml` (lowercase letters, digits, `_` or `-`).
2. Put any Python code it calls under `src/`.
3. Run `validate` to check references, dependencies and sub-pipeline calls.
