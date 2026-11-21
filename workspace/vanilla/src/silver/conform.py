"""Silver: the bronze tables with their dates as date-times, plus the dimension seeds.

Called by the `silver` pipeline (entrypoint silver.conform:run). For each bronze
table, a Structured Streaming query reads the rows added since the last run and
appends them to the silver table of the same name, with every date column as a
date-time (timestamp, UTC):

    date_of_birth, joined_date, opened_date   long ms -> date-time at 00:00 UTC
    transaction_ts                            long ms -> date-time, to the millisecond

The CSV seeds (seeds/*.csv) are loaded as dimension tables silver.dim_<name> on
every run. tenant_id picks the tenant catalog (tenant1 by default).
"""

from __future__ import annotations

from pathlib import Path

from infra.csv_seed import load_seeds
from infra.materialization import table_exists
from infra.streaming import run_once, table_stream
from infra.transformations import to_datetimes
from runtime.catalog import tenant_catalog
from runtime.paths import workspace_path
from runtime.spark_session import get_spark

SOURCE, TARGET = "bronze", "silver"


def run(tables: list[str], datetime_columns: list[str], checkpoint_dir: str, seeds_dir: str = "seeds",
        tenant_id: str | None = None, ctx=None) -> dict:
    root = ctx.workspace if ctx is not None else Path.cwd()
    log = ctx.log if ctx is not None else print
    spark = get_spark(ctx)
    catalog = tenant_catalog(ctx, schemas=(TARGET,), catalog=tenant_id)
    checkpoints = workspace_path(root, checkpoint_dir)

    out = {}
    for table in tables:
        source, target = catalog.table(SOURCE, table), catalog.table(TARGET, table)
        if not table_exists(spark, source):
            raise FileNotFoundError(f"{source.name} doesn't exist; run the bronze pipeline first")
        df = to_datetimes(table_stream(spark, source.name), datetime_columns)
        result = run_once(df, target, checkpoints / table, query_name=f"silver_{table}")
        out[table] = result["rows"]
        log(f"{source.name} -> {target.name}: {result['rows']:,} new rows")

    seeds = load_seeds(spark, workspace_path(root, seeds_dir), catalog, TARGET, log)
    return {"tables": out, "rows": sum(out.values()), "dimensions": seeds}
