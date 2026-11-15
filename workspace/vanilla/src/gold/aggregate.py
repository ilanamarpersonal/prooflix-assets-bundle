"""Gold: business aggregates built from silver, with Spark.

`transactions_per_date` (entrypoint gold.aggregate:transactions_per_date) counts the
transactions of each calendar day (UTC) in silver.transactions and writes them to
gold.agg_transactions_per_date:

    transaction_date   date    the day (UTC) of transaction_ts
    transaction_count  long    number of transactions that day

The table is rebuilt from all of silver on every run, so it always matches silver.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from infra.materialization import overwrite, table_exists
from infra.transformations import daily_counts
from runtime.catalog import tenant_catalog
from runtime.spark_session import get_spark

TABLE = "agg_transactions_per_date"


def transactions_per_date(source_table: str = "silver.transactions", timestamp_column: str = "transaction_ts",
                          ctx=None) -> dict:
    log = ctx.log if ctx is not None else print
    spark = get_spark(ctx)
    catalog = tenant_catalog(ctx, schemas=("gold",))
    schema, table = source_table.split(".")[-2:]
    ref = catalog.table(schema, table)
    source = ref.name
    if not table_exists(spark, ref):
        raise FileNotFoundError(f"{source} doesn't exist; run the bronze and silver pipelines first")

    agg = daily_counts(spark.read.table(source), timestamp_column, "transaction_date", "transaction_count").cache()
    target = catalog.table("gold", TABLE)
    days = overwrite(agg, target)
    stats = agg.agg(F.min("transaction_date"), F.max("transaction_date"), F.sum("transaction_count")).first()
    agg.unpersist()
    first, last, total = stats[0], stats[1], int(stats[2] or 0)
    log(f"{source}: {total:,} transactions over {days} days ({first} to {last}) -> {target.name}")
    return {
        "table": target.name,
        "days": days,
        "transactions": total,
        "first_date": first.isoformat() if first else None,
        "last_date": last.isoformat() if last else None,
    }
