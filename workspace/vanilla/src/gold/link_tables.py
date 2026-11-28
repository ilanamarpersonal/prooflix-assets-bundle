"""Gold: the tables of InsightIQ's computed links, with Spark SQL.

`build` (entrypoint gold.link_tables:build) is called by the pipelines InsightIQ's Object Model
generates for its computed links (pipelines/object-link-<type>-<link>.yaml, one per link, run by
hand or on the link's schedule). It runs the link's SQL, a SELECT that gives pairs of keys:

    from_key   a key of the link's source type (the value of its "from" property)
    to_key     a key of the target type (the value of its "to" property)

and replaces the rows of the link table, <catalog>.gold.link_<type>_<link>, with the distinct
pairs that have both keys. The table is rebuilt whole on every run, so it always matches its SQL.
"""

from __future__ import annotations

from infra.materialization import overwrite
from runtime.catalog import tenant_catalog
from runtime.spark_session import get_spark


def build(table: str, sql: str, ctx=None) -> dict:
    log = ctx.log if ctx is not None else print
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError(f"the link table must be catalog.schema.table, not {table}")
    catalog_name, schema, name = parts
    spark = get_spark(ctx)
    catalog = tenant_catalog(ctx, schemas=(schema,), catalog=catalog_name)
    ref = catalog.table(schema, name)

    result = spark.sql(sql)
    missing = [c for c in ("from_key", "to_key") if c not in result.columns]
    if missing:
        raise ValueError(f"the link's SQL must give the columns from_key and to_key (missing: {', '.join(missing)})")
    pairs = result.select("from_key", "to_key").where("from_key IS NOT NULL AND to_key IS NOT NULL").distinct()
    rows = overwrite(pairs, ref)
    log(f"{ref.name}: {rows:,} pairs")
    return {"table": ref.name, "pairs": rows}
