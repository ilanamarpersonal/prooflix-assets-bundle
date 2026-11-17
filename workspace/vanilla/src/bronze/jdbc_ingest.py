"""Bronze: copies tables of a relational database into Delta tables, with Spark over JDBC.

Called by the ingestion jobs the Ingestion tab defines (entrypoint
bronze.jdbc_ingest:run): one pipeline per job, run by hand or on its cron schedule.
Each table of the job becomes tenant1.bronze.<target>, with the chosen columns
(all when none are chosen) plus:

    _source_table   "<connection>:<schema.table>"
    _ingested_at    when this run wrote the row

How rows are picked depends on the table's watermark column:

- With a watermark (a date, timestamp or increasing integer column, such as
  updated_at or an id), each run appends only the new rows: those whose watermark
  is above the highest value already in bronze. The first run, when the bronze
  table is empty, starts from the job's start date (rows with watermark >= it),
  else from the system start date (PROOFLIX_START_DATE), else takes every row. An
  integer watermark ignores the start dates. Rows changed without moving the
  watermark forward, and deleted rows, are not seen.
- Without a watermark, each run copies the whole table again and replaces the
  bronze table's rows (fine for small reference tables).

Rows already in bronze are overwritten, never duplicated, when the table has key
columns (`keys`, by default the source table's primary key): new rows are merged
into bronze (Delta MERGE), so a row whose key is there already is replaced by its
newest version (the one with the highest watermark when a batch has several) and
the others are added. Without keys, new rows are appended.

A run can instead be given two dates (`from_date`, `to_date`, both days included;
the Ingestion tab's "Ingest dates" button): every table with a date or timestamp
watermark then copies only the rows in that range and, without keys, replaces the
rows of that range in bronze (with keys, merges them as above), so a range can be ingested again without duplicating rows (a
backfill, or a correction). Leaving one of the dates out leaves that side open.
Tables with an integer watermark or none are skipped in such a run. The next normal
run continues from the highest watermark in bronze, so ingesting a range later than
the data already there moves that point forward.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

from infra.jdbc import literal, quote, read, select_sql
from infra.materialization import ensure_table, overwrite, table_exists
from runtime.catalog import tenant_catalog
from runtime.spark_session import get_spark

SCHEMA = "bronze"


def _start(job_start: str | None, ctx) -> str | None:
    """Where a first run starts, as "YYYY-MM-DD HH:MM:SS" (UTC), or None for all rows."""
    if job_start:
        return date.fromisoformat(job_start).isoformat() + " 00:00:00"
    system = getattr(ctx, "start_date", None)
    if system is not None:
        return system.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return None


def _range(from_date: str | None, to_date: str | None) -> tuple[str | None, str | None]:
    """[low, high) as "YYYY-MM-DD 00:00:00": from the first day to the day after the last."""
    low = date.fromisoformat(from_date) if from_date else None
    last = date.fromisoformat(to_date) if to_date else None
    if low and last and last < low:
        raise ValueError(f"to_date {to_date} is before from_date {from_date}")
    fmt = lambda d: d.isoformat() + " 00:00:00"  # noqa: E731
    return (fmt(low) if low else None, fmt(last + timedelta(days=1)) if last else None)


def _merge(df, ref, keys: list[str], watermark: str | None) -> dict:
    """Upsert df into the table by its key columns: matching rows are overwritten, others inserted."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = df.sparkSession
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"key column {', '.join(missing)} is not in the copied columns")
    ensure_table(spark, ref, df.schema)
    if watermark and watermark in df.columns:  # several versions of a row in one batch: keep the newest
        newest = Window.partitionBy(*keys).orderBy(F.col(watermark).desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(newest)).where("_rn = 1").drop("_rn")
    else:
        df = df.dropDuplicates(keys)
    view = f"jdbc_src_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    q = lambda name: "`" + name.replace("`", "``") + "`"  # noqa: E731 - column names come from the database
    on = " AND ".join(f"t.{q(k)} <=> s.{q(k)}" for k in keys)  # <=>: null keys match too
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # new columns are added
    try:
        res = spark.sql(
            f"MERGE INTO {ref.name} t USING {view} s ON {on} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        ).first()
    finally:
        spark.catalog.dropTempView(view)
    stats = res.asDict() if res is not None else {}
    return {"updated": int(stats.get("num_updated_rows", 0)), "inserted": int(stats.get("num_inserted_rows", 0))}


def _ingest_range(spark, conn, kind, source, columns, watermark, ref, tag, low, high, keys) -> tuple[int, str]:
    """Copy the rows whose watermark is in [low, high) and replace that range in bronze."""
    from pyspark.sql import functions as F

    q = quote(kind, watermark)
    where = " AND ".join(
        ([f"{q} >= {literal(kind, low)}"] if low else []) + ([f"{q} < {literal(kind, high)}"] if high else [])
    )
    df = read(spark, conn, select_sql(kind, source, columns, where))
    df = df.withColumn("_source_table", F.lit(tag)).withColumn("_ingested_at", F.current_timestamp()).cache()
    try:
        rows = df.count()
        if keys:
            m = _merge(df, ref, keys, watermark)
            return rows, f"{rows:,} rows in the range: {m['updated']:,} overwritten, {m['inserted']:,} added"
        ensure_table(spark, ref, df.schema)
        col = F.col(watermark)
        cond = F.lit(True)
        if low:
            cond = cond & (col >= F.to_timestamp(F.lit(low)))
        if high:
            cond = cond & (col < F.to_timestamp(F.lit(high)))
        # replaces exactly the rows of the range (and adds the new ones) in one commit
        df.writeTo(ref.name).option("mergeSchema", "true").overwrite(cond)
    finally:
        df.unpersist()
    return rows, f"{rows:,} rows in the range (the range replaced)"


def _high_water(spark, ref, column: str, integer: bool):
    """The highest watermark already in bronze, or None."""
    from pyspark.sql import functions as F

    if not table_exists(spark, ref) or column not in spark.read.table(ref.name).columns:
        return None
    col = F.max(F.col(column))
    # formatted in Spark (session time zone UTC), not converted by Python
    value = spark.read.table(ref.name).agg(col if integer else F.date_format(col, "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()[0]
    return int(value) if integer and value is not None else value


def run(
    connection: str,
    tables: list[dict],
    start_date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    ctx=None,
) -> dict:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    if ctx is None:
        raise RuntimeError("bronze.jdbc_ingest runs as a Prooflix task (it needs ctx.connection)")
    log = ctx.log
    conn = ctx.connection(connection)
    kind = conn["kind"]
    spark = get_spark(ctx)
    catalog = tenant_catalog(ctx, schemas=(SCHEMA,))
    log(f"{connection}: {kind} at {conn['jdbc_url']}")
    in_range = bool(from_date or to_date)
    low, high = _range(from_date, to_date)
    if in_range:
        log(f"ingesting {from_date or 'the beginning'} to {to_date or 'now'} (days included), "
            "replacing that range in bronze")
    out = {}
    skipped = []
    for t in tables:
        source, target = t["source"], t["target"]
        columns, watermark = list(t.get("columns") or []), t.get("watermark")
        keys = list(t.get("keys") or [])
        ref = catalog.table(SCHEMA, target)
        tag = F.lit(f"{connection}:{source}")

        if in_range:
            probe = read(spark, conn, select_sql(kind, source, columns, where="1 = 0"))
            dtype = probe.schema[watermark].dataType if watermark and watermark in probe.columns else None
            if not isinstance(dtype, (T.DateType, T.TimestampType, T.TimestampNTZType)):
                why = "no watermark" if not watermark else f"{watermark} is not a date or timestamp"
                log(f"{source}: skipped ({why}; a date range needs a date watermark)")
                skipped.append(source)
                continue
            rows, note = _ingest_range(spark, conn, kind, source, columns, watermark, ref, f"{connection}:{source}",
                                 low, high, keys)
            log(f"{source} -> {ref.name}: {note}")
            out[target] = rows
            continue

        if not watermark:
            df = read(spark, conn, select_sql(kind, source, columns))
            rows = overwrite(df.withColumn("_source_table", tag).withColumn("_ingested_at", F.current_timestamp()), ref)
            log(f"{source} -> {ref.name}: full copy, {rows:,} rows")
            out[target] = rows
            continue

        probe = read(spark, conn, select_sql(kind, source, columns, where="1 = 0"))
        if watermark not in probe.columns:
            raise ValueError(f"{source}: no column {watermark}")
        integer = isinstance(probe.schema[watermark].dataType, (T.ByteType, T.ShortType, T.IntegerType, T.LongType,
                                                                 T.DecimalType))
        last = _high_water(spark, ref, watermark, integer)
        if last is not None:
            where, note = f"{quote(kind, watermark)} > {literal(kind, last)}", f"{watermark} > {last}"
        elif not integer and (start := _start(start_date, ctx)):
            where, note = f"{quote(kind, watermark)} >= {literal(kind, start)}", f"{watermark} >= {start} (first run)"
        else:
            where, note = None, "every row (first run)"
        df = read(spark, conn, select_sql(kind, source, columns, where))
        df = df.withColumn("_source_table", tag).withColumn("_ingested_at", F.current_timestamp())
        df = df.cache()
        try:
            rows = df.count()
            merged = None
            if rows and keys:
                merged = _merge(df, ref, keys, watermark)
            elif rows:
                ensure_table(spark, ref, df.schema)
                df.writeTo(ref.name).option("mergeSchema", "true").append()
        finally:
            df.unpersist()
        detail = f" ({merged['updated']:,} overwritten, {merged['inserted']:,} added)" if merged else ""
        log(f"{source} -> {ref.name}: {note}: {rows:,} new rows{detail}")
        out[target] = rows
    result = {"tables": out, "rows": sum(out.values()), "connection": connection, "schema": SCHEMA}
    if in_range:
        result.update(from_date=from_date, to_date=to_date, skipped=skipped)
    return result
