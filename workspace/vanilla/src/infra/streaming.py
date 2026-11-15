"""Structured Streaming plumbing shared by the incremental layers.

Each incremental step is a stream that runs once over what is new and stops
(trigger availableNow), writing to a Delta table. Spark's checkpoint (one folder
per stream) remembers what was processed, so the next run picks up only new files
or new rows:

    df = file_stream(spark, source_folder, "avro")          # new files in a folder
    df = table_stream(spark, "tenant1.bronze.accounts")     # new rows of a Delta table
    result = run_once(df, ref, checkpoint_folder)           # {"rows": ..., "batches": ...}
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from infra.materialization import ensure_table
from runtime.catalog import TableRef


def file_stream(spark, folder: Path, fmt: str, since: datetime | None = None) -> DataFrame:
    """Files in folder (any depth) as a stream, with _source_file. `since`: only files
    changed after it (the system start date, PROOFLIX_START_DATE)."""
    path = str(folder)
    schema = spark.read.format(fmt).option("recursiveFileLookup", "true").load(path).schema
    df = (
        spark.readStream.format(fmt)
        .schema(schema)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", f"*.{fmt}")
        .load(path)
    )
    if since is not None:
        df = df.where(F.col("_metadata.file_modification_time") > F.lit(since))
    return df.withColumn("_source_file", F.col("_metadata.file_path"))


def table_stream(spark, name: str) -> DataFrame:
    """New rows of a Delta table as a stream."""
    return spark.readStream.table(name)


def run_once(df: DataFrame, ref: TableRef, checkpoint: Path, query_name: str | None = None) -> dict:
    """Write everything new in the stream to the table, then stop. Returns rows and batches."""
    ensure_table(df.sparkSession, ref, df.schema)
    checkpoint.mkdir(parents=True, exist_ok=True)
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
    )
    if query_name:
        writer = writer.queryName(query_name)
    query = writer.toTable(ref.name)  # through Unity Catalog, by name
    query.awaitTermination()
    if query.exception() is not None:
        raise RuntimeError(f"stream to {ref.name} failed: {query.exception()}")
    progress = [p for p in query.recentProgress if p.get("numInputRows") is not None]
    return {"rows": sum(int(p["numInputRows"]) for p in progress),
            "batches": sum(1 for p in progress if p["numInputRows"])}
