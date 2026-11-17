"""Bronze: lands new source files as Delta tables, with Spark Structured Streaming.

Called by the `bronze` pipeline (entrypoint bronze.ingest:run). Each folder of the
source data is a table: source/accounts/*.avro -> tenant1.bronze.accounts. One
stream per table reads the files it has not seen yet and appends their rows, as
they are, plus:

    _source_file   the file the row came from
    _ingested_at   when this run wrote it

Spark's checkpoint (<checkpoint_dir>/<table>) remembers which files were read, so a
file is ingested once; adding data means adding files. When a table has no
checkpoint yet (the system is being initialized), only files changed after the
system start date (PROOFLIX_START_DATE, System tab) are read; without one, all are.
"""

from __future__ import annotations

from pathlib import Path

from infra.streaming import file_stream, run_once
from infra.transformations import with_ingest_metadata
from runtime.catalog import tenant_catalog
from runtime.paths import workspace_path
from runtime.spark_session import get_spark

SCHEMA = "bronze"


def run(source_dir: str, checkpoint_dir: str, file_format: str = "avro", ctx=None) -> dict:
    root = ctx.workspace if ctx is not None else Path.cwd()
    log = ctx.log if ctx is not None else print
    source = workspace_path(root, source_dir)
    if not source.is_dir():
        raise FileNotFoundError(f"source folder {source_dir} not found ({source})")
    checkpoints = workspace_path(root, checkpoint_dir)
    tables = sorted(p for p in source.iterdir() if p.is_dir() and not p.name.startswith((".", "_")))
    if not tables:
        raise FileNotFoundError(f"no table folders in {source_dir}")

    spark = get_spark(ctx)
    catalog = tenant_catalog(ctx, schemas=(SCHEMA,))
    start = getattr(ctx, "start_date", None)
    out = {}
    for folder in tables:
        ref = catalog.table(SCHEMA, folder.name)
        checkpoint = checkpoints / folder.name
        since = start if not checkpoint.exists() else None
        log(f"{folder.name}: new {file_format} files -> {ref.name}"
            + (f" (files changed after {since.isoformat()})" if since else ""))
        df = with_ingest_metadata(file_stream(spark, folder, file_format, since))
        result = run_once(df, ref, checkpoint, query_name=f"bronze_{folder.name}")
        out[folder.name] = result["rows"]
        log(f"{folder.name}: {result['rows']:,} new rows")
    return {"tables": out, "rows": sum(out.values()), "catalog": catalog.name, "schema": SCHEMA}
