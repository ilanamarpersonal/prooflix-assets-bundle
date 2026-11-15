"""Writing Delta tables in Unity Catalog: create, overwrite, append.

Tables are external Delta tables: Unity Catalog holds the name and schema, the
Delta files live at the TableRef's location (see runtime/catalog.py).

    ensure_table(spark, ref, df.schema)   # create it if missing
    overwrite(df, ref)                    # replace its rows (keeps the table and its history)
    append(df, ref)                       # add rows
"""

from __future__ import annotations

from runtime.catalog import TableRef, drop_table_entry


def has_delta_files(ref: TableRef) -> bool:
    return (ref.location / "_delta_log").is_dir()


def table_exists(spark, ref: TableRef) -> bool:
    """The table is in Unity Catalog and its Delta files are there."""
    return has_delta_files(ref) and spark.catalog.tableExists(ref.name)


def ensure_table(spark, ref: TableRef, schema, comment: str | None = None) -> bool:
    """Create the table (empty, with this schema) unless it exists. Returns True when created.

    A table Unity Catalog lists but whose Delta files are gone (the data lake folder
    was cleared by hand) is removed from the catalog and created again, empty. Spark
    can't be asked about such a table (loading it fails), so the files are checked
    first and the stale entry is removed through Unity Catalog's API.
    """
    if has_delta_files(ref):
        if spark.catalog.tableExists(ref.name):
            return False
    else:
        drop_table_entry(ref)  # if Unity Catalog still lists it
    ref.location.parent.mkdir(parents=True, exist_ok=True)
    columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
    note = f" COMMENT '{comment}'" if comment else ""
    spark.sql(f"CREATE TABLE {ref.name} ({columns}) USING DELTA LOCATION '{ref.uri}'{note}")
    return True


def overwrite(df, ref: TableRef) -> int:
    """Replace the table's rows with df (created first if missing). Returns the row count."""
    from pyspark.sql import functions as F

    spark = df.sparkSession
    ensure_table(spark, ref, df.schema)
    df.writeTo(ref.name).overwrite(F.lit(True))  # through Unity Catalog, by name
    return spark.read.table(ref.name).count()


def append(df, ref: TableRef) -> None:
    """Add df's rows to the table (created first if missing)."""
    ensure_table(df.sparkSession, ref, df.schema)
    df.writeTo(ref.name).append()
