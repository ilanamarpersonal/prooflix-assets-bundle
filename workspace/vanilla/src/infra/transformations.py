"""Common DataFrame transformations used by the layers."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F, types as T


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    """Add _ingested_at (when this run wrote the row)."""
    return df.withColumn("_ingested_at", F.current_timestamp())


def epoch_ms_to_timestamp(column: Column, data_type: T.DataType) -> Column:
    """A long (ms since 1970-01-01 UTC), date or timestamp column as a timestamp."""
    if isinstance(data_type, (T.LongType, T.IntegerType, T.ShortType)):
        return F.timestamp_millis(column.cast("long"))
    if isinstance(data_type, (T.DateType, T.TimestampNTZType)):
        return column.cast(T.TimestampType())
    if isinstance(data_type, T.TimestampType):
        return column
    raise TypeError(f"can't read {data_type.simpleString()} as a timestamp")


def to_datetimes(df: DataFrame, columns: list[str]) -> DataFrame:
    """The given columns (those present) as timestamps."""
    types = {f.name: f.dataType for f in df.schema.fields}
    return df.withColumns({c: epoch_ms_to_timestamp(F.col(c), types[c]) for c in columns if c in types})


def drop_internal(df: DataFrame) -> DataFrame:
    """Without the _-prefixed bookkeeping columns."""
    return df.drop(*[c for c in df.columns if c.startswith("_")])


def daily_counts(df: DataFrame, ts_column: str, date_name: str, count_name: str) -> DataFrame:
    """Rows per UTC day of a timestamp column, sorted by day."""
    return (
        df.groupBy(F.to_date(F.col(ts_column)).alias(date_name))
        .agg(F.count(F.lit(1)).alias(count_name))
        .orderBy(date_name)
    )
