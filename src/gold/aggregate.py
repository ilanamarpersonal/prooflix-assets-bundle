"""Gold aggregates. Called by the `gold` pipeline.

`transactions_per_date` (entrypoint: gold.aggregate:transactions_per_date) counts the
transactions of each calendar day in the bronze transactions table and writes the
result as the gold table `agg_transactions_per_date`:

    transaction_date   date    the day (UTC) of transaction_ts
    transaction_count  int64   number of transactions that day

The table is rebuilt from all of bronze on every run (not incremental), so it is
always consistent with bronze, however many files bronze has added.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TABLE = "agg_transactions_per_date"


def _read_table_folder(folder: Path, columns: list[str] | None = None) -> pa.Table:
    """All Parquet files of a table folder, read as one table (like the Catalog does).
    Reading only the columns needed keeps memory small for large tables."""
    files = sorted(
        p for p in folder.rglob("*.parquet") if not any(x.startswith((".", "_")) for x in p.relative_to(folder).parts)
    )
    if not files:
        raise FileNotFoundError(f"no Parquet files in {folder}; run the bronze pipeline first")
    return pa.concat_tables([pq.read_table(f, columns=columns) for f in files], promote_options="default")


def transactions_per_date(
    bronze_transactions_dir: str,
    gold_dir: str,
    timestamp_column: str = "transaction_ts",
    ctx=None,
) -> dict:
    root = ctx.workspace if ctx is not None else Path.cwd()
    log = ctx.log if ctx is not None else print
    source = (root / bronze_transactions_dir).resolve()
    out_dir = (root / gold_dir / TABLE).resolve()

    transactions = _read_table_folder(source, columns=[timestamp_column])
    log(f"read {transactions.num_rows} transactions from {bronze_transactions_dir}")

    ts = transactions[timestamp_column]
    if pa.types.is_timestamp(ts.type) and ts.type.tz:
        ts = pc.cast(ts, pa.timestamp(ts.type.unit))  # drop the zone: UTC wall-clock time
    days = pa.table({"transaction_date": pc.cast(ts, pa.date32())})
    agg = (
        days.group_by("transaction_date")
        .aggregate([("transaction_date", "count")])
        .rename_columns(["transaction_date", "transaction_count"])
        .sort_by("transaction_date")
    )
    agg = agg.cast(pa.schema([("transaction_date", pa.date32()), ("transaction_count", pa.int64())]))

    # Replace the table: write the new file next to the old one, then swap, so a
    # reader never sees a half-written table. Older files in the folder are removed
    # because every Parquet file in a table folder belongs to the table.
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{TABLE}.parquet"
    tmp = out_dir / f".{TABLE}.parquet.tmp"
    pq.write_table(agg, tmp)
    for old in out_dir.glob("*.parquet"):
        if old != target:
            old.unlink()
    tmp.replace(target)

    first, last = (agg["transaction_date"][0].as_py(), agg["transaction_date"][-1].as_py()) if agg.num_rows else (None, None)
    log(f"wrote {agg.num_rows} days ({first} to {last}) to {gold_dir}/{TABLE}/")
    return {
        "table": TABLE,
        "days": agg.num_rows,
        "transactions": transactions.num_rows,
        "first_date": first.isoformat() if first else None,
        "last_date": last.isoformat() if last else None,
        "path": f"{gold_dir}/{TABLE}/{target.name}",
    }
