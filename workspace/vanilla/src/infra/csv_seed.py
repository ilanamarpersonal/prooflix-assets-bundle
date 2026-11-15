"""Loading the CSV dimension seeds into Delta tables.

Each CSV file in the seeds folder (header row, one dimension per file) becomes a
table dim_<file name>: seeds/currencies.csv -> tenant1.silver.dim_currencies. The
tables are replaced on every load, so they always match the files.
"""

from __future__ import annotations

from pathlib import Path

from infra.materialization import overwrite
from runtime.catalog import TenantCatalog


def read_seed(spark, path: Path):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(str(path))


def load_seeds(spark, folder: Path, catalog: TenantCatalog, schema: str, log=print) -> dict[str, int]:
    """Every seeds/*.csv as table <schema>.dim_<name>. Returns rows per table."""
    out = {}
    for path in sorted(folder.glob("*.csv")):
        ref = catalog.table(schema, f"dim_{path.stem}")
        out[ref.name] = overwrite(read_seed(spark, path), ref)
        log(f"seed {path.name} -> {ref.name} ({out[ref.name]} rows)")
    return out
