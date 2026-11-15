"""Grow existing customers.avro / accounts.avro files to a target size by adding rows.

Keeps the rows already there and appends new ones generated like
generate_banking_data.py does (same schemas, same sync marker):

* customers: ids continue after the last one.
* accounts: ids continue after the last one, each owned by a random customer
  (new customers included). New accounts have no transactions, so their balance
  equals their opening balance and transactions.avro stays consistent.

Customers are extended first, so new accounts can belong to new customers. Each
file is rebuilt next to the original, checked, then swapped in, so a failure
never leaves a half-written file. The swapped-in file is a new file, so bronze
sees it as new and converts it again.

    uv run --no-project --with fastavro --with numpy \\
        python scripts/extend_banking_data.py --source source --customers-gb 5 --accounts-gb 5
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import fastavro
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_banking_data import (  # noqa: E402
    FIRST_ACCOUNT_ID,
    FIRST_CUSTOMER_ID,
    GB,
    SYNC,
    accounts_and_transactions,
    bytes_per_row,
    customer_chunk,
    customers,
    header_size,
    write_avro,
)

ID = {"customers": "customer_id", "accounts": "account_id"}
FIRST_ID = {"customers": FIRST_CUSTOMER_ID, "accounts": FIRST_ACCOUNT_ID}


def inspect(path: Path, table: str) -> tuple[int, int]:
    """(rows, last id) of an Avro file written by the generator; checks its sync marker."""
    with path.open("rb") as f:
        reader = fastavro.reader(f)
        if reader._header["sync"] != SYNC:
            raise SystemExit(f"{path}: not written by generate_banking_data.py (different sync marker)")
    rows, last = 0, None
    with path.open("rb") as f:
        for block in fastavro.block_reader(f):
            rows += block.num_records
            last = block
    last_id = list(last)[-1][ID[table]] if last is not None else FIRST_ID[table] - 1
    return rows, last_id


def account_chunk_no_txns(args: tuple) -> str:
    path, first_index, n, n_customers, seed = args
    accounts, _ = accounts_and_transactions(first_index, np.zeros(n, dtype=np.int64), 1, n_customers, seed)
    write_avro(Path(path), "accounts", accounts)
    return path


def extend(path: Path, table: str, target_gb: float, n_customers: int, workers: int, seed: int) -> int:
    """Append rows until path is about target_gb. Returns the table's row count afterwards."""
    rows, last_id = inspect(path, table)
    if last_id != FIRST_ID[table] + rows - 1:
        raise SystemExit(f"{path}: ids are not contiguous ({rows:,} rows, last id {last_id}); not extending")
    size = path.stat().st_size
    extra = int(target_gb * GB) - size
    if extra <= 0:
        print(f"{path}: already {size / GB:.2f} GB, nothing to add")
        return rows

    if table == "customers":
        sample = customers(FIRST_CUSTOMER_ID, 50_000, seed)
    else:
        sample, _ = accounts_and_transactions(0, np.zeros(50_000, dtype=np.int64), 1, n_customers, seed)
    n_new = int(extra / bytes_per_row(table, sample))
    print(f"{table}: {rows:,} rows ({size / GB:.2f} GB) + {n_new:,} new rows, ids {last_id + 1:,}..", flush=True)

    started = time.time()
    work = Path(tempfile.mkdtemp(prefix=f".extend-{table}-", dir=path.parent))
    try:
        per = 1_000_000 if table == "customers" else 400_000
        with ProcessPoolExecutor(workers) as pool:
            if table == "customers":
                jobs = [(str(work / f"c{i:05d}.avro"), last_id + 1 + s, min(per, n_new - s), seed + 1 + i)
                        for i, s in enumerate(range(0, n_new, per))]
                chunks = list(pool.map(customer_chunk, jobs))
            else:
                jobs = [(str(work / f"a{i:05d}.avro"), rows + s, min(per, n_new - s), n_customers, seed + 1 + i)
                        for i, s in enumerate(range(0, n_new, per))]
                chunks = list(pool.map(account_chunk_no_txns, jobs))
        print(f"{table}: {len(chunks)} chunks generated after {time.time() - started:.0f}s", flush=True)

        # the original file, then each chunk's blocks (its header skipped)
        tmp = path.with_name(f".{path.name}.extend-tmp")
        skip = header_size(table)
        with tmp.open("wb") as out:
            with path.open("rb") as f:
                shutil.copyfileobj(f, out, 16 * 1024 * 1024)
            for chunk in chunks:
                with open(chunk, "rb") as f:
                    f.seek(skip)
                    shutil.copyfileobj(f, out, 16 * 1024 * 1024)

        new_rows, new_last = inspect(tmp, table)
        if new_rows != rows + n_new or new_last != last_id + n_new:
            tmp.unlink()
            raise SystemExit(f"{table}: check failed ({new_rows:,} rows, last id {new_last}); original kept")
        tmp.replace(path)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print(f"{path}: {path.stat().st_size / GB:.2f} GB, {new_rows:,} rows, done in {time.time() - started:.0f}s",
          flush=True)
    return new_rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source", type=Path, required=True, help="folder holding customers/ and accounts/")
    ap.add_argument("--customers-gb", type=float, help="target size of customers.avro")
    ap.add_argument("--accounts-gb", type=float, help="target size of accounts.avro")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--seed", type=int, default=20261115)
    args = ap.parse_args()

    src = args.source.resolve()
    customers_file = src / "customers" / "customers.avro"
    n_customers = inspect(customers_file, "customers")[0]
    if args.customers_gb:
        n_customers = extend(customers_file, "customers", args.customers_gb, 0, args.workers, args.seed)
    if args.accounts_gb:
        extend(src / "accounts" / "accounts.avro", "accounts", args.accounts_gb, n_customers, args.workers,
               args.seed + 1_000_000)


if __name__ == "__main__":
    main()
