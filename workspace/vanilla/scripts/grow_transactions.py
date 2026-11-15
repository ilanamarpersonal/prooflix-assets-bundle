"""Add transaction files for the accounts that have none, and split accounts.avro into parts.

After extend_banking_data.py, the accounts it added (ids after the original ones)
have no transactions. This script gives them transactions, written as new files of a
fixed size next to transactions.avro, and keeps the data consistent: each new
account's balance becomes its opening balance plus its transactions, so accounts are
rewritten too, as accounts_001.avro ... (the old accounts.avro is removed).
customers.avro is split the same way into customers_001.avro ... (same rows).

The new accounts are generated again with the seeds extend_banking_data.py used, so
every field but the balance is identical (checked before anything is written).

File names use "_" on purpose: bronze removes "<stem>-*.parquet" when it converts a
file again, so "transactions-001" parts would be deleted along with "transactions".

    uv run --no-project --with fastavro --with numpy \\
        python scripts/grow_transactions.py --source source --files 15 --file-gb 1 \\
            --account-files 5 --customer-files 5
"""

from __future__ import annotations

import argparse
import io
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
    GB,
    SYNC,
    accounts_and_transactions,
    bytes_per_row,
    header_size,
    write_avro,
)

ORIGINAL_ACCOUNTS = 24_660_731  # rows generate_banking_data.py wrote; extend_banking_data.py added the rest
EXTEND_SEED = 20261115 + 1_000_000  # extend_banking_data.py's default seed for accounts
CHUNK = 400_000  # accounts per chunk, as in extend_banking_data.py


# ------------------------------------------------------------------ Avro blocks
def _varint(f) -> int:
    shift = result = 0
    while True:
        b = f.read(1)
        if not b:
            raise EOFError
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return (result >> 1) ^ -(result & 1)  # zigzag
        shift += 7


def blocks(path: Path, table: str) -> list[tuple[int, int, int]]:
    """(offset, length, rows) of each data block of a file written with SYNC (codec null)."""
    skip = header_size(table)
    out = []
    with path.open("rb") as f:
        f.seek(skip - 16)
        if f.read(16) != SYNC:
            raise SystemExit(f"{path}: unexpected header (not written by the generator)")
        size = path.stat().st_size
        pos = skip
        while pos < size:
            f.seek(pos)
            rows = _varint(f)
            length = _varint(f)
            data_start = f.tell()
            f.seek(data_start + length)
            if f.read(16) != SYNC:
                raise SystemExit(f"{path}: bad sync marker after block at {pos}")
            end = data_start + length + 16
            out.append((pos, end - pos, rows))
            pos = end
    return out


def write_blocks(target: Path, table: str, segments: list[tuple[Path, int, int]]) -> None:
    """A new Avro file: the table's header, then the given (file, offset, length) blocks."""
    buf = io.BytesIO()
    fastavro.writer(buf, fastavro.parse_schema(_schema(table)), [], codec="null", sync_marker=SYNC)
    with target.open("wb") as out:
        out.write(buf.getvalue())
        for src, offset, length in segments:
            with src.open("rb") as f:
                f.seek(offset)
                remaining = length
                while remaining:
                    piece = f.read(min(remaining, 16 * 1024 * 1024))
                    out.write(piece)
                    remaining -= len(piece)


def _schema(table: str) -> dict:
    from generate_banking_data import SCHEMAS

    return SCHEMAS[table]


def count_rows(path: Path) -> tuple[int, dict, dict]:
    with path.open("rb") as f:
        n, first, last = 0, None, None
        for b in fastavro.block_reader(f):
            if first is None:
                first = next(iter(b))
            n += b.num_records
            last = b
    return n, first, list(last)[-1]


def split_by_size(segs: list[tuple[Path, int, int]], parts: int) -> list[list[tuple[Path, int, int]]]:
    """Consecutive groups of blocks with about the same number of bytes each."""
    total = sum(ln for _, _, ln in segs)
    out, part, acc = [], [], 0
    for seg in segs:
        part.append(seg)
        acc += seg[2]
        if len(out) < parts - 1 and acc >= total * (len(out) + 1) / parts:
            out.append(part)
            part = []
    out.append(part)
    return out


# ------------------------------------------------------------------ workers
def chunk_job(args: tuple) -> tuple[str, str]:
    a_path, t_path, first_index, counts, first_txn_id, n_customers, seed = args
    accounts, transactions = accounts_and_transactions(first_index, counts, first_txn_id, n_customers, seed)
    write_avro(Path(a_path), "accounts", accounts)
    del accounts
    write_avro(Path(t_path), "transactions", transactions)
    return a_path, t_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source", type=Path, required=True)
    ap.add_argument("--files", type=int, default=15, help="new transaction files")
    ap.add_argument("--file-gb", type=float, default=1.0, help="size of each (GiB)")
    ap.add_argument("--account-files", type=int, default=5, help="parts to split accounts into")
    ap.add_argument("--customer-files", type=int, default=5, help="parts to split customers into (0: leave it)")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--seed", type=int, default=20261116, help="for the transaction counts")
    ap.add_argument("--check-only", action="store_true", help="only check the regenerated accounts match")
    args = ap.parse_args()

    src = args.source.resolve()
    accounts_file = src / "accounts" / "accounts.avro"
    txn_dir = src / "transactions"
    n_customers = count_rows(src / "customers" / "customers.avro")[0]
    n_accounts, _, last_account = count_rows(accounts_file)
    n_old_txns, _, last_txn = count_rows(txn_dir / "transactions.avro")
    n_new_accounts = n_accounts - ORIGINAL_ACCOUNTS
    if last_account["account_id"] != FIRST_ACCOUNT_ID + n_accounts - 1 or n_new_accounts <= 0:
        raise SystemExit("accounts.avro doesn't look like extend_banking_data.py's output")
    if last_txn["transaction_id"] != n_old_txns:
        raise SystemExit("transaction ids are not 1..n in transactions.avro")
    print(f"customers {n_customers:,}  accounts {n_accounts:,} ({n_new_accounts:,} without transactions)  "
          f"transactions {n_old_txns:,}", flush=True)

    # 1. the regenerated new accounts must match the ones in the file (all but balance)
    acc_blocks = blocks(accounts_file, "accounts")
    original_rows = 0
    first_new_block = 0
    for i, (_, _, rows) in enumerate(acc_blocks):
        if original_rows == ORIGINAL_ACCOUNTS:
            first_new_block = i
            break
        original_rows += rows
    with accounts_file.open("rb") as f:
        stored = None
        for i, b in enumerate(fastavro.block_reader(f)):
            if i == first_new_block:
                stored = list(b)
                break
    regen, _ = accounts_and_transactions(ORIGINAL_ACCOUNTS, np.full(len(stored), 2), 1, n_customers, EXTEND_SEED + 1)
    from generate_banking_data import EPOCH

    for a, b in zip(stored, regen):
        if not isinstance(a["opened_date"], int):  # files from before dates_to_long.py
            a = {**a, "opened_date": (a["opened_date"] - EPOCH).days * 86_400_000}
        if {**a, "balance": 0} != {**b, "balance": 0} or a["opening_balance"] != a["balance"]:
            raise SystemExit(f"regenerated account differs: {a} vs {b}")
    print(f"check: {len(stored):,} regenerated accounts match (all fields but balance)", flush=True)
    if args.check_only:
        return

    # 2. how many transactions per file, and which accounts each file covers
    probe_counts = np.full(20_000, 3)
    _, probe = accounts_and_transactions(n_accounts // 2, probe_counts, n_old_txns * 2, n_customers, args.seed)
    bpr = bytes_per_row("transactions", probe)
    per_file = int((args.file_gb * GB - header_size("transactions")) / bpr)
    n_chunks = -(-n_new_accounts // CHUNK)
    bounds = [round(f * n_chunks / args.files) for f in range(args.files + 1)]
    rng = np.random.default_rng(args.seed)
    counts = np.zeros(n_new_accounts, dtype=np.int64)
    for f in range(args.files):
        lo, hi = bounds[f] * CHUNK, min(bounds[f + 1] * CHUNK, n_new_accounts)
        c = rng.poisson(per_file / (hi - lo), hi - lo).astype(np.int64)
        diff = per_file - int(c.sum())
        while diff:  # hit the file's row count exactly
            idx = rng.integers(0, hi - lo, abs(diff))
            if diff > 0:
                np.add.at(c, idx, 1)
            else:
                idx = idx[c[idx] > 0]
                np.subtract.at(c, idx, 1)
                c = np.maximum(c, 0)
            diff = per_file - int(c.sum())
        counts[lo:hi] = c
    first_txn = n_old_txns + 1 + np.concatenate([[0], np.cumsum(counts)[:-1]])
    print(f"{bpr:.1f} bytes/transaction: {args.files} files x {per_file:,} transactions "
          f"({args.files * per_file:,} new, ids {n_old_txns + 1:,}..)", flush=True)

    started = time.time()
    work = Path(tempfile.mkdtemp(prefix=".grow-", dir=src))
    try:
        jobs = []
        for i in range(n_chunks):
            s = i * CHUNK
            n = min(CHUNK, n_new_accounts - s)
            jobs.append((str(work / f"a{i:05d}.avro"), str(work / f"t{i:05d}.avro"), ORIGINAL_ACCOUNTS + s,
                         counts[s:s + n], int(first_txn[s]), n_customers, EXTEND_SEED + 1 + i))
        with ProcessPoolExecutor(args.workers) as pool:
            done = list(pool.map(chunk_job, jobs))
        print(f"{n_chunks} chunks generated after {time.time() - started:.0f}s", flush=True)

        # 3. transaction files: the chunks of each file joined
        t_skip = header_size("transactions")
        new_txn_files = []
        for f in range(args.files):
            segs = []
            for i in range(bounds[f], bounds[f + 1]):
                t_path = Path(done[i][1])
                segs += [(t_path, o, ln) for o, ln, _ in blocks(t_path, "transactions")]
            target = txn_dir / f".transactions_{f + 1:03d}.avro.tmp"
            write_blocks(target, "transactions", segs)
            new_txn_files.append(target)
        del t_skip

        # 4. accounts: the original blocks, then the regenerated ones, split by size
        segs = [(accounts_file, o, ln) for o, ln, _ in acc_blocks[:first_new_block]]
        for a_path, _ in done:
            segs += [(Path(a_path), o, ln) for o, ln, _ in blocks(Path(a_path), "accounts")]
        acc_targets = []
        for k, part in enumerate(split_by_size(segs, args.account_files)):
            target = src / "accounts" / f".accounts_{k + 1:03d}.avro.tmp"
            write_blocks(target, "accounts", part)
            acc_targets.append(target)

        # 4b. customers: the same rows, split by size
        customers_file = src / "customers" / "customers.avro"
        cust_targets = []
        if args.customer_files:
            c_segs = [(customers_file, o, ln) for o, ln, _ in blocks(customers_file, "customers")]
            for k, part in enumerate(split_by_size(c_segs, args.customer_files)):
                target = src / "customers" / f".customers_{k + 1:03d}.avro.tmp"
                write_blocks(target, "customers", part)
                cust_targets.append(target)
            crows = [count_rows(p) for p in cust_targets]
            if sum(r[0] for r in crows) != n_customers or crows[-1][2]["customer_id"] != 1000 + n_customers:
                raise SystemExit("customers check failed; nothing replaced")

        # 5. check, then swap in
        rows = [count_rows(p) for p in acc_targets]
        if sum(r[0] for r in rows) != n_accounts or rows[-1][2]["account_id"] != last_account["account_id"]:
            raise SystemExit("accounts check failed; nothing replaced")
        trows = [count_rows(p) for p in new_txn_files]
        if any(r[0] != per_file for r in trows) or trows[-1][2]["transaction_id"] != n_old_txns + args.files * per_file:
            raise SystemExit("transactions check failed; nothing replaced")
        for p in new_txn_files + acc_targets + cust_targets:
            p.replace(p.with_name(p.name[1:].removesuffix(".tmp")))
        accounts_file.unlink()
        if cust_targets:
            customers_file.unlink()
    finally:
        shutil.rmtree(work, ignore_errors=True)
        for p in [*txn_dir.glob(".transactions_*.tmp"), *(src / "accounts").glob(".accounts_*.tmp"),
                  *(src / "customers").glob(".customers_*.tmp")]:
            p.unlink()

    for d in ("customers", "accounts", "transactions"):
        for p in sorted((src / d).glob("*.avro")):
            print(f"{p.name}: {p.stat().st_size / GB:.3f} GiB")
    print(f"done in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
