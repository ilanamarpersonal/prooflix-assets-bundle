"""Generate large synthetic banking source files (customers, accounts, transactions).

Writes Avro object container files with the same schemas as the bundled samples:

    <out>/customers/customers.avro
    <out>/accounts/accounts.avro
    <out>/transactions/transactions.avro

Sizes are given in GB on disk (uncompressed Avro, codec "null"); the row counts are
worked out from a sample so each file lands close to its target. The data is
consistent: every account belongs to a customer, every transaction to an account,
transactions of an account are in time order after it was opened, balance_after is
the running balance, and accounts.balance = opening_balance + sum of its amounts.

Work is split across processes. Each writes a chunk file with the same header and
sync marker, and the chunks are joined into one valid Avro file per table.

Needs fastavro and numpy, which the Prooflix server doesn't ship, so run it with uv:

    uv run --no-project --with fastavro --with numpy \\
        python scripts/generate_banking_data.py --out data/tenant1/source \\
        --customers-gb 1 --accounts-gb 1 --transactions-gb 5
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
from datetime import date, datetime, timezone
from pathlib import Path

import fastavro
import numpy as np

GB = 1024**3
SYNC = b"prooflix-bankgen"  # 16 bytes, shared by every chunk so they can be joined
assert len(SYNC) == 16

DATE = {"type": "int", "logicalType": "date"}
TS = {"type": "long", "logicalType": "timestamp-millis"}
SCHEMAS = {
    "customers": {
        "type": "record",
        "name": "customers",
        "namespace": "prooflix.banking",
        "fields": [
            {"name": "customer_id", "type": "long"},
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "phone", "type": "string"},
            {"name": "country", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "date_of_birth", "type": DATE},
            {"name": "joined_date", "type": DATE},
            {"name": "segment", "type": "string"},
        ],
    },
    "accounts": {
        "type": "record",
        "name": "accounts",
        "namespace": "prooflix.banking",
        "fields": [
            {"name": "account_id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "account_type", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "opened_date", "type": DATE},
            {"name": "status", "type": "string"},
            {"name": "opening_balance", "type": "double"},
            {"name": "balance", "type": "double"},
        ],
    },
    "transactions": {
        "type": "record",
        "name": "transactions",
        "namespace": "prooflix.banking",
        "fields": [
            {"name": "transaction_id", "type": "long"},
            {"name": "account_id", "type": "long"},
            {"name": "transaction_ts", "type": TS},
            {"name": "type", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "description", "type": "string"},
            {"name": "balance_after", "type": "double"},
        ],
    },
}
PARSED = {name: fastavro.parse_schema(s) for name, s in SCHEMAS.items()}

FIRST = "Adam Amit Ariel Avery Ben Chloe Clara Daniel Eitan Ella Emma Ethan Hana Iris Itai Jonah Lea Leo Liam Lucas Maya Mia Nina Omar Ravi Sofia Tamar Yael Zoe".split()
LAST = "Bauer Carter Costa Dahan Fischer Haddad Ibrahim Kaplan Kowalski Levi Lindqvist Mizrahi Moreau Nguyen Novak Okafor Peretz Romano Rossi Schwartz Shapiro Silva Tanaka Weber".split()
PLACES = [("IL", "Tel Aviv"), ("IL", "Jerusalem"), ("IL", "Haifa"), ("US", "New York"), ("US", "Austin"),
          ("GB", "London"), ("FR", "Paris"), ("DE", "Berlin")]
PLACE_P = [0.2, 0.12, 0.1, 0.14, 0.1, 0.12, 0.11, 0.11]
ACCOUNT_KINDS = [("checking", "ILS", 0.30), ("savings", "ILS", 0.19), ("checking", "USD", 0.15), ("checking", "EUR", 0.14),
                 ("savings", "USD", 0.08), ("savings", "EUR", 0.05), ("checking", "GBP", 0.05), ("savings", "GBP", 0.04)]
# (type, description, probability, min amount, max amount)
TXN_KINDS = [
    ("deposit", "Deposit", 0.19, 20.0, 1500.0),
    ("transfer_in", "Transfer from checking", 0.106, 100.0, 2000.0),
    ("salary", "Salary", 0.094, 2500.0, 7000.0),
    ("withdrawal", "ATM withdrawal", 0.074, -800.0, -20.0),
    ("withdrawal", "Withdrawal", 0.073, -800.0, -20.0),
    ("transfer_out", "Transfer to savings", 0.063, -1500.0, -50.0),
    *[("card_payment", d, 0.0353, -250.0, -3.0) for d in
      ["City Transit", "Grocery Market", "Fuel Station", "Electronics Store", "Pharmacy", "Streaming Service",
       "Coffee House", "Restaurant", "Gym", "Online Books"]],
    ("interest", "Monthly interest", 0.02, 1.0, 40.0),
    ("fee", "Account fee", 0.027, -15.0, -1.0),
]
_p = np.array([k[2] for k in TXN_KINDS])
TXN_P = _p / _p.sum()

EPOCH = date(1970, 1, 1)
DAY = lambda d: (d - EPOCH).days  # noqa: E731
FIRST_CUSTOMER_ID = 1001
FIRST_ACCOUNT_ID = 5001
JOIN_FROM, JOIN_TO = DAY(date(2023, 1, 1)), DAY(date(2025, 6, 1))
DOB_FROM, DOB_TO = DAY(date(1950, 1, 1)), DAY(date(2005, 12, 31))
TXN_UNTIL_MS = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)


# ------------------------------------------------------------------ records
def customers(start: int, n: int, seed: int) -> list[dict]:
    rng = np.random.default_rng(seed)
    ids = np.arange(start, start + n)
    fi = rng.integers(0, len(FIRST), n)
    li = rng.integers(0, len(LAST), n)
    pi = rng.choice(len(PLACES), n, p=PLACE_P)
    phone = rng.integers(0, 10_000_000, n)
    dob = rng.integers(DOB_FROM, DOB_TO, n)
    joined = rng.integers(JOIN_FROM, JOIN_TO, n)
    premium = rng.random(n) < 0.27
    out = []
    for cid, f, l, p, ph, d, j, pr in zip(ids.tolist(), fi.tolist(), li.tolist(), pi.tolist(), phone.tolist(),
                                          dob.tolist(), joined.tolist(), premium.tolist()):
        first, last = FIRST[f], LAST[l]
        out.append({
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
            "phone": f"+000-{ph // 10000:03d}-{ph % 10000:04d}",
            "country": PLACES[p][0],
            "city": PLACES[p][1],
            "date_of_birth": d,
            "joined_date": j,
            "segment": "premium" if pr else "retail",
        })
    return out


def accounts_and_transactions(
    first_index: int, counts: np.ndarray, first_txn_id: int, n_customers: int, seed: int
) -> tuple[list[dict], list[dict]]:
    """Accounts first_index.. (one per entry of counts) and their counts[i] transactions each."""
    rng = np.random.default_rng(seed)
    n = len(counts)
    account_ids = np.arange(FIRST_ACCOUNT_ID + first_index, FIRST_ACCOUNT_ID + first_index + n)
    customer_ids = FIRST_CUSTOMER_ID + rng.integers(0, n_customers, n)
    kind = rng.choice(len(ACCOUNT_KINDS), n, p=[k[2] for k in ACCOUNT_KINDS])
    opened = rng.integers(JOIN_FROM, DAY(date(2025, 12, 1)), n)
    opening = np.round(rng.uniform(100, 20000, n), 2)

    total = int(counts.sum())
    owner = np.repeat(np.arange(n), counts)  # account index of each transaction
    tk = rng.choice(len(TXN_KINDS), total, p=TXN_P)
    lo = np.array([k[3] for k in TXN_KINDS])[tk]
    hi = np.array([k[4] for k in TXN_KINDS])[tk]
    amount = np.round(lo + (hi - lo) * rng.random(total), 2)

    # times: start a little after the account opened, then random gaps, in order per account
    starts = np.cumsum(counts) - counts  # index of each account's first transaction (== total if it has none)

    def per_account_cumsum(x: np.ndarray) -> np.ndarray:
        """Running sum of x that restarts at each account's first transaction."""
        cs = np.concatenate([[0.0], np.cumsum(x)])  # cs[i] = sum of x[:i]; valid for i == total too
        return cs[1:] - np.repeat(cs[starts], counts)

    # times: in order per account, spread from just after the account opened to the end
    # of the window. Random gaps are scaled so each account's history fills its time.
    opened_ms = opened.astype(np.int64) * 86_400_000
    room = (TXN_UNTIL_MS - opened_ms - 7_200_000).astype(float)  # ms from opening to the window end
    within = per_account_cumsum(rng.exponential(1.0, total))
    has_txns = counts > 0
    span = np.ones(n)
    # the sum of count+1 random gaps, so the last transaction lands before the window ends
    span[has_txns] = within[starts[has_txns] + counts[has_txns] - 1] + rng.exponential(1.0, int(has_txns.sum()))
    ts = (opened_ms[owner] + 3_600_000 + within * (room / span)[owner]).astype(np.int64)

    running = per_account_cumsum(amount)
    balance_after = np.round(opening[owner] + running, 2)
    final = opening.copy()
    has = counts > 0
    final[has] = balance_after[starts[has] + counts[has] - 1]

    accounts = [
        {
            "account_id": a,
            "customer_id": c,
            "account_type": ACCOUNT_KINDS[k][0],
            "currency": ACCOUNT_KINDS[k][1],
            "opened_date": o,
            "status": "active",
            "opening_balance": ob,
            "balance": b,
        }
        for a, c, k, o, ob, b in zip(account_ids.tolist(), customer_ids.tolist(), kind.tolist(), opened.tolist(),
                                      opening.tolist(), np.round(final, 2).tolist())
    ]
    txn_ids = np.arange(first_txn_id, first_txn_id + total)
    transactions = [
        {
            "transaction_id": t,
            "account_id": a,
            "transaction_ts": s,
            "type": TXN_KINDS[k][0],
            "amount": m,
            "description": TXN_KINDS[k][1],
            "balance_after": b,
        }
        for t, a, s, k, m, b in zip(txn_ids.tolist(), account_ids[owner].tolist(), ts.tolist(), tk.tolist(),
                                     amount.tolist(), balance_after.tolist())
    ]
    return accounts, transactions


# ------------------------------------------------------------------ writing
def write_avro(path: Path, table: str, records: list[dict]) -> None:
    with path.open("wb") as f:
        fastavro.writer(f, PARSED[table], records, codec="null", sync_marker=SYNC, sync_interval=64 * 1024 * 1024)


def header_size(table: str) -> int:
    buf = io.BytesIO()
    fastavro.writer(buf, PARSED[table], [], codec="null", sync_marker=SYNC)
    return len(buf.getvalue())


def join_chunks(table: str, chunks: list[Path], target: Path) -> None:
    """One Avro file from chunk files that share the header and sync marker."""
    skip = header_size(table)
    tmp = target.with_name(f".{target.name}.tmp")
    with tmp.open("wb") as out:
        for i, chunk in enumerate(chunks):
            with chunk.open("rb") as f:
                if i:
                    f.seek(skip)
                shutil.copyfileobj(f, out, 16 * 1024 * 1024)
            chunk.unlink()
    tmp.replace(target)


def bytes_per_row(table: str, records: list[dict]) -> float:
    buf = io.BytesIO()
    fastavro.writer(buf, PARSED[table], records, codec="null", sync_marker=SYNC)
    return (len(buf.getvalue()) - header_size(table)) / len(records)


# ------------------------------------------------------------------ workers
def customer_chunk(args: tuple) -> str:
    path, start, n, seed = args
    write_avro(Path(path), "customers", customers(start, n, seed))
    return path


def account_chunk(args: tuple) -> tuple[str, str]:
    a_path, t_path, first_index, counts, first_txn_id, n_customers, seed = args
    accounts, transactions = accounts_and_transactions(first_index, counts, first_txn_id, n_customers, seed)
    write_avro(Path(a_path), "accounts", accounts)
    del accounts
    write_avro(Path(t_path), "transactions", transactions)
    return a_path, t_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, required=True, help="source folder, e.g. data/tenant1/source")
    ap.add_argument("--customers-gb", type=float, default=1.0)
    ap.add_argument("--accounts-gb", type=float, default=1.0)
    ap.add_argument("--transactions-gb", type=float, default=5.0)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--seed", type=int, default=20261110)
    ap.add_argument("--dry-run", action="store_true", help="only print the planned row counts")
    args = ap.parse_args()

    # Row counts from the size of a sample of each table.
    sample_counts = np.full(20_000, 5)
    sa, st = accounts_and_transactions(0, sample_counts, 1, 1000, args.seed)
    bpr = {
        "customers": bytes_per_row("customers", customers(FIRST_CUSTOMER_ID, 50_000, args.seed)),
        "accounts": bytes_per_row("accounts", sa),
        "transactions": bytes_per_row("transactions", st),
    }
    n_customers = int(args.customers_gb * GB / bpr["customers"])
    n_accounts = int(args.accounts_gb * GB / bpr["accounts"])
    n_txns = int(args.transactions_gb * GB / bpr["transactions"])
    print(f"bytes/row: { {k: round(v, 1) for k, v in bpr.items()} }")
    print(f"rows: customers {n_customers:,}  accounts {n_accounts:,}  transactions {n_txns:,}  ({args.workers} workers)")
    if args.dry_run:
        return

    rng = np.random.default_rng(args.seed)
    counts = rng.poisson(n_txns / n_accounts, n_accounts).astype(np.int64)
    counts[-1] += max(0, n_txns - int(counts.sum()))  # hit the target exactly if Poisson fell short
    first_txn = np.concatenate([[1], 1 + np.cumsum(counts)[:-1]])

    out = args.out.resolve()
    for table in SCHEMAS:
        (out / table).mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".bankgen-", dir=out))
    started = time.time()
    try:
        with ProcessPoolExecutor(args.workers) as pool:
            per = 1_000_000
            c_jobs = [(str(work / f"c{i:05d}.avro"), FIRST_CUSTOMER_ID + s, min(per, n_customers - s), args.seed + 1 + i)
                      for i, s in enumerate(range(0, n_customers, per))]
            a_per = 400_000
            a_jobs = [(str(work / f"a{i:05d}.avro"), str(work / f"t{i:05d}.avro"), s, counts[s:s + a_per],
                       int(first_txn[s]), n_customers, args.seed + 100_000 + i)
                      for i, s in enumerate(range(0, n_accounts, a_per))]
            c_done = pool.map(customer_chunk, c_jobs)
            a_done = pool.map(account_chunk, a_jobs)
            c_files = [Path(p) for p in c_done]
            print(f"customers chunks done after {time.time() - started:.0f}s", flush=True)
            pairs = list(a_done)
            print(f"accounts/transactions chunks done after {time.time() - started:.0f}s", flush=True)
        join_chunks("customers", c_files, out / "customers" / "customers.avro")
        join_chunks("accounts", [Path(a) for a, _ in pairs], out / "accounts" / "accounts.avro")
        join_chunks("transactions", [Path(t) for _, t in pairs], out / "transactions" / "transactions.avro")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    for table in SCHEMAS:
        p = out / table / f"{table}.avro"
        print(f"{p}: {p.stat().st_size / GB:.2f} GB")
    print(f"done in {time.time() - started:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
