"""Store every date and timestamp in the banking Avro files as a plain long (epoch millis).

    customers.date_of_birth, customers.joined_date   int (date, days)       -> long, ms at 00:00 UTC
    accounts.opened_date                             int (date, days)       -> long, ms at 00:00 UTC
    transactions.transaction_ts                      long (timestamp-millis) -> long, same value

No logical types are left, so readers see plain numbers. transaction_ts keeps its
bytes: only the file header (the schema) changes and the data blocks are copied as
they are. The date fields are decoded and written again.

Each file is rewritten next to the original, checked (row count and first/last
values), then swapped in. Files already converted are skipped, so it can be run
again.

    uv run --no-project --with fastavro python scripts/dates_to_long.py --source source
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path

import fastavro

DAY_MS = 86_400_000
EPOCH = date(1970, 1, 1)
SYNC = b"prooflix-bankgen"
DATE_FIELDS = {"customers": ("date_of_birth", "joined_date"), "accounts": ("opened_date",), "transactions": ()}
TS_FIELDS = {"customers": (), "accounts": (), "transactions": ("transaction_ts",)}


def _plain_long_schema(schema: dict, table: str) -> dict:
    fields = []
    for f in schema["fields"]:
        if f["name"] in DATE_FIELDS[table] + TS_FIELDS[table]:
            f = {"name": f["name"], "type": "long"}
        fields.append(f)
    return {**schema, "fields": fields}


def _needs_conversion(schema: dict, table: str) -> bool:
    names = DATE_FIELDS[table] + TS_FIELDS[table]
    return any(isinstance(f["type"], dict) for f in schema["fields"] if f["name"] in names)


def _header(schema: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.writer(buf, fastavro.parse_schema(schema), [], codec="null", sync_marker=SYNC)
    return buf.getvalue()


def convert(job: tuple[str, str]) -> str:
    path_s, table = job
    path = Path(path_s)
    with path.open("rb") as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        if reader._header["sync"] != SYNC or reader.codec != "null":
            return f"{path.name}: skipped (not written by the generator)"
    if not _needs_conversion(schema, table):
        return f"{path.name}: already plain longs"
    new_schema = _plain_long_schema(schema, table)
    tmp = path.with_name(f".{path.name}.long-tmp")
    started = time.time()
    try:
        if DATE_FIELDS[table]:
            dates = DATE_FIELDS[table]

            def records():
                with path.open("rb") as f:
                    for r in fastavro.reader(f):
                        for d in dates:
                            r[d] = (r[d] - EPOCH).days * DAY_MS
                        yield r

            with tmp.open("wb") as out:
                fastavro.writer(out, fastavro.parse_schema(new_schema), records(), codec="null",
                                sync_marker=SYNC, sync_interval=64 * 1024 * 1024)
        else:
            # same values and encoding: new header, the data blocks as they are
            old_len = len(_header(schema))
            with path.open("rb") as f, tmp.open("wb") as out:
                f.seek(old_len - 16)
                if f.read(16) != SYNC:
                    raise RuntimeError("unexpected header length")
                out.write(_header(new_schema))
                while piece := f.read(16 * 1024 * 1024):
                    out.write(piece)

        # check: same rows, first and last records converted
        def ends(p: Path) -> tuple[int, dict, dict]:
            n, first, last = 0, None, None
            with p.open("rb") as f:
                for b in fastavro.block_reader(f):
                    if first is None:
                        first = next(iter(b))
                    n += b.num_records
                    last = b
            return n, first, list(last)[-1]

        n_old, first_old, last_old = ends(path)
        n_new, first_new, last_new = ends(tmp)
        for old, new in ((first_old, first_new), (last_old, last_new)):
            for k, v in old.items():
                if k in DATE_FIELDS[table]:
                    v = (v - EPOCH).days * DAY_MS
                elif k in TS_FIELDS[table]:
                    v = int(v.timestamp() * 1000)
                if new[k] != v:
                    raise RuntimeError(f"{k}: {new[k]!r} != {v!r}")
        if n_new != n_old:
            raise RuntimeError(f"{n_new:,} rows != {n_old:,}")
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return f"{path.name}: {n_new:,} rows converted in {time.time() - started:.0f}s"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    args = ap.parse_args()
    src = args.source.resolve()
    jobs = [(str(p), table) for table in DATE_FIELDS for p in sorted((src / table).glob("*.avro"))]
    # the slow ones (decoded and written again) first
    jobs.sort(key=lambda j: j[1] == "transactions")
    started = time.time()
    with ProcessPoolExecutor(args.workers) as pool:
        for line in pool.map(convert, jobs):
            print(line, flush=True)
    print(f"{len(jobs)} files in {time.time() - started:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
