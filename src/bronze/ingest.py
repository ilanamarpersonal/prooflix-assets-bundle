"""Bronze ingest: copy new source files into the destination tree as Parquet.

Called by the `bronze` pipeline (entrypoint: bronze.ingest:run).

- Reads every data file under `source_dir` that was **created** after the date in
  the checkpoint file. Editing or touching a file does not change its creation
  time, so a file already processed is not picked up again.
- Writes each one to the same relative path under `destination_dir`, with a
  `.parquet` extension, so the destination mirrors the source folder tree.
- Large Avro files (64 MB and up) are converted block by block in several worker
  processes, so memory holds a few blocks instead of the whole file. Each block becomes
  one Parquet part, `<name>-00000.parquet`, `<name>-00001.parquet`, ... in the table's
  folder; the Catalog reads a folder of Parquet files as one table.
- After every file is written, stores the run's start time as the new checkpoint,
  so the next run only picks up files created after this one started.

The checkpoint file is JSON, for example:

    {
      "last_checkpoint": "2026-11-10T17:55:36.850493+00:00",
      "previous_checkpoint": "1970-01-01T00:00:00+00:00",
      "run_id": "0cf08a6bf038",
      "files": 3,
      "rows": 15856
    }

Only `last_checkpoint` is read back; the other fields describe the run that wrote it.
"""

from __future__ import annotations

import csv
import json
import multiprocessing
import os
import shutil
import site
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bronze.avro_reader import block_offsets, decode_block_columns, flat_fields, read_avro, read_header

PARALLEL_MIN_BYTES = 64 * 1024 * 1024  # smaller Avro files are converted in one go
MAX_WORKERS = 8  # each worker holds about one decoded block (~0.5 GB for 1M rows)
SRC_DIR = Path(__file__).resolve().parents[1]  # this project's src/, for the worker processes

SUPPORTED = {".avro", ".csv", ".json", ".parquet"}
DEFAULT_CHECKPOINT = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ------------------------------------------------------------------ checkpoint
CHECKPOINT_KEY = "last_checkpoint"


def _parse_time(text: str) -> datetime:
    value = datetime.fromisoformat(text.strip())
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def read_checkpoint(path: Path) -> datetime:
    """The `last_checkpoint` date in the JSON checkpoint file. A missing or empty
    file means "from the beginning". A file holding just a date (the format
    before JSON) is still accepted; the next run rewrites it as JSON."""
    text = path.read_text().strip() if path.is_file() else ""
    if not text:
        return DEFAULT_CHECKPOINT
    if not text.startswith("{"):
        return _parse_time(text)
    data = json.loads(text)
    if not isinstance(data.get(CHECKPOINT_KEY), str):
        raise ValueError(f"{path}: expected a JSON object with a {CHECKPOINT_KEY!r} date")
    return _parse_time(data[CHECKPOINT_KEY])


def write_checkpoint(path: Path, value: datetime, **details: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {CHECKPOINT_KEY: value.isoformat(timespec="microseconds"), **details}
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(path)  # atomic: a crash never leaves a half-written checkpoint


def created_at(path: Path) -> datetime:
    """When the file was created (st_birthtime on macOS and Windows). Linux file
    systems don't expose it through os.stat, so there the modification time is used."""
    st = path.stat()
    return datetime.fromtimestamp(getattr(st, "st_birthtime", st.st_mtime), timezone.utc)


# ------------------------------------------------------------------ readers
def _arrow_type(avro: object) -> pa.DataType | None:
    """Arrow type for an Avro field type, or None to let Arrow infer it."""
    if isinstance(avro, list):
        branches = [b for b in avro if b != "null"]
        return _arrow_type(branches[0]) if len(branches) == 1 else None
    if isinstance(avro, dict):
        logical = avro.get("logicalType")
        if logical == "date":
            return pa.date32()
        if logical in ("timestamp-millis", "timestamp-micros"):
            return pa.timestamp("ms" if logical.endswith("millis") else "us", tz="UTC")
        if logical in ("local-timestamp-millis", "local-timestamp-micros"):
            return pa.timestamp("ms" if logical.endswith("millis") else "us")
        if logical is None and avro.get("type") in _PRIMITIVE_ARROW:
            return _PRIMITIVE_ARROW[avro["type"]]
        return None
    return _PRIMITIVE_ARROW.get(avro) if isinstance(avro, str) else None


_PRIMITIVE_ARROW = {
    "null": pa.null(),
    "boolean": pa.bool_(),
    "int": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "bytes": pa.binary(),
    "string": pa.string(),
}


def read_avro_table(path: Path) -> pa.Table:
    schema, records = read_avro(path)
    fields = schema.get("fields") if isinstance(schema, dict) else None
    if not fields:
        return pa.Table.from_pylist(records)
    types = {f["name"]: _arrow_type(f["type"]) for f in fields}
    if all(types.values()):
        arrow_schema = pa.schema(
            [
                pa.field(f["name"], types[f["name"]], nullable=isinstance(f["type"], list) and "null" in f["type"])
                for f in fields
            ]
        )
        return pa.Table.from_pylist(records, schema=arrow_schema)
    return pa.Table.from_pylist(records)


# ------------------------------------------------------------------ large Avro, block by block
def _arrow_column(values: list, base: str, logical: str | None) -> pa.Array:
    """Raw decoded values (bytes for strings, ints for dates and timestamps) as an Arrow array."""
    if base in ("string", "bytes"):
        arr = pa.array(values, pa.binary())
        return arr.cast(pa.string()) if base == "string" else arr
    if base in ("int", "long"):
        arr = pa.array(values, pa.int32() if base == "int" else pa.int64())
        if logical == "date":
            return arr.cast(pa.int32()).cast(pa.date32())
        if logical in ("timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros"):
            unit = "ms" if logical.endswith("millis") else "us"
            return arr.cast(pa.int64()).cast(pa.timestamp(unit, tz=None if logical.startswith("local") else "UTC"))
        return arr
    return pa.array(values, {"double": pa.float64(), "float": pa.float32(), "boolean": pa.bool_()}[base])


def block_table(path: Path, offset: int, fields: list, codec: str) -> pa.Table:
    _, columns = decode_block_columns(path, offset, fields, codec)
    arrays = [_arrow_column(col, base, logical) for col, (_, base, logical, _) in zip(columns, fields)]
    return pa.table(arrays, names=[name for name, *_ in fields])


def _convert_block(job: tuple) -> int:
    """Runs in a worker process: one Avro block -> one Parquet file."""
    path, offset, fields, codec, out = job
    table = block_table(Path(path), offset, fields, codec)
    pq.write_table(table, out)
    return table.num_rows


def convert_large_avro(path: Path, out_dir: Path, stem: str, log) -> tuple[int, list[str]]:
    """Convert a large flat Avro file into Parquet parts in out_dir. Returns (rows, part names)."""
    schema, codec, _, _ = read_header(path)
    fields = flat_fields(schema)
    blocks = block_offsets(path)
    total = sum(count for _, count in blocks)
    parts_dir = out_dir / f".{stem}.parts-{os.getpid()}"  # hidden, so the Catalog ignores it meanwhile
    shutil.rmtree(parts_dir, ignore_errors=True)
    parts_dir.mkdir(parents=True)
    jobs = [(str(path), offset, fields, codec, str(parts_dir / f"{stem}-{i:05d}.parquet")) for i, (offset, _) in enumerate(blocks)]
    workers = max(1, min(len(jobs), (os.cpu_count() or 2) - 1, MAX_WORKERS))
    log(f"  {path.name}: {len(blocks)} blocks, {total:,} rows, converting with {workers} worker processes")
    started, done_rows, done_blocks, next_report = time.time(), 0, 0, 0.1
    try:
        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(workers, mp_context=ctx, initializer=site.addsitedir, initargs=(str(SRC_DIR),)) as pool:
            for future in as_completed([pool.submit(_convert_block, job) for job in jobs]):
                done_rows += future.result()
                done_blocks += 1
                if done_rows >= next_report * total or done_blocks == len(jobs):
                    log(f"  {path.name}: {done_rows / max(total, 1):.0%} ({done_rows:,} rows, {time.time() - started:.0f}s)")
                    next_report += 0.1
    except BrokenProcessPool:
        log("  worker processes are not available here; converting in this process instead")
        done_rows = sum(_convert_block(job) for job in jobs)

    # swap the new parts in for any earlier output of this file
    for old in [out_dir / f"{stem}.parquet", *out_dir.glob(f"{stem}-*.parquet")]:
        old.unlink(missing_ok=True)
    names = []
    for part in sorted(parts_dir.glob("*.parquet")):
        part.replace(out_dir / part.name)
        names.append(part.name)
    shutil.rmtree(parts_dir, ignore_errors=True)
    return done_rows, names


def read_table(path: Path) -> pa.Table:
    ext = path.suffix.lower()
    if ext == ".avro":
        return read_avro_table(path)
    if ext == ".parquet":
        return pq.read_table(path)
    if ext == ".csv":
        import pyarrow.csv as pacsv

        return pacsv.read_csv(path)
    if ext == ".json":
        text = path.read_text()
        data = json.loads(text) if text.lstrip().startswith("[") else [json.loads(l) for l in text.splitlines() if l.strip()]
        return pa.Table.from_pylist(data)
    raise ValueError(f"unsupported file type: {path.name}")


# ------------------------------------------------------------------ job
def run(source_dir: str, destination_dir: str, checkpoint_file: str, ctx=None) -> dict:
    root = ctx.workspace if ctx is not None else Path.cwd()
    log = ctx.log if ctx is not None else print
    source = (root / source_dir).resolve()
    destination = (root / destination_dir).resolve()
    checkpoint = (root / checkpoint_file).resolve()
    checkpoint_dir = checkpoint.parent

    started = datetime.now(timezone.utc)  # becomes the next checkpoint
    since = read_checkpoint(checkpoint)
    log(f"checkpoint: {since.isoformat()} (files created after it are new)")

    new_files = sorted(
        p
        for p in source.rglob("*")
        if p.is_file()
        and p.suffix.lower() in SUPPORTED
        and not p.is_relative_to(checkpoint_dir)
        and not any(part.startswith(".") for part in p.relative_to(source).parts)
        and created_at(p) > since
    )
    log(f"{len(new_files)} new file(s) under {source_dir}")

    written = []
    total_rows = 0
    for path in new_files:
        rel = path.relative_to(source)
        out = destination / rel.with_suffix(".parquet")
        out.parent.mkdir(parents=True, exist_ok=True)
        large = (
            path.suffix.lower() == ".avro"
            and path.stat().st_size >= PARALLEL_MIN_BYTES
            and flat_fields(read_header(path)[0]) is not None
        )
        if large:
            rows, parts = convert_large_avro(path, out.parent, out.stem, log)
            target = f"{out.parent.relative_to(destination).as_posix()}/{out.stem}-*.parquet ({len(parts)} parts)"
        else:
            table = read_table(path)
            for old in out.parent.glob(f"{out.stem}-*.parquet"):  # parts from an earlier large version
                old.unlink()
            pq.write_table(table, out)
            rows, target = table.num_rows, out.relative_to(destination).as_posix()
        total_rows += rows
        written.append({"source": rel.as_posix(), "parquet": target, "rows": rows})
        log(f"{rel.as_posix()} -> {destination_dir}/{target} ({rows:,} rows)")

    write_checkpoint(
        checkpoint,
        started,
        previous_checkpoint=since.isoformat(),
        run_id=ctx.run_id if ctx is not None else None,
        files=len(written),
        rows=total_rows,
    )
    log(f"new checkpoint: {started.isoformat()}")
    return {
        "previous_checkpoint": since.isoformat(),
        "checkpoint": started.isoformat(),
        "files": len(written),
        "rows": total_rows,
        "written": written,
    }
