"""Bronze ingest: copy new source files into the destination tree as Parquet.

Called by the `bronze` pipeline (entrypoint: bronze.ingest:run).

- Reads every data file under `source_dir` that was **created** after the date in
  the checkpoint file. Editing or touching a file does not change its creation
  time, so a file already processed is not picked up again.
- Writes each one to the same relative path under `destination_dir`, with a
  `.parquet` extension, so the destination mirrors the source folder tree.
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
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bronze.avro_reader import read_avro

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
        table = read_table(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out)
        total_rows += table.num_rows
        written.append({"source": rel.as_posix(), "parquet": out.relative_to(destination).as_posix(), "rows": table.num_rows})
        log(f"{rel.as_posix()} -> {destination_dir}/{written[-1]['parquet']} ({table.num_rows} rows)")

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
