"""Minimal reader for Avro object container files (.avro), in pure Python.

The Prooflix server ships pyarrow but no Avro library, so the bronze job decodes
Avro itself. Supports every Avro type and the `null` and `deflate` codecs.
"""

from __future__ import annotations

import io
import json
import struct
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO

MAGIC = b"Obj\x01"
EPOCH_DATE = date(1970, 1, 1)
EPOCH_TS = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _read_long(f: BinaryIO) -> int:
    shift = result = 0
    while True:
        b = f.read(1)
        if not b:
            raise EOFError("unexpected end of Avro data")
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return (result >> 1) ^ -(result & 1)  # zig-zag decode
        shift += 7


def _read_bytes(f: BinaryIO) -> bytes:
    return f.read(_read_long(f))


class _Decoder:
    def __init__(self, schema: Any) -> None:
        self.named: dict[str, Any] = {}
        self.schema = schema
        self._register(schema, None)

    def _register(self, s: Any, ns: str | None) -> None:
        if isinstance(s, list):
            for b in s:
                self._register(b, ns)
        elif isinstance(s, dict):
            t = s.get("type")
            if t in ("record", "error", "enum", "fixed"):
                name = s["name"]
                ns = s.get("namespace", ns)
                full = name if "." in name or not ns else f"{ns}.{name}"
                self.named[full] = s
                self.named[name] = s
            if t in ("record", "error"):
                for field in s["fields"]:
                    self._register(field["type"], ns)
            elif t == "array":
                self._register(s["items"], ns)
            elif t == "map":
                self._register(s["values"], ns)

    def read(self, s: Any, f: BinaryIO) -> Any:
        if isinstance(s, list):  # union: index, then the branch
            return self.read(s[_read_long(f)], f)
        if isinstance(s, str):
            if s in self.named:
                return self.read(self.named[s], f)
            s = {"type": s}
        t = s["type"]
        if isinstance(t, (dict, list)):
            return self.read(t, f)
        if t in self.named and t not in _PRIMITIVES:
            return self.read(self.named[t], f)
        value = self._read_plain(s, t, f)
        return _apply_logical(s.get("logicalType"), value)

    def _read_plain(self, s: dict, t: str, f: BinaryIO) -> Any:
        if t == "null":
            return None
        if t == "boolean":
            return f.read(1) == b"\x01"
        if t in ("int", "long"):
            return _read_long(f)
        if t == "float":
            return struct.unpack("<f", f.read(4))[0]
        if t == "double":
            return struct.unpack("<d", f.read(8))[0]
        if t == "bytes":
            return _read_bytes(f)
        if t == "string":
            return _read_bytes(f).decode("utf-8")
        if t == "fixed":
            return f.read(s["size"])
        if t == "enum":
            return s["symbols"][_read_long(f)]
        if t in ("record", "error"):
            return {fld["name"]: self.read(fld["type"], f) for fld in s["fields"]}
        if t == "array":
            out: list = []
            while (n := _read_long(f)) != 0:
                if n < 0:
                    n = -n
                    _read_long(f)  # block size in bytes, not needed
                out.extend(self.read(s["items"], f) for _ in range(n))
            return out
        if t == "map":
            m: dict = {}
            while (n := _read_long(f)) != 0:
                if n < 0:
                    n = -n
                    _read_long(f)
                for _ in range(n):
                    k = _read_bytes(f).decode("utf-8")
                    m[k] = self.read(s["values"], f)
            return m
        raise ValueError(f"unsupported Avro type {t!r}")


_PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}


def _apply_logical(logical: str | None, v: Any) -> Any:
    if v is None or logical is None:
        return v
    if logical == "date":
        return EPOCH_DATE + timedelta(days=v)
    if logical == "timestamp-millis":
        return EPOCH_TS + timedelta(milliseconds=v)
    if logical == "timestamp-micros":
        return EPOCH_TS + timedelta(microseconds=v)
    if logical == "local-timestamp-millis":
        return (EPOCH_TS + timedelta(milliseconds=v)).replace(tzinfo=None)
    if logical == "local-timestamp-micros":
        return (EPOCH_TS + timedelta(microseconds=v)).replace(tzinfo=None)
    return v  # decimal, uuid, time-*: keep the underlying value


def read_avro(path: Path) -> tuple[dict, list[dict]]:
    """Return (schema, records) for an Avro object container file."""
    with path.open("rb") as f:
        if f.read(4) != MAGIC:
            raise ValueError(f"{path} is not an Avro object container file")
        meta: dict[str, bytes] = {}
        while (n := _read_long(f)) != 0:
            if n < 0:
                n = -n
                _read_long(f)
            for _ in range(n):
                k = _read_bytes(f).decode("utf-8")
                meta[k] = _read_bytes(f)
        sync = f.read(16)
        schema = json.loads(meta["avro.schema"])
        codec = meta.get("avro.codec", b"null").decode()
        if codec not in ("null", "deflate"):
            raise ValueError(f"{path}: Avro codec {codec!r} is not supported (use null or deflate)")
        decoder = _Decoder(schema)
        records: list[dict] = []
        while True:
            head = f.read(1)
            if not head:
                break
            f.seek(-1, io.SEEK_CUR)
            count = _read_long(f)
            block = _read_bytes(f)
            if codec == "deflate":
                block = zlib.decompress(block, -15)
            buf = io.BytesIO(block)
            records.extend(decoder.read(schema, buf) for _ in range(count))
            if f.read(16) != sync:
                raise ValueError(f"{path}: corrupt Avro block (sync marker mismatch)")
        return schema, records


# ------------------------------------------------------------------ large files, block by block
# An Avro file is a header followed by independent blocks (record count, byte size,
# data, sync marker). Large files are read one block at a time, so memory holds one
# block, and blocks can be decoded in parallel. Flat records (fields of primitive,
# date/timestamp or nullable types; the usual case for source tables) are decoded
# straight into one list per field, which is much faster and smaller than dicts.

_DOUBLE = struct.Struct("<d").unpack_from
_FLOAT = struct.Struct("<f").unpack_from


def read_header(path: Path) -> tuple[dict, str, bytes, int]:
    """(schema, codec, sync marker, offset of the first block)."""
    with path.open("rb") as f:
        if f.read(4) != MAGIC:
            raise ValueError(f"{path} is not an Avro object container file")
        meta: dict[str, bytes] = {}
        while (n := _read_long(f)) != 0:
            if n < 0:
                n = -n
                _read_long(f)
            for _ in range(n):
                k = _read_bytes(f).decode("utf-8")
                meta[k] = _read_bytes(f)
        sync = f.read(16)
        codec = meta.get("avro.codec", b"null").decode()
        if codec not in ("null", "deflate"):
            raise ValueError(f"{path}: Avro codec {codec!r} is not supported (use null or deflate)")
        return json.loads(meta["avro.schema"]), codec, sync, f.tell()


def block_offsets(path: Path) -> list[tuple[int, int]]:
    """(file offset, record count) of every block, found without decoding any records."""
    _, _, sync, offset = read_header(path)
    blocks = []
    with path.open("rb") as f:
        f.seek(offset)
        while True:
            start = f.tell()
            if not f.read(1):
                break
            f.seek(start)
            count = _read_long(f)
            size = _read_long(f)
            f.seek(size, io.SEEK_CUR)
            if f.read(16) != sync:
                raise ValueError(f"{path}: corrupt Avro block at byte {start} (sync marker mismatch)")
            blocks.append((start, count))
    return blocks


def flat_fields(schema: Any) -> list[tuple[str, str, str | None, int]] | None:
    """For a flat record: (name, base type, logical type, index of the null branch or -1)
    per field. None when the schema needs the general decoder."""
    if not isinstance(schema, dict) or schema.get("type") not in ("record", "error"):
        return None
    out = []
    for field in schema["fields"]:
        t, null_index = field["type"], -1
        if isinstance(t, list):
            branches = [b for b in t if b != "null"]
            if len(branches) != 1 or len(t) != 2:
                return None
            null_index = t.index("null")
            t = branches[0]
        logical = None
        if isinstance(t, dict):
            logical = t.get("logicalType")
            t = t.get("type")
        if t not in ("boolean", "int", "long", "float", "double", "bytes", "string"):
            return None
        out.append((field["name"], t, logical, null_index))
    return out


def _field_reader(base: str, null_index: int):
    """A function (buf, pos) -> (value, pos) for one field. Strings and bytes stay raw
    bytes; dates and timestamps stay integers. Arrow converts them in bulk later."""

    def long_(buf, pos):
        b = buf[pos]
        pos += 1
        n = b & 0x7F
        shift = 7
        while b & 0x80:
            b = buf[pos]
            pos += 1
            n |= (b & 0x7F) << shift
            shift += 7
        return (n >> 1) ^ -(n & 1), pos

    if base in ("int", "long"):
        plain = long_
    elif base == "double":
        plain = lambda buf, pos: (_DOUBLE(buf, pos)[0], pos + 8)  # noqa: E731
    elif base == "float":
        plain = lambda buf, pos: (_FLOAT(buf, pos)[0], pos + 4)  # noqa: E731
    elif base == "boolean":
        plain = lambda buf, pos: (buf[pos] == 1, pos + 1)  # noqa: E731
    else:  # string, bytes

        def plain(buf, pos):
            n, pos = long_(buf, pos)
            return bytes(buf[pos : pos + n]), pos + n

    if null_index < 0:
        return plain

    def nullable(buf, pos):
        branch, pos = long_(buf, pos)
        return (None, pos) if branch == null_index else plain(buf, pos)

    return nullable


def decode_block_columns(path: Path, offset: int, fields: list, codec: str) -> tuple[int, list[list]]:
    """Decode the block at offset into one list of raw values per field."""
    with path.open("rb") as f:
        f.seek(offset)
        count = _read_long(f)
        data = _read_bytes(f)
    if codec == "deflate":
        data = zlib.decompress(data, -15)
    buf = memoryview(data)
    readers = [_field_reader(base, null_index) for _, base, _, null_index in fields]
    columns: list[list] = [[] for _ in fields]
    plan = list(zip(readers, [c.append for c in columns]))
    pos = 0
    for _ in range(count):
        for read, append in plan:
            value, pos = read(buf, pos)
            append(value)
    return count, columns
