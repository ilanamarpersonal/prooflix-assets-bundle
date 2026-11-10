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
