"""Reading relational databases with Spark over JDBC (Oracle, SQL Server, MySQL, PostgreSQL).

A connection comes from Prooflix (`ctx.connection(name)`, defined in the Ingestion
tab) with its kind, JDBC URL, driver class, user and password. The JDBC drivers
are in the Spark packages (infra/config.py).

    conn = ctx.connection("bankdb")
    sql = select_sql(conn["kind"], "core.accounts", ["account_id", "updated_at"],
                     where=f"{quote(kind, 'updated_at')} > {literal(kind, '2024-05-01 00:00:00')}")
    df = read(spark, conn, sql)

Each database quotes names and writes timestamps its own way; the helpers here
build SQL each one accepts.
"""

from __future__ import annotations

import re

_QUOTES = {"postgresql": ('"', '"'), "oracle": ('"', '"'), "mysql": ("`", "`"), "sqlserver": ("[", "]")}
FETCH_SIZE = 10_000  # rows per round trip


def quote(kind: str, name: str) -> str:
    """A quoted identifier (keeps the name's case exactly as the database shows it)."""
    left, right = _QUOTES[kind]
    return left + name.replace(right, right * 2) + right


def table_name(kind: str, source: str) -> str:
    """"schema.table" as a quoted, qualified name."""
    schema, _, table = source.partition(".")
    return f"{quote(kind, schema)}.{quote(kind, table)}" if table else quote(kind, schema)


def literal(kind: str, value: str | int) -> str:
    """A watermark value in SQL: an integer, or a "YYYY-MM-DD HH:MM:SS[.ffffff]" time."""
    if isinstance(value, int):
        return str(value)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d{1,9})?)?", value):
        raise ValueError(f"not a time: {value!r}")
    if " " not in value:
        value += " 00:00:00"
    if kind == "sqlserver":
        return f"CAST('{value}' AS DATETIME2)"
    if kind == "mysql":
        return f"TIMESTAMP('{value}')"
    return f"TIMESTAMP '{value}'"  # ANSI: PostgreSQL, Oracle


def select_sql(kind: str, source: str, columns: list[str], where: str | None = None) -> str:
    cols = ", ".join(quote(kind, c) for c in columns) if columns else "*"
    return f"SELECT {cols} FROM {table_name(kind, source)}" + (f" WHERE {where}" if where else "")


# driver settings that must stay off whatever the connection says: a database could
# otherwise ask for a local file of the server (LOAD DATA LOCAL) or send Java objects
SAFE_OPTIONS = {
    "mysql": {"allowLoadLocalInfile": "false", "allowUrlInLocalInfile": "false", "autoDeserialize": "false"},
}


def read(spark, conn: dict, sql: str, fetch_size: int = FETCH_SIZE):
    """The rows of a SELECT, as a DataFrame (read when an action runs)."""
    reader = (
        spark.read.format("jdbc")
        .option("url", conn["jdbc_url"])
        .option("driver", conn["driver"])
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("query", sql)
        .option("fetchsize", fetch_size)
    )
    for key, value in SAFE_OPTIONS.get(conn["kind"], {}).items():
        reader = reader.option(key, value)
    return reader.load()
