"""The tenant catalog in Unity Catalog, resolved through the shared Spark session.

    from runtime.catalog import tenant_catalog
    cat = tenant_catalog(ctx)                 # tenant1 (PROOFLIX_UC_CATALOG)
    ref = cat.table("bronze", "accounts")     # tenant1.bronze.accounts
    ref.name, ref.location                    # full name, folder of its Delta files

Resolving makes sure the catalog exists in Unity Catalog (created through its REST
API the first time), that it is registered in the Spark session, and creates the
schemas a task asks for. Tables are external Delta tables whose files live under
<lakehouse>/<schema>/<table> (see infra/config.py).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from runtime import spark_session

API = "/api/2.1/unity-catalog"


@dataclass(frozen=True)
class TableRef:
    catalog: str
    schema: str
    table: str
    location: Path

    @property
    def name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def uri(self) -> str:
        return self.location.as_uri()

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class TenantCatalog:
    name: str
    lakehouse: Path

    def table(self, schema: str, table: str) -> TableRef:
        return TableRef(self.name, schema, table, self.lakehouse / schema / table)


def _uc(method: str, path: str, body: dict | None, settings) -> dict:
    req = urllib.request.Request(
        settings.uc_uri + API + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json",
                 **({"Authorization": f"Bearer {settings.uc_token}"} if settings.uc_token else {})},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            body = res.read()
            try:
                return json.loads(body or b"{}")
            except ValueError:
                return {}  # e.g. DELETE answers with a non-JSON body
    except urllib.error.HTTPError as e:
        if e.code == 404 and method == "GET":
            return {}
        raise RuntimeError(f"Unity Catalog {method} {path}: {e.code} {e.read().decode(errors='replace')}") from None
    except urllib.error.URLError as e:
        raise RuntimeError(f"Unity Catalog is not reachable at {settings.uc_uri}: {e.reason}") from None


def drop_table_entry(ref: TableRef, ctx=None) -> bool:
    """Remove a table from Unity Catalog without loading it (its files may be gone).
    External table: only the catalog entry goes. Returns True when one was removed."""
    s = spark_session.settings(ctx)
    if not _uc("GET", f"/tables/{ref.name}", None, s):
        return False
    _uc("DELETE", f"/tables/{ref.name}", None, s)
    return True


def tenant_catalog(ctx=None, schemas: tuple[str, ...] = (), catalog: str | None = None) -> TenantCatalog:
    """The tenant catalog, ready to use: created in Unity Catalog and Spark if needed, with `schemas`."""
    spark = spark_session.get_spark(ctx)
    s = spark_session.settings(ctx)
    name = catalog or s.catalog
    if not _uc("GET", f"/catalogs/{name}", None, s):
        _uc("POST", "/catalogs", {"name": name, "comment": "Prooflix tenant catalog"}, s)
    for key, value in spark_session.uc_catalog_conf(name, s).items():
        spark.conf.set(key, value)  # catalogs load lazily, so one can be added at any time
    for schema in schemas:
        if not _uc("GET", f"/schemas/{name}.{schema}", None, s):
            _uc("POST", "/schemas", {"name": schema, "catalog_name": name}, s)
    lakehouse = s.lakehouse if name == s.catalog else s.lakehouse.parent.parent / name / "lakehouse"
    lakehouse.mkdir(parents=True, exist_ok=True)
    return TenantCatalog(name, lakehouse)
