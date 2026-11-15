"""The Spark session every pipeline task uses.

Prooflix orchestrates the pipelines; Spark only executes the work inside a task.
Tasks run in the Prooflix server process, so one session (and one JVM) is created
the first time a task asks for it and shared by every task after that:

    from runtime.spark_session import get_spark
    spark = get_spark(ctx)

The session has Delta Lake and the Unity Catalog connector, with the tenant
catalog registered as a Unity Catalog catalog (see runtime/catalog.py) and set as
the default, so `bronze.accounts` means `tenant1.bronze.accounts`. Settings come
from infra/config.py.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path

from infra import config

_lock = threading.Lock()
_session = None
_settings: config.SparkSettings | None = None


def _workspace(ctx) -> Path:
    return Path(ctx.workspace) if ctx is not None else Path.cwd()


def settings(ctx=None) -> config.SparkSettings:
    """The settings the session was (or will be) created with."""
    return _settings or config.load(_workspace(ctx))


def uc_catalog_conf(name: str, s: config.SparkSettings) -> dict[str, str]:
    """Spark conf that registers Unity Catalog catalog `name`."""
    prefix = f"spark.sql.catalog.{name}"
    return {prefix: "io.unitycatalog.spark.UCSingleCatalog", f"{prefix}.uri": s.uc_uri, f"{prefix}.token": s.uc_token}


def get_spark(ctx=None):
    """The shared SparkSession, created on first use."""
    global _session, _settings
    with _lock:
        if _session is not None and not _session.sparkContext._jsc.sc().isStopped():
            return _session
        s = config.load(_workspace(ctx))
        if s.java_home:
            os.environ["JAVA_HOME"] = s.java_home
        from pyspark.sql import SparkSession  # imported late: only tasks that use Spark need it

        log = ctx.log if ctx is not None else print
        log(f"starting Spark ({s.master}, Unity Catalog {s.uc_uri}, catalog {s.catalog})")
        builder = (
            SparkSession.builder.master(s.master)
            .appName(s.app_name)
            .config("spark.driver.memory", s.driver_memory)
            .config("spark.jars.packages", ",".join(s.packages))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.defaultCatalog", s.catalog)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.ui.showConsoleProgress", "false")
        )
        if s.ivy_dir:
            builder = builder.config("spark.jars.ivy", s.ivy_dir)
        for k, v in {**uc_catalog_conf(s.catalog, s), **s.conf}.items():
            builder = builder.config(k, v)
        _session = builder.getOrCreate()
        _session.sparkContext.setLogLevel("WARN")
        _settings = s
        return _session


def stop() -> None:
    """Stop the shared session (the next get_spark starts a new one)."""
    global _session
    with _lock:
        if _session is not None:
            _session.stop()
            _session = None
