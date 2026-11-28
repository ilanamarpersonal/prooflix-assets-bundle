"""Spark configuration for the pipeline tasks.

Every setting has a default for running on a laptop, can be set in the workspace's
conf/spark.yaml, and can be overridden by an environment variable (the Helm chart
sets those in the pod):

    setting          spark.yaml key    environment variable         default
    master           master            PROOFLIX_SPARK_MASTER        local[*]
    driver memory    driver_memory     PROOFLIX_SPARK_DRIVER_MEMORY 4g
    packages         packages          PROOFLIX_SPARK_PACKAGES      Delta, Unity Catalog, Avro, JDBC drivers (below)
    package cache    ivy_dir           PROOFLIX_SPARK_IVY           Spark's default (~/.ivy2)
    Java             java_home         JAVA_HOME                    Homebrew's openjdk@17 when present
    Unity Catalog    uc_uri            PROOFLIX_UC_URI              http://localhost:8080
    UC token         uc_token          PROOFLIX_UC_TOKEN            "" (none)
    tenant catalog   catalog           PROOFLIX_UC_CATALOG          tenant1
    table storage    lakehouse         PROOFLIX_LAKEHOUSE           <workspace>/data/<catalog>/lakehouse
    more Spark conf  conf (a map)      PROOFLIX_SPARK_CONF          {} (JSON object, merged)

Tables are external Delta tables registered in Unity Catalog; `lakehouse` is the
folder their data lives in (<lakehouse>/<schema>/<table>).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

SPARK_VERSION = "4.0.5"
PACKAGES = (
    "io.delta:delta-spark_4.0_2.13:4.3.1",
    "io.unitycatalog:unitycatalog-spark_4.0_2.13:0.7.0",
    f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION}",
    # JDBC drivers for ingesting relational databases (the platform task prooflix.tasks.jdbc_ingest)
    "com.oracle.database.jdbc:ojdbc11:23.5.0.24.07",
    "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11",
    "com.mysql:mysql-connector-j:9.1.0",
    "org.postgresql:postgresql:42.7.4",
)
HOMEBREW_JAVA = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
CONF_FILE = Path("conf") / "spark.yaml"


@dataclass(frozen=True)
class SparkSettings:
    master: str = "local[*]"
    app_name: str = "prooflix"
    driver_memory: str = "4g"
    packages: tuple[str, ...] = PACKAGES
    ivy_dir: str | None = None
    java_home: str | None = None
    uc_uri: str = "http://localhost:8080"
    uc_token: str = ""
    catalog: str = "tenant1"
    lakehouse: Path | None = None
    conf: dict[str, str] = field(default_factory=dict)


def load(workspace: Path) -> SparkSettings:
    """The settings for tasks of this workspace: defaults, then conf/spark.yaml, then the environment."""
    file = workspace / CONF_FILE
    values: dict = yaml.safe_load(file.read_text()) or {} if file.is_file() else {}
    env = os.environ

    def pick(key: str, var: str, default):
        return env.get(var) or values.get(key) or default

    packages = pick("packages", "PROOFLIX_SPARK_PACKAGES", PACKAGES)
    if isinstance(packages, str):
        packages = [p.strip() for p in packages.split(",") if p.strip()]
    conf = {str(k): str(v) for k, v in (values.get("conf") or {}).items()}
    if extra := env.get("PROOFLIX_SPARK_CONF"):
        conf.update({str(k): str(v) for k, v in json.loads(extra).items()})
    java = env.get("JAVA_HOME") or values.get("java_home") or (str(HOMEBREW_JAVA) if HOMEBREW_JAVA.is_dir() else None)
    catalog = pick("catalog", "PROOFLIX_UC_CATALOG", "tenant1")
    lakehouse = pick("lakehouse", "PROOFLIX_LAKEHOUSE", None)
    lakehouse_path = Path(lakehouse) if lakehouse else workspace / "data" / catalog / "lakehouse"
    return SparkSettings(
        master=pick("master", "PROOFLIX_SPARK_MASTER", "local[*]"),
        driver_memory=pick("driver_memory", "PROOFLIX_SPARK_DRIVER_MEMORY", "4g"),
        packages=tuple(packages),
        ivy_dir=pick("ivy_dir", "PROOFLIX_SPARK_IVY", None),
        java_home=java,
        uc_uri=pick("uc_uri", "PROOFLIX_UC_URI", "http://localhost:8080").rstrip("/"),
        uc_token=pick("uc_token", "PROOFLIX_UC_TOKEN", ""),
        catalog=catalog,
        lakehouse=lakehouse_path.resolve(),  # data/ may be a link to the data lake
        conf=conf,
    )
