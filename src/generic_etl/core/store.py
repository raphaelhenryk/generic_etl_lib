"""Physical table storage abstraction.

Every subsystem reads and writes tables through a :class:`TableStore` so the
library works the same way on Databricks (Unity Catalog / Hive metastore tables
backed by Delta Lake) and on a laptop (plain parquet folders).

Two addressing modes are supported:

* **catalog mode** – ``TableStore(spark, database="main.etl")`` – tables are
  addressed as ``<database>.<name>`` and created with ``saveAsTable``.
* **path mode** – ``TableStore(spark, base_path="/mnt/dw")`` – tables live in
  ``<base_path>/<name>`` folders.

The storage format defaults to Delta when Delta Lake is available and falls
back to parquet otherwise.  Operations that require Delta (MERGE, time travel,
RESTORE, CLONE, OPTIMIZE) degrade gracefully to equivalent full-rewrite
strategies when running on parquet.
"""
from __future__ import annotations

import os
import threading
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# Plain parquet folders have no transaction log, so two threads appending to the
# same folder (e.g. parallel scheduler tasks writing the run log) can collide.
# Non-Delta writes from this process are therefore serialised.
_PARQUET_WRITE_LOCK = threading.RLock()


def delta_available(spark: SparkSession) -> bool:
    """Return True when Delta Lake can be used in this Spark session."""
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    try:
        import delta.tables  # noqa: F401
    except ImportError:
        return False
    try:
        extensions = spark.conf.get("spark.sql.extensions", "") or ""
    except Exception:  # pragma: no cover - defensive
        extensions = ""
    return "DeltaSparkSessionExtension" in extensions


class TableStore:
    """Read / write tables independent of the physical layout."""

    def __init__(
        self,
        spark: SparkSession,
        database: Optional[str] = None,
        base_path: Optional[str] = None,
        fmt: Optional[str] = None,
    ) -> None:
        if not database and not base_path:
            raise ValueError("TableStore needs either `database` or `base_path`")
        self.spark = spark
        self.database = database
        self.base_path = base_path.rstrip("/") if base_path else None
        self.format = fmt or ("delta" if delta_available(spark) else "parquet")

    # ------------------------------------------------------------------ naming
    @property
    def is_delta(self) -> bool:
        return self.format == "delta"

    @property
    def path_mode(self) -> bool:
        return self.base_path is not None

    def location(self, name: str) -> str:
        """Physical identifier of a table (path or fully-qualified name)."""
        if self.path_mode:
            return f"{self.base_path}/{name}"
        return f"{self.database}.{name}"

    def sql_ref(self, name: str) -> str:
        """Reference usable inside a SQL statement."""
        if self.path_mode:
            return f"{self.format}.`{self.location(name)}`"
        return self.location(name)

    # ------------------------------------------------------------------ basics
    def exists(self, name: str) -> bool:
        if self.path_mode:
            marker = "_delta_log" if self.is_delta else ""
            try:
                jvm = self.spark._jvm
                path = jvm.org.apache.hadoop.fs.Path(f"{self.location(name)}/{marker}".rstrip("/"))
                fs = path.getFileSystem(self.spark._jsc.hadoopConfiguration())
                if not fs.exists(path):
                    return False
            except Exception:  # Spark Connect: no JVM access -> probe by reading
                pass
            try:
                self.spark.read.format(self.format).load(self.location(name)).schema
                return True
            except Exception:
                return False
        return self.spark.catalog.tableExists(self.location(name))

    def read(self, name: str, version: Optional[int] = None, timestamp: Optional[str] = None) -> DataFrame:
        reader = self.spark.read.format(self.format)
        if version is not None or timestamp is not None:
            if not self.is_delta:
                raise NotImplementedError("Time travel requires Delta Lake")
            if version is not None:
                reader = reader.option("versionAsOf", version)
            else:
                reader = reader.option("timestampAsOf", timestamp)
        if self.path_mode:
            return reader.load(self.location(name))
        if version is None and timestamp is None:
            return self.spark.table(self.location(name))
        return reader.table(self.location(name))

    def read_or_empty(self, name: str, schema: StructType) -> DataFrame:
        if self.exists(name):
            return self.read(name)
        return self.spark.createDataFrame([], schema)

    def write(
        self,
        df: DataFrame,
        name: str,
        mode: str = "append",
        partition_by: Optional[Sequence[str]] = None,
        merge_schema: bool = True,
        replace_where: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        writer = df.write.format(self.format).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if merge_schema and mode == "append":
            writer = writer.option("mergeSchema", "true")
        if mode == "overwrite" and self.is_delta:
            writer = writer.option("overwriteSchema", "true")
        if replace_where:
            if not self.is_delta:
                raise NotImplementedError("replaceWhere requires Delta; use overwrite_partitions")
            writer = writer.option("replaceWhere", replace_where)
        for k, v in (options or {}).items():
            writer = writer.option(k, v)
        if self.is_delta:
            self._save(writer, name)
        else:
            with _PARQUET_WRITE_LOCK:
                self._save(writer, name)

    def _save(self, writer, name: str) -> None:
        if self.path_mode:
            writer.save(self.location(name))
        else:
            writer.saveAsTable(self.location(name))

    def append(self, df: DataFrame, name: str, **kw) -> None:
        self.write(df, name, mode="append", **kw)

    def overwrite(self, df: DataFrame, name: str, **kw) -> None:
        """Overwrite a table, safely even when ``df`` was derived from it."""
        if not self.is_delta:
            # Parquet cannot overwrite a folder it is reading from: break lineage.
            df = materialize(df)
        self.write(df, name, mode="overwrite", merge_schema=False, **kw)

    def append_rows(self, name: str, rows: Iterable[dict], schema: StructType) -> None:
        rows = list(rows)
        if not rows:
            return
        df = self.spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)
        self.append(df, name)

    def drop(self, name: str) -> None:
        if self.path_mode:
            jvm = self.spark._jvm
            path = jvm.org.apache.hadoop.fs.Path(self.location(name))
            fs = path.getFileSystem(self.spark._jsc.hadoopConfiguration())
            fs.delete(path, True)
        else:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.location(name)}")

    # ------------------------------------------------------------ merge logic
    def upsert(
        self,
        df: DataFrame,
        name: str,
        keys: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
        delete_condition: Optional[str] = None,
    ) -> None:
        """Insert-or-update ``df`` into table ``name`` matching on ``keys``.

        Uses Delta ``MERGE`` when possible, otherwise rewrites the table
        (existing rows not present in ``df`` + all rows of ``df``).
        """
        if not self.exists(name):
            self.write(df, name, mode="overwrite")
            return
        if self.is_delta:
            from delta.tables import DeltaTable

            target = (
                DeltaTable.forPath(self.spark, self.location(name))
                if self.path_mode
                else DeltaTable.forName(self.spark, self.location(name))
            )
            cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
            builder = target.alias("t").merge(df.alias("s"), cond)
            if delete_condition:
                builder = builder.whenMatchedDelete(condition=delete_condition)
            cols = update_columns or df.columns
            builder = builder.whenMatchedUpdate(set={c: f"s.`{c}`" for c in cols})
            builder = builder.whenNotMatchedInsertAll()
            builder.execute()
            return
        with _PARQUET_WRITE_LOCK:
            existing = self.read(name)
            kept = existing.join(df.select(*keys).distinct(), on=list(keys), how="left_anti")
            merged = kept.unionByName(df, allowMissingColumns=True)
            self.overwrite(merged, name)

    def overwrite_partitions(self, df: DataFrame, name: str, partition_cols: Sequence[str]) -> None:
        """Replace only the partitions present in ``df`` (snapshot reloads)."""
        if not self.exists(name):
            self.write(df, name, mode="overwrite", partition_by=partition_cols)
            return
        if self.is_delta:
            values = df.select(*partition_cols).distinct().collect()
            preds = []
            for row in values:
                parts = []
                for c in partition_cols:
                    v = row[c]
                    parts.append(f"`{c}` IS NULL" if v is None else f"`{c}` = '{v}'")
                preds.append("(" + " AND ".join(parts) + ")")
            self.write(df, name, mode="overwrite", replace_where=" OR ".join(preds) or "1=0")
            return
        existing = self.read(name)
        kept = existing.join(df.select(*partition_cols).distinct(), on=list(partition_cols), how="left_anti")
        self.overwrite(kept.unionByName(df, allowMissingColumns=True), name, partition_by=partition_cols)

    # -------------------------------------------------------- delta utilities
    def current_version(self, name: str) -> Optional[int]:
        if not self.is_delta or not self.exists(name):
            return None
        row = self.spark.sql(f"DESCRIBE HISTORY {self.sql_ref(name)} LIMIT 1").collect()
        return int(row[0]["version"]) if row else None

    def restore(self, name: str, version: int) -> None:
        if not self.is_delta:
            raise NotImplementedError("RESTORE requires Delta Lake")
        self.spark.sql(f"RESTORE TABLE {self.sql_ref(name)} TO VERSION AS OF {int(version)}")

    def optimize(self, name: str, zorder_by: Optional[Sequence[str]] = None) -> None:
        if not self.is_delta:
            return
        stmt = f"OPTIMIZE {self.sql_ref(name)}"
        if zorder_by:
            stmt += " ZORDER BY (" + ", ".join(f"`{c}`" for c in zorder_by) + ")"
        self.spark.sql(stmt)


def materialize(df: DataFrame) -> DataFrame:
    """Cut the lineage of ``df`` so it can overwrite its own source."""
    try:
        return df.localCheckpoint(eager=True)
    except Exception:  # pragma: no cover - e.g. Spark Connect / serverless
        pdf = df.toPandas()
        return df.sparkSession.createDataFrame(pdf, df.schema)


def row_hash(columns: Sequence[str], name: str = "_row_hash") -> Column:
    """Null-safe SHA-256 hash of ``columns`` used for change detection."""
    parts: List[Column] = [
        F.coalesce(F.col(c).cast("string"), F.lit("\u0000NULL\u0000")) for c in columns
    ]
    if not parts:
        return F.lit(None).cast("string").alias(name)
    return F.sha2(F.concat_ws("\u0001", *parts), 256).alias(name)
