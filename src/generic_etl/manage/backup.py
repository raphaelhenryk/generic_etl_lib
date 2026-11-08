"""Subsystem 23 – Backup System.

Backs up staging datasets, warehouse tables and the ETL's own metadata
tables:

* Delta: ``DEEP CLONE`` (full, independent copy incl. data files) or
  ``SHALLOW CLONE`` (metadata only, for fast short-lived restore points)
* other formats: physical copy into ``<backup_store>/<table>__bkp_<ts>``

Backups are catalogued in ``etl_backup_catalog`` and can be restored or
expired by retention policy.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from ..core.store import TableStore

BACKUP_CATALOG = "etl_backup_catalog"
BACKUP_SCHEMA = T.StructType([
    T.StructField("backup_id", T.StringType()),
    T.StructField("source_table", T.StringType()),
    T.StructField("backup_table", T.StringType()),
    T.StructField("method", T.StringType()),
    T.StructField("source_version", T.LongType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("expires_at", T.TimestampType()),
    T.StructField("status", T.StringType()),
])


class BackupManager:
    def __init__(self, ctx: EtlContext, backup_store: Optional[TableStore] = None, retention_days: int = 30) -> None:
        self.ctx = ctx
        self.source = ctx.store
        self.target = backup_store or ctx.store
        self.retention_days = retention_days

    def backup(self, table: str, method: str = "deep", source: Optional[TableStore] = None) -> Dict[str, object]:
        src = source or self.source
        ts = utcnow()
        backup_id = f"{table}__bkp_{ts:%Y%m%d%H%M%S}"
        version = src.current_version(table)
        if src.is_delta and self.target.is_delta and method in ("deep", "shallow"):
            kind = "DEEP" if method == "deep" else "SHALLOW"
            self.ctx.spark.sql(f"CREATE OR REPLACE TABLE {self.target.sql_ref(backup_id)} "
                               f"{kind} CLONE {src.sql_ref(table)}")
        else:
            method = "copy"
            self.target.write(src.read(table), backup_id, mode="overwrite")
        rows = self.target.read(backup_id).count()
        rec = {"backup_id": backup_id, "source_table": table, "backup_table": self.target.location(backup_id),
               "method": method, "source_version": version, "row_count": rows, "batch_id": self.ctx.batch_id,
               "created_at": ts, "expires_at": ts + timedelta(days=self.retention_days), "status": "ACTIVE"}
        self.ctx.meta_store.append_rows(BACKUP_CATALOG, [rec], BACKUP_SCHEMA)
        return rec

    def backup_many(self, tables: Sequence[str], method: str = "deep") -> List[Dict[str, object]]:
        return [self.backup(t, method) for t in tables]

    def catalog(self, table: Optional[str] = None, active_only: bool = True) -> DataFrame:
        df = self.ctx.meta_store.read_or_empty(BACKUP_CATALOG, BACKUP_SCHEMA)
        if table:
            df = df.where(F.col("source_table") == table)
        if active_only:
            expired = df.where(F.col("status") == "EXPIRED").select("backup_id")
            df = df.where(F.col("status") == "ACTIVE").join(expired, "backup_id", "left_anti")
        return df

    def latest(self, table: str) -> Optional[str]:
        row = self.catalog(table).orderBy(F.desc("created_at")).first()
        return row["backup_id"] if row else None

    def restore(self, table: str, backup_id: Optional[str] = None) -> int:
        backup_id = backup_id or self.latest(table)
        if not backup_id:
            raise ValueError(f"no backup available for {table}")
        df = self.target.read(backup_id)
        self.source.overwrite(df, table)
        return self.source.read(table).count()

    def expire(self, now=None) -> List[str]:
        now = now or utcnow()
        old = self.catalog().where(F.col("expires_at") < F.lit(now)).collect()
        removed = []
        for r in old:
            self.target.drop(r["backup_id"])
            d = r.asDict()
            d["status"] = "EXPIRED"
            self.ctx.meta_store.append_rows(BACKUP_CATALOG, [d], BACKUP_SCHEMA)
            removed.append(r["backup_id"])
        return removed
