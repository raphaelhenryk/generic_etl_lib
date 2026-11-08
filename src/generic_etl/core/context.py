"""Runtime context shared by every subsystem."""
from __future__ import annotations

import getpass
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from .store import TableStore


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def current_user() -> str:
    for var in ("DATABRICKS_USER", "USER", "USERNAME"):
        if os.environ.get(var):
            return os.environ[var]
    try:
        return getpass.getuser()
    except Exception:  # pragma: no cover
        return "unknown"


@dataclass
class EtlContext:
    """Everything a subsystem needs to know about the current run.

    ``store`` is where *warehouse* tables are written; ``meta_store`` holds the
    ETL's own operational tables (error events, audit, run log, lineage...). By
    default they are the same.
    """

    spark: SparkSession
    store: TableStore
    job_name: str = "etl_job"
    environment: str = "dev"
    batch_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: datetime = field(default_factory=utcnow)
    user: str = field(default_factory=current_user)
    code_version: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    meta_store: Optional[TableStore] = None

    def __post_init__(self) -> None:
        if self.meta_store is None:
            self.meta_store = self.store

    @classmethod
    def create(
        cls,
        spark: Optional[SparkSession] = None,
        database: Optional[str] = None,
        base_path: Optional[str] = None,
        meta_database: Optional[str] = None,
        meta_base_path: Optional[str] = None,
        fmt: Optional[str] = None,
        **kwargs: Any,
    ) -> "EtlContext":
        spark = spark or SparkSession.builder.getOrCreate()
        store = TableStore(spark, database=database, base_path=base_path, fmt=fmt)
        meta = None
        if meta_database or meta_base_path:
            meta = TableStore(spark, database=meta_database, base_path=meta_base_path, fmt=fmt)
        return cls(spark=spark, store=store, meta_store=meta, **kwargs)

    def new_batch(self) -> "EtlContext":
        """Return a copy of the context with a fresh batch id / start time."""
        return EtlContext(
            spark=self.spark,
            store=self.store,
            job_name=self.job_name,
            environment=self.environment,
            user=self.user,
            code_version=self.code_version,
            params=dict(self.params),
            meta_store=self.meta_store,
        )

    def runtime_info(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "job_name": self.job_name,
            "environment": self.environment,
            "user": self.user,
            "code_version": self.code_version,
            "spark_version": self.spark.version,
            "databricks_runtime": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
            "started_at": self.started_at,
        }
