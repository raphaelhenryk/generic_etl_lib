"""Subsystem 18 – Fact Table Provider System.

Owns one or more fact tables. It *receives* conformed dimensions from the
Dimension Manager, verifies it is using the latest published versions,
runs the surrogate key pipeline, loads the fact table through the proper
builder and notifies downstream aggregates of what changed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from .dimension_manager import DimensionManager
from .key_pipeline import KeyPipelineResult, SurrogateKeyPipeline

FACT_LOAD_LOG = "etl_fact_load_log"
FACT_LOAD_SCHEMA = T.StructType([
    T.StructField("fact_table", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_loaded", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("dimension_versions", T.MapType(T.StringType(), T.LongType())),
    T.StructField("loaded_at", T.TimestampType()),
])


class StaleDimensionError(RuntimeError):
    pass


@dataclass
class FactLoadResult:
    rows_in: int
    rows_loaded: int
    rows_rejected: int
    rejected: DataFrame
    builder_stats: object = None
    dimension_versions: Dict[str, int] = field(default_factory=dict)


class FactTableProvider:
    def __init__(self, ctx: EtlContext, fact_table: str, pipeline: SurrogateKeyPipeline, builder,
                 dimension_manager: Optional[DimensionManager] = None, subscriber: str = "master",
                 suspense_table: Optional[str] = None) -> None:
        self.ctx = ctx
        self.fact_table = fact_table
        self.pipeline = pipeline
        self.builder = builder
        self.dm = dimension_manager
        self.subscriber = subscriber
        self.suspense_table = suspense_table or f"{fact_table}_suspense"
        self.listeners: List[Callable[[DataFrame], None]] = []

    def on_loaded(self, fn: Callable[[DataFrame], None]) -> None:
        """Register a callback (e.g. an aggregate refresh) receiving the loaded rows."""
        self.listeners.append(fn)

    def dimension_versions(self) -> Dict[str, int]:
        if not self.dm:
            return {}
        return {lk.dimension: self.dm.latest_version(lk.dimension, self.subscriber) for lk in self.pipeline.lookups}

    def check_dimensions(self, required: Optional[Dict[str, int]] = None) -> Dict[str, int]:
        """Ensure the dimensions received are at least at ``required`` versions."""
        versions = self.dimension_versions()
        for dim, need in (required or {}).items():
            if versions.get(dim, 0) < need:
                raise StaleDimensionError(f"{dim} at version {versions.get(dim, 0)}, need {need}")
        return versions

    def load(self, facts: DataFrame, audit_key: Optional[int] = None,
             required_versions: Optional[Dict[str, int]] = None) -> FactLoadResult:
        versions = self.check_dimensions(required_versions)
        rows_in = facts.count()
        keyed: KeyPipelineResult = self.pipeline.run(facts, audit_key=audit_key)
        stats = self.builder.load(keyed.facts, audit_key=audit_key)
        rows_rejected = keyed.rejected.count()
        if rows_rejected:
            self.ctx.store.append(keyed.rejected.withColumn("_batch_id", F.lit(self.ctx.batch_id)),
                                  self.suspense_table)
        rows_loaded = rows_in - rows_rejected
        self.ctx.meta_store.append_rows(FACT_LOAD_LOG, [{
            "fact_table": self.fact_table, "batch_id": self.ctx.batch_id, "rows_in": rows_in,
            "rows_loaded": rows_loaded, "rows_rejected": rows_rejected,
            "dimension_versions": versions, "loaded_at": utcnow()}], FACT_LOAD_SCHEMA)
        for fn in self.listeners:
            fn(keyed.facts)
        return FactLoadResult(rows_in, rows_loaded, rows_rejected, keyed.rejected, stats, versions)
