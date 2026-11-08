"""Subsystem 6 – Audit Dimension Assembler.

Builds one *audit dimension* row per load (batch x target table) capturing
the runtime context – job, batch id, environment, code version, source,
row counts and data-quality indicators – and stamps every fact / dimension
row loaded in that batch with the resulting ``audit_key``.

This lets any BI user ask "how trustworthy is this number and where did it
come from?" directly from the star schema.
"""
from __future__ import annotations

import json
from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from ..deliver.surrogate_keys import SurrogateKeyGenerator

AUDIT_DIM = "etl_audit_dim"
AUDIT_KEY = "audit_key"

AUDIT_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("environment", T.StringType()),
    T.StructField("target_table", T.StringType()),
    T.StructField("source_system", T.StringType()),
    T.StructField("code_version", T.StringType()),
    T.StructField("spark_version", T.StringType()),
    T.StructField("run_user", T.StringType()),
    T.StructField("process_start", T.TimestampType()),
    T.StructField("process_end", T.TimestampType()),
    T.StructField("rows_extracted", T.LongType()),
    T.StructField("rows_loaded", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("error_events", T.LongType()),
    T.StructField("avg_dq_score", T.DoubleType()),
    T.StructField("quality_indicator", T.StringType()),
    T.StructField("completeness_indicator", T.StringType()),
    T.StructField("extra", T.StringType()),
])


class AuditDimensionAssembler:
    """Create the audit row for a load and tag DataFrames with its key."""

    def __init__(self, ctx: EtlContext, table: str = AUDIT_DIM) -> None:
        self.ctx = ctx
        self.table = table

    def assemble(
        self,
        target_table: str,
        source_system: Optional[str] = None,
        rows_extracted: Optional[int] = None,
        rows_loaded: Optional[int] = None,
        rows_rejected: Optional[int] = None,
        screen_result=None,
        extra: Optional[Dict[str, object]] = None,
    ) -> int:
        """Persist the audit row and return its integer ``audit_key``."""
        error_events = None
        avg_score = None
        indicator = "Clean"
        if screen_result is not None:
            error_events = screen_result.events.count()
            rows_rejected = rows_rejected if rows_rejected is not None else screen_result.rejected.count()
            rows_loaded = rows_loaded if rows_loaded is not None else screen_result.clean.count()
            avg_score = screen_result.clean.agg(F.avg("_dq_score")).first()[0]
            severities = {s["severity"] for s in screen_result.summary if s["failures"]}
            indicator = ("Failed" if "FATAL" in severities else "Errors" if "ERROR" in severities
                         else "Warnings" if "WARN" in severities else "Clean")
        complete = "Complete"
        if rows_extracted is not None and rows_loaded is not None and rows_loaded < rows_extracted:
            complete = "Partial"
        info = self.ctx.runtime_info()
        row = (
            self.ctx.batch_id, self.ctx.job_name, self.ctx.environment, target_table, source_system,
            self.ctx.code_version, info["spark_version"], self.ctx.user, self.ctx.started_at, utcnow(),
            rows_extracted, rows_loaded, rows_rejected, error_events,
            float(avg_score) if avg_score is not None else None, indicator, complete,
            json.dumps(extra, default=str) if extra else None,
        )
        df = self.ctx.spark.createDataFrame([row], AUDIT_SCHEMA)
        keyed = SurrogateKeyGenerator(self.ctx, self.table, AUDIT_KEY).assign(df)
        keyed = keyed.select(AUDIT_KEY, *[f.name for f in AUDIT_SCHEMA.fields])
        self.ctx.meta_store.append(keyed, self.table)
        return int(keyed.first()[AUDIT_KEY])

    @staticmethod
    def attach(df: DataFrame, audit_key: int, column: str = AUDIT_KEY) -> DataFrame:
        return df.withColumn(column, F.lit(int(audit_key)).cast("long"))

    def lookup(self, audit_key: int) -> Dict[str, object]:
        row = self.ctx.meta_store.read(self.table).where(F.col(AUDIT_KEY) == audit_key).first()
        return row.asDict() if row else {}
