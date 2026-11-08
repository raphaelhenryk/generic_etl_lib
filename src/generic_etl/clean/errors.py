"""Subsystem 5 – Error Event Schema / Handler.

A centralised dimensional schema recording *every* error thrown by every
quality screen:

* ``etl_screen_dim``        – one row per screen (type, severity, action...)
* ``etl_error_event_fact``  – one row per (record, screen) failure, with the
  batch, table, the failing record's key and an optional JSON snapshot.

The schema makes it possible to report data quality over time, triage
problems back to source systems and drive the audit dimension's quality
indicators.
"""
from __future__ import annotations

import json
import uuid
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

SCREEN_DIM = "etl_screen_dim"
ERROR_EVENT_FACT = "etl_error_event_fact"

SCREEN_SCHEMA = T.StructType([
    T.StructField("screen_name", T.StringType()),
    T.StructField("screen_type", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("action", T.StringType()),
    T.StructField("columns", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("registered_at", T.TimestampType()),
])

ERROR_EVENT_SCHEMA = T.StructType([
    T.StructField("error_event_id", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("environment", T.StringType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("screen_name", T.StringType()),
    T.StructField("screen_type", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("action", T.StringType()),
    T.StructField("column_names", T.StringType()),
    T.StructField("record_key", T.StringType()),
    T.StructField("record_data", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
])


def build_error_events(ctx: EtlContext, annotated: DataFrame, screens: Sequence, flags: Sequence[str],
                       table_name: str, record_key: Sequence[str], original_cols: Sequence[str],
                       capture_record: bool = True) -> DataFrame:
    """Turn annotated failure flags into error-event rows (one per failure)."""
    spark = ctx.spark
    if not screens:
        return spark.createDataFrame([], ERROR_EVENT_SCHEMA)
    key_cols = list(record_key) or list(original_cols)
    key_json = F.to_json(F.struct(*[F.col(f"`{c}`") for c in key_cols]))
    rec_json = F.to_json(F.struct(*[F.col(f"`{c}`") for c in original_cols])) if capture_record else F.lit(None)
    parts = []
    for s, flag in zip(screens, flags):
        parts.append(
            annotated.where(F.col(flag)).select(
                F.expr("uuid()").alias("error_event_id"),
                F.lit(ctx.batch_id).alias("batch_id"),
                F.lit(ctx.job_name).alias("job_name"),
                F.lit(ctx.environment).alias("environment"),
                F.lit(table_name).alias("table_name"),
                F.lit(s.name).alias("screen_name"),
                F.lit(s.screen_type).alias("screen_type"),
                F.lit(s.severity).alias("severity"),
                F.lit(s.action).alias("action"),
                F.lit(",".join(s.columns)).alias("column_names"),
                key_json.alias("record_key"),
                rec_json.cast("string").alias("record_data"),
                F.current_timestamp().alias("event_ts"),
            )
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def table_screen_event(ctx: EtlContext, screen, table_name: str) -> tuple:
    return (uuid.uuid4().hex, ctx.batch_id, ctx.job_name, ctx.environment, table_name, screen.name,
            "table", screen.severity, screen.action, ",".join(screen.columns), None, None, utcnow())


class ErrorEventHandler:
    """Persists screens and error events and answers triage questions."""

    def __init__(self, ctx: EtlContext) -> None:
        self.ctx = ctx
        self.store = ctx.meta_store

    def register_screens(self, screens: Sequence) -> None:
        now = utcnow()
        rows = [(s.name, s.screen_type, s.severity, s.action, ",".join(s.columns), s.description, now)
                for s in screens]
        if rows:
            self.store.upsert(self.ctx.spark.createDataFrame(rows, SCREEN_SCHEMA), SCREEN_DIM, ["screen_name"])

    def record(self, events: DataFrame) -> int:
        n = events.count()
        if n:
            self.store.append(events.select(*[f.name for f in ERROR_EVENT_SCHEMA.fields]), ERROR_EVENT_FACT)
        return n

    def record_result(self, result, screens: Optional[Sequence] = None) -> int:
        """Convenience: register screens and record a :class:`ScreenResult`."""
        if screens:
            self.register_screens(screens)
        return self.record(result.events)

    def events(self, batch_id: Optional[str] = None) -> DataFrame:
        if not self.store.exists(ERROR_EVENT_FACT):
            return self.ctx.spark.createDataFrame([], ERROR_EVENT_SCHEMA)
        df = self.store.read(ERROR_EVENT_FACT)
        return df.where(F.col("batch_id") == batch_id) if batch_id else df

    def summary(self, batch_id: Optional[str] = None) -> DataFrame:
        return (self.events(batch_id)
                .groupBy("batch_id", "table_name", "screen_name", "severity", "action")
                .agg(F.count("*").alias("error_count"), F.max("event_ts").alias("last_seen"))
                .orderBy(F.desc("error_count")))

    def quality_indicator(self, batch_id: Optional[str] = None) -> Dict[str, object]:
        """Summarise a batch: worst severity, number of events, indicator text."""
        rows = self.events(batch_id or self.ctx.batch_id).groupBy("severity").count().collect()
        counts = {r["severity"]: int(r["count"]) for r in rows}
        order = ["FATAL", "ERROR", "WARN", "INFO"]
        worst = next((s for s in order if counts.get(s)), None)
        indicator = {"FATAL": "Failed", "ERROR": "Errors", "WARN": "Warnings", "INFO": "Clean"}.get(worst, "Clean")
        return {"worst_severity": worst, "error_events": sum(counts.values()), "indicator": indicator,
                "by_severity": counts}

    @staticmethod
    def parse_record(record_json: str) -> Dict[str, object]:
        return json.loads(record_json) if record_json else {}

    def failing_records(self, table_name: str, screen_name: str, batch_id: Optional[str] = None) -> List[dict]:
        rows = (self.events(batch_id).where((F.col("table_name") == table_name) &
                                            (F.col("screen_name") == screen_name))
                .select("record_data").collect())
        return [self.parse_record(r["record_data"]) for r in rows]
