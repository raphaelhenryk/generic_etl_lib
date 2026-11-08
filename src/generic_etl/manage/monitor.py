"""Subsystem 27 – Workflow Monitor.

Records every task execution (start, end, duration, status, rows in/out,
throughput, error) into ``etl_run_log`` and exposes dashboard queries:
current batch status, slowest tasks, throughput trends and failure rates.
"""
from __future__ import annotations

import logging
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Iterator, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

log = logging.getLogger("generic_etl")

RUN_LOG = "etl_run_log"
RUN_LOG_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("task_name", T.StringType()),
    T.StructField("environment", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("attempt", T.IntegerType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("ended_at", T.TimestampType()),
    T.StructField("duration_sec", T.DoubleType()),
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_out", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("rows_per_sec", T.DoubleType()),
    T.StructField("error", T.StringType()),
    T.StructField("metrics", T.MapType(T.StringType(), T.StringType())),
])


@dataclass
class TaskRun:
    task_name: str
    attempt: int = 1
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    rows_rejected: Optional[int] = None
    metrics: Dict[str, str] = field(default_factory=dict)

    def set(self, **kw) -> None:
        for k, v in kw.items():
            if k in ("rows_in", "rows_out", "rows_rejected"):
                setattr(self, k, None if v is None else int(v))
            else:
                self.metrics[k] = str(v)


class WorkflowMonitor:
    def __init__(self, ctx: EtlContext, persist: bool = True) -> None:
        self.ctx = ctx
        self.persist = persist
        self.events = []  # in-memory copy for the current process

    def record(self, task_name: str, status: str, started_at, ended_at, attempt: int = 1,
               rows_in=None, rows_out=None, rows_rejected=None, error: Optional[str] = None,
               metrics: Optional[Dict[str, str]] = None) -> dict:
        dur = (ended_at - started_at).total_seconds() if ended_at and started_at else None
        rate = (rows_out / dur) if rows_out and dur else None
        row = {"batch_id": self.ctx.batch_id, "job_name": self.ctx.job_name, "task_name": task_name,
               "environment": self.ctx.environment, "status": status, "attempt": attempt,
               "started_at": started_at, "ended_at": ended_at, "duration_sec": dur, "rows_in": rows_in,
               "rows_out": rows_out, "rows_rejected": rows_rejected, "rows_per_sec": rate,
               "error": error, "metrics": metrics or {}}
        self.events.append(row)
        log.info("task %s %s in %.2fs rows_out=%s", task_name, status, dur or 0.0, rows_out)
        if self.persist:
            self.ctx.meta_store.append_rows(RUN_LOG, [row], RUN_LOG_SCHEMA)
        return row

    @contextmanager
    def track(self, task_name: str, attempt: int = 1) -> Iterator[TaskRun]:
        """``with monitor.track("load_dim_customer") as run: run.set(rows_out=n)``"""
        run = TaskRun(task_name, attempt)
        start = utcnow()
        t0 = time.perf_counter()
        try:
            yield run
        except Exception as exc:
            self.record(task_name, "FAILED", start, utcnow(), attempt, run.rows_in, run.rows_out,
                        run.rows_rejected, f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=5)}",
                        run.metrics)
            raise
        else:
            run.metrics.setdefault("wall_clock_sec", f"{time.perf_counter() - t0:.3f}")
            self.record(task_name, "SUCCEEDED", start, utcnow(), attempt, run.rows_in, run.rows_out,
                        run.rows_rejected, None, run.metrics)

    # ------------------------------------------------------------ dashboards
    def runs(self) -> DataFrame:
        return self.ctx.meta_store.read_or_empty(RUN_LOG, RUN_LOG_SCHEMA)

    def batch_status(self, batch_id: Optional[str] = None) -> DataFrame:
        return (self.runs().where(F.col("batch_id") == (batch_id or self.ctx.batch_id))
                .select("task_name", "status", "attempt", "started_at", "duration_sec", "rows_out",
                        "rows_rejected", "rows_per_sec", "error")
                .orderBy("started_at"))

    def task_statistics(self, job_name: Optional[str] = None) -> DataFrame:
        df = self.runs()
        if job_name:
            df = df.where(F.col("job_name") == job_name)
        return (df.groupBy("job_name", "task_name")
                .agg(F.count("*").alias("runs"),
                     F.avg(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failure_rate"),
                     F.avg("duration_sec").alias("avg_duration_sec"),
                     F.max("duration_sec").alias("max_duration_sec"),
                     F.avg("rows_per_sec").alias("avg_rows_per_sec"),
                     F.sum("rows_out").alias("total_rows_out"),
                     F.max("started_at").alias("last_run"))
                .orderBy(F.desc("avg_duration_sec")))

    def anomalies(self, sigma: float = 3.0) -> DataFrame:
        """Runs whose duration deviates more than ``sigma`` std-devs from the task mean."""
        df = self.runs().where(F.col("status") == "SUCCEEDED")
        stats = df.groupBy("task_name").agg(F.avg("duration_sec").alias("mu"), F.stddev("duration_sec").alias("sd"))
        return (df.join(stats, "task_name")
                .where(F.col("sd").isNotNull() & (F.abs(F.col("duration_sec") - F.col("mu")) > sigma * F.col("sd")))
                .select("batch_id", "task_name", "duration_sec", "mu", "sd"))
