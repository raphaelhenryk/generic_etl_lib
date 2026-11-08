"""Subsystem 31 – Parallelizing / Pipelining System.

Spark already parallelises *within* a job; this module parallelises
*between* independent ETL units (e.g. loading 30 dimensions at once) by
submitting Spark jobs from multiple threads, each in its own fair-scheduler
pool, and offers partition sizing helpers so each unit uses the cluster
efficiently.
"""
from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession


@dataclass
class ParallelResult:
    key: Any
    value: Any = None
    error: Optional[BaseException] = None

    @property
    def ok(self) -> bool:
        return self.error is None


class ParallelExecutor:
    def __init__(self, spark: SparkSession, max_workers: int = 4, pool_prefix: Optional[str] = "etl") -> None:
        self.spark = spark
        self.max_workers = max_workers
        self.pool_prefix = pool_prefix

    def _wrap(self, fn: Callable, key, index: int) -> Callable:
        def run(*args, **kwargs):
            sc = self.spark.sparkContext
            if self.pool_prefix:
                try:
                    sc.setLocalProperty("spark.scheduler.pool", f"{self.pool_prefix}_{index % self.max_workers}")
                except Exception:  # Spark Connect
                    pass
            try:
                sc.setJobGroup(str(key), f"etl unit {key}", interruptOnCancel=True)
            except Exception:
                pass
            return fn(*args, **kwargs)
        return run

    def run_all(self, tasks: Dict[Any, Callable[[], Any]], fail_fast: bool = False) -> Dict[Any, ParallelResult]:
        """Run independent zero-arg callables concurrently."""
        results: Dict[Any, ParallelResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._wrap(fn, k, i)): k for i, (k, fn) in enumerate(tasks.items())}
            for fut in as_completed(futures):
                k = futures[fut]
                try:
                    results[k] = ParallelResult(k, fut.result())
                except BaseException as exc:  # noqa: BLE001
                    results[k] = ParallelResult(k, error=exc)
                    if fail_fast:
                        for f in futures:
                            f.cancel()
                        raise
        return results

    def map(self, fn: Callable[[Any], Any], items: Iterable[Any], fail_fast: bool = False) -> Dict[Any, ParallelResult]:
        return self.run_all({item: (lambda it=item: fn(it)) for item in items}, fail_fast)

    def pipeline(self, items: Sequence[Any], stages: Sequence[Callable[[Any], Any]]) -> Dict[Any, ParallelResult]:
        """Push each item through ``stages`` sequentially while different items
        progress concurrently (item 2 extracts while item 1 loads)."""
        def chain(item):
            value = item
            for stage in stages:
                value = stage(value)
            return value
        return self.map(chain, items)


def optimal_partitions(size_bytes: int, target_partition_mb: int = 128, min_partitions: int = 1,
                       max_partitions: int = 10000) -> int:
    n = math.ceil(size_bytes / (target_partition_mb * 1024 * 1024)) if size_bytes else min_partitions
    return max(min_partitions, min(max_partitions, n))


def estimate_size_bytes(df: DataFrame) -> Optional[int]:
    """Catalyst size estimate of a DataFrame (may be None on Spark Connect)."""
    try:
        return int(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes().toString())
    except Exception:
        return None


def repartition_for_write(df: DataFrame, target_partition_mb: int = 128, by: Sequence[str] = ()) -> DataFrame:
    size = estimate_size_bytes(df)
    if size is None:
        return df
    n = optimal_partitions(size, target_partition_mb)
    return df.repartition(n, *by) if by else df.repartition(n)


def enable_adaptive_execution(spark: SparkSession, advisory_partition_mb: int = 128) -> None:
    """Recommended Spark settings for ETL workloads."""
    for k, v in {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": f"{advisory_partition_mb}m",
    }.items():
        try:
            spark.conf.set(k, v)
        except Exception:
            pass
