"""Subsystem 28 – Sort System.

Sorting is still at the heart of ETL – merges, de-duplication, sorted
flat-file deliveries and data skipping. In Spark the "sort package" is the
distributed shuffle sort; this module wraps the right primitive per need:

* global total order (``orderBy``) for sorted deliveries
* per-partition sort (``sortWithinPartitions``) for cheap local order
* range-partitioned sort for balanced, ordered output files
* bucketed & sorted tables for shuffle-free sort-merge joins
* Delta ``OPTIMIZE ... ZORDER BY`` / liquid clustering for data skipping
* external-merge style: sort-merge join of two pre-sorted inputs
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext

ColumnLike = Union[str, Column]


def _cols(cols: Sequence[ColumnLike], descending: Sequence[bool] = ()) -> List[Column]:
    out = []
    for i, c in enumerate(cols):
        col = F.col(c) if isinstance(c, str) else c
        if descending and i < len(descending) and descending[i]:
            col = col.desc_nulls_last()
        elif isinstance(c, str):
            col = col.asc_nulls_last()
        out.append(col)
    return out


class SortSystem:
    def __init__(self, ctx: Optional[EtlContext] = None) -> None:
        self.ctx = ctx

    @staticmethod
    def sort(df: DataFrame, columns: Sequence[ColumnLike], descending: Sequence[bool] = (),
             scope: str = "global", num_partitions: Optional[int] = None) -> DataFrame:
        """``scope``: ``global`` (total order), ``partition`` (local), ``range``."""
        cols = _cols(columns, descending)
        if scope == "global":
            return df.orderBy(*cols)
        if scope == "partition":
            return df.sortWithinPartitions(*cols)
        if scope == "range":
            n = num_partitions or df.rdd.getNumPartitions()
            return df.repartitionByRange(n, *cols).sortWithinPartitions(*cols)
        raise ValueError(f"unknown scope {scope}")

    @staticmethod
    def top_n(df: DataFrame, n: int, columns: Sequence[ColumnLike], descending: Sequence[bool] = (),
              partition_by: Sequence[str] = ()) -> DataFrame:
        """First ``n`` rows (optionally per group) without a full global sort."""
        if not partition_by:
            return df.orderBy(*_cols(columns, descending)).limit(n)
        from pyspark.sql.window import Window

        w = Window.partitionBy(*partition_by).orderBy(*_cols(columns, descending))
        return df.withColumn("_rank", F.row_number().over(w)).where(F.col("_rank") <= n).drop("_rank")

    @staticmethod
    def is_sorted(df: DataFrame, column: str) -> bool:
        """Check a DataFrame's global order on ``column`` (by monotonically increasing id)."""
        from pyspark.sql.window import Window

        w = Window.orderBy(F.monotonically_increasing_id())
        return df.select(F.col(column), F.lag(column).over(w).alias("_prev")) \
                 .where(F.col("_prev") > F.col(column)).limit(1).count() == 0

    def write_sorted_file(self, df: DataFrame, path: str, columns: Sequence[ColumnLike], fmt: str = "csv",
                          single_file: bool = True, **options) -> None:
        """Deliver a sorted flat file (e.g. for a legacy consumer)."""
        out = df.orderBy(*_cols(columns))
        if single_file:
            out = out.coalesce(1)
        if fmt == "csv":
            options.setdefault("header", "true")
        out.write.mode("overwrite").format(fmt).options(**options).save(path)

    def write_bucketed(self, df: DataFrame, table: str, bucket_columns: Sequence[str], num_buckets: int,
                       sort_columns: Optional[Sequence[str]] = None) -> None:
        """Bucketed + sorted metastore table (parquet) for shuffle-free joins."""
        (df.write.mode("overwrite").format("parquet")
         .bucketBy(num_buckets, *bucket_columns).sortBy(*(sort_columns or bucket_columns))
         .saveAsTable(table))

    def zorder(self, table: str, columns: Sequence[str]) -> None:
        """Delta ``OPTIMIZE ZORDER BY`` (no-op on non-Delta storage)."""
        if self.ctx is None:
            raise ValueError("SortSystem needs a context for table operations")
        self.ctx.store.optimize(table, zorder_by=columns)

    def cluster_by(self, table: str, columns: Sequence[str]) -> None:
        """Databricks liquid clustering (replaces partitioning + Z-order)."""
        if self.ctx is None:
            raise ValueError("SortSystem needs a context for table operations")
        cols = ", ".join(f"`{c}`" for c in columns)
        self.ctx.spark.sql(f"ALTER TABLE {self.ctx.store.sql_ref(table)} CLUSTER BY ({cols})")

    @staticmethod
    def merge_sorted(left: DataFrame, right: DataFrame, keys: Sequence[str], how: str = "inner") -> DataFrame:
        """Sort-merge join hint, the distributed equivalent of a flat-file merge."""
        return left.hint("merge").join(right, list(keys), how)
