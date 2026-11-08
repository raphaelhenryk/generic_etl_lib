"""Subsystem 1 – Data Profiling System.

Analyses the content, structure and consistency of source data *before* ETL
logic is designed: column statistics, value distributions, pattern analysis,
candidate primary keys and candidate foreign-key relationships.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

_NUMERIC = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)
_ORDERABLE = _NUMERIC + (T.DateType, T.TimestampType, T.StringType)

PROFILE_SCHEMA = T.StructType(
    [
        T.StructField("column_name", T.StringType()),
        T.StructField("data_type", T.StringType()),
        T.StructField("row_count", T.LongType()),
        T.StructField("null_count", T.LongType()),
        T.StructField("null_pct", T.DoubleType()),
        T.StructField("distinct_count", T.LongType()),
        T.StructField("distinct_pct", T.DoubleType()),
        T.StructField("min_value", T.StringType()),
        T.StructField("max_value", T.StringType()),
        T.StructField("mean", T.DoubleType()),
        T.StructField("stddev", T.DoubleType()),
        T.StructField("min_length", T.IntegerType()),
        T.StructField("max_length", T.IntegerType()),
        T.StructField("blank_count", T.LongType()),
        T.StructField("is_candidate_key", T.BooleanType()),
    ]
)


@dataclass
class ForeignKeyCandidate:
    child_column: str
    parent_column: str
    child_distinct: int
    matched_distinct: int
    orphan_count: int

    @property
    def coverage(self) -> float:
        return self.matched_distinct / self.child_distinct if self.child_distinct else 0.0


class DataProfiler:
    """Profile a DataFrame.

    >>> profile = DataProfiler(df).profile()        # one row per column
    >>> DataProfiler(df).candidate_keys(max_columns=2)
    """

    def __init__(self, df: DataFrame, approximate: bool = False, rsd: float = 0.02) -> None:
        self.df = df
        self.approximate = approximate
        self.rsd = rsd

    def _distinct(self, c: str):
        if self.approximate:
            return F.approx_count_distinct(F.col(c), self.rsd)
        return F.countDistinct(F.col(c))

    # --------------------------------------------------------------- columns
    def profile(self, columns: Optional[Sequence[str]] = None) -> DataFrame:
        """Column-level statistics computed in a single pass over the data."""
        cols = list(columns or self.df.columns)
        types = {f.name: f.dataType for f in self.df.schema.fields}
        aggs = [F.count(F.lit(1)).alias("__rows")]
        for i, c in enumerate(cols):
            dt = types[c]
            col = F.col(f"`{c}`")
            aggs.append(F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(f"n{i}"))
            aggs.append(self._distinct(f"`{c}`").alias(f"d{i}"))
            if isinstance(dt, _ORDERABLE):
                aggs.append(F.min(col).cast("string").alias(f"mn{i}"))
                aggs.append(F.max(col).cast("string").alias(f"mx{i}"))
            if isinstance(dt, _NUMERIC):
                aggs.append(F.avg(col).cast("double").alias(f"av{i}"))
                aggs.append(F.stddev(col).cast("double").alias(f"sd{i}"))
            if isinstance(dt, T.StringType):
                aggs.append(F.min(F.length(col)).alias(f"ml{i}"))
                aggs.append(F.max(F.length(col)).alias(f"xl{i}"))
                aggs.append(F.sum(F.when(F.trim(col) == "", 1).otherwise(0)).alias(f"b{i}"))
        stats = self.df.agg(*aggs).collect()[0].asDict()
        rows_total = int(stats["__rows"] or 0)
        out = []
        for i, c in enumerate(cols):
            nulls = int(stats.get(f"n{i}") or 0)
            distinct = int(stats.get(f"d{i}") or 0)
            out.append(
                (
                    c,
                    types[c].simpleString(),
                    rows_total,
                    nulls,
                    (nulls / rows_total) if rows_total else 0.0,
                    distinct,
                    (distinct / rows_total) if rows_total else 0.0,
                    stats.get(f"mn{i}"),
                    stats.get(f"mx{i}"),
                    stats.get(f"av{i}"),
                    stats.get(f"sd{i}"),
                    stats.get(f"ml{i}"),
                    stats.get(f"xl{i}"),
                    int(stats[f"b{i}"]) if stats.get(f"b{i}") is not None else None,
                    rows_total > 0 and nulls == 0 and distinct == rows_total,
                )
            )
        return self.df.sparkSession.createDataFrame(out, PROFILE_SCHEMA)

    def value_frequencies(self, column: str, top_n: int = 20) -> DataFrame:
        """Most frequent values with their share of all rows."""
        total = self.df.count()
        return (
            self.df.groupBy(F.col(f"`{column}`").alias("value"))
            .count()
            .withColumn("pct", F.col("count") / F.lit(total if total else 1))
            .orderBy(F.desc("count"))
            .limit(top_n)
        )

    def pattern_frequencies(self, column: str, top_n: int = 20) -> DataFrame:
        """Character-class patterns (A=letter, 9=digit) – reveals format anomalies."""
        pattern = F.regexp_replace(
            F.regexp_replace(F.col(f"`{column}`").cast("string"), "[A-Za-z]", "A"), "[0-9]", "9"
        )
        return self.df.groupBy(pattern.alias("pattern")).count().orderBy(F.desc("count")).limit(top_n)

    # ------------------------------------------------------------- structure
    def candidate_keys(self, max_columns: int = 2, columns: Optional[Sequence[str]] = None) -> List[List[str]]:
        """Minimal column combinations that uniquely identify each row."""
        cols = list(columns or self.df.columns)
        total = self.df.count()
        found: List[List[str]] = []
        for size in range(1, max_columns + 1):
            for combo in combinations(cols, size):
                if any(set(k).issubset(combo) for k in found):
                    continue  # not minimal
                not_null = self.df.where(" AND ".join(f"`{c}` IS NOT NULL" for c in combo))
                if not_null.count() != total:
                    continue
                if not_null.select(*[f"`{c}`" for c in combo]).distinct().count() == total:
                    found.append(list(combo))
        return found

    def functional_dependency(self, determinant: Sequence[str], dependent: str) -> float:
        """Share of determinant values mapping to exactly one dependent value.

        1.0 means ``determinant -> dependent`` holds (e.g. zip -> city), which
        tells the designer an attribute belongs to a dimension hierarchy.
        """
        grouped = self.df.groupBy(*determinant).agg(F.countDistinct(dependent).alias("n"))
        stats = grouped.agg(F.count("*").alias("t"), F.sum(F.when(F.col("n") <= 1, 1).otherwise(0)).alias("ok")).first()
        return (stats["ok"] or 0) / stats["t"] if stats["t"] else 1.0

    @staticmethod
    def foreign_key_candidate(
        child: DataFrame, child_column: str, parent: DataFrame, parent_column: str
    ) -> ForeignKeyCandidate:
        """Measure how well ``child.child_column`` references ``parent.parent_column``."""
        c = child.select(F.col(child_column).alias("k")).where("k IS NOT NULL").distinct()
        p = parent.select(F.col(parent_column).alias("k")).distinct()
        child_distinct = c.count()
        matched = c.join(p, "k", "inner").count()
        orphans = child.join(
            p.withColumnRenamed("k", child_column), child_column, "left_anti"
        ).where(F.col(child_column).isNotNull()).count()
        return ForeignKeyCandidate(child_column, parent_column, child_distinct, matched, orphans)

    @staticmethod
    def discover_foreign_keys(
        child: DataFrame, parents: Dict[str, DataFrame], min_coverage: float = 0.95
    ) -> List[ForeignKeyCandidate]:
        """Try every same-typed child/parent column pair; keep high-coverage pairs.

        ``parents`` maps a table name to its DataFrame; only parent columns that
        are unique (candidate keys) are considered.
        """
        results: List[ForeignKeyCandidate] = []
        child_types = {f.name: f.dataType for f in child.schema.fields}
        for pname, pdf in parents.items():
            total = pdf.count()
            for pf in pdf.schema.fields:
                if pdf.select(pf.name).distinct().count() != total:
                    continue
                for cname, ctype in child_types.items():
                    if ctype != pf.dataType:
                        continue
                    cand = DataProfiler.foreign_key_candidate(child, cname, pdf, pf.name)
                    if cand.child_distinct and cand.coverage >= min_coverage:
                        cand.parent_column = f"{pname}.{pf.name}"
                        results.append(cand)
        return results

    # ------------------------------------------------------------- anomalies
    def anomalies(
        self, max_null_pct: float = 0.5, outlier_z: float = 4.0, profile: Optional[DataFrame] = None
    ) -> List[Dict[str, object]]:
        """Simple rule-based anomaly report built on the profile."""
        prof = (profile or self.profile()).collect()
        issues: List[Dict[str, object]] = []
        for r in prof:
            if r["row_count"] and r["null_pct"] > max_null_pct:
                issues.append({"column": r["column_name"], "issue": "high_null_rate", "value": r["null_pct"]})
            if r["row_count"] and r["distinct_count"] == 1:
                issues.append({"column": r["column_name"], "issue": "constant_column", "value": r["min_value"]})
            if r["blank_count"]:
                issues.append({"column": r["column_name"], "issue": "blank_strings", "value": r["blank_count"]})
            if r["stddev"]:
                lo = r["mean"] - outlier_z * r["stddev"]
                hi = r["mean"] + outlier_z * r["stddev"]
                n = self.df.where((F.col(f"`{r['column_name']}`") < lo) | (F.col(f"`{r['column_name']}`") > hi)).count()
                if n:
                    issues.append({"column": r["column_name"], "issue": "outliers", "value": n})
        return issues
