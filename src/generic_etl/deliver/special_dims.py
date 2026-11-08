"""Subsystem 12 – Special Dimensions Manager.

Builders for dimensions that do not come from a source system as-is:

* **date** and **time-of-day** dimensions (generated)
* **junk** dimensions – low-cardinality flags/indicators combined into one
* **mini-dimensions** – banded, rapidly-changing attributes split off a
  large dimension
* **shrunken** dimensions – conformed roll-up subsets of a base dimension
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..core.context import EtlContext
from ..core.store import row_hash
from .surrogate_keys import SurrogateKeyGenerator


# ----------------------------------------------------------------- date
def build_date_dimension(spark: SparkSession, start: Union[str, date], end: Union[str, date],
                         fiscal_year_start_month: int = 1, holidays: Optional[Dict[str, str]] = None,
                         week_start: str = "monday") -> DataFrame:
    """Calendar + fiscal date dimension with an integer ``date_key`` (yyyymmdd).

    Also includes the special "unknown" row (``date_key = -1``).
    """
    df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date")
    d = F.col("full_date")
    fm = fiscal_year_start_month
    fiscal_month = ((F.month(d) - fm + 12) % 12) + 1
    fiscal_year = F.when(F.month(d) >= fm, F.year(d) + (1 if fm > 1 else 0)).otherwise(F.year(d))
    dow = F.dayofweek(d)  # 1 = Sunday
    iso_dow = ((dow + 5) % 7) + 1  # 1 = Monday
    out = df.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        d.alias("full_date"),
        F.year(d).alias("calendar_year"),
        F.quarter(d).alias("calendar_quarter"),
        F.concat(F.year(d), F.lit("-Q"), F.quarter(d)).alias("year_quarter"),
        F.month(d).alias("calendar_month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "MMM").alias("month_abbrev"),
        F.date_format(d, "yyyy-MM").alias("year_month"),
        F.weekofyear(d).alias("iso_week"),
        F.dayofmonth(d).alias("day_of_month"),
        F.dayofyear(d).alias("day_of_year"),
        (iso_dow if week_start == "monday" else dow).alias("day_of_week"),
        F.date_format(d, "EEEE").alias("day_name"),
        F.date_format(d, "EEE").alias("day_abbrev"),
        dow.isin(1, 7).alias("is_weekend"),
        (d == F.last_day(d)).alias("is_month_end"),
        F.trunc(d, "month").alias("month_start_date"),
        F.last_day(d).alias("month_end_date"),
        fiscal_year.alias("fiscal_year"),
        fiscal_month.alias("fiscal_month"),
        (F.floor((fiscal_month - 1) / 3) + 1).cast("int").alias("fiscal_quarter"),
    )
    hol = holidays or {}
    if hol:
        m = F.create_map(*[x for k, v in hol.items() for x in (F.lit(k), F.lit(v))])
        out = out.withColumn("holiday_name", m[F.date_format(d, "yyyy-MM-dd")])
    else:
        out = out.withColumn("holiday_name", F.lit(None).cast("string"))
    out = out.withColumn("is_holiday", F.col("holiday_name").isNotNull())
    out = out.withColumn("is_workday", ~F.col("is_weekend") & ~F.col("is_holiday"))
    unknown = spark.createDataFrame([(-1,)], "date_key int")
    return out.unionByName(unknown, allowMissingColumns=True)


def build_time_dimension(spark: SparkSession, grain: str = "minute") -> DataFrame:
    """Time-of-day dimension at ``second`` or ``minute`` grain (``time_key`` = hhmm[ss])."""
    step = 1 if grain == "second" else 60
    df = spark.range(0, 86400, step).withColumnRenamed("id", "sec")
    h, m, s = F.floor(F.col("sec") / 3600), F.floor(F.col("sec") % 3600 / 60), F.col("sec") % 60
    key = h * 10000 + m * 100 + s if grain == "second" else h * 100 + m
    return df.select(
        key.cast("int").alias("time_key"),
        F.format_string("%02d:%02d:%02d", h, m, s).alias("time_of_day"),
        h.cast("int").alias("hour_24"),
        (((h + 11) % 12) + 1).cast("int").alias("hour_12"),
        F.when(h < 12, "AM").otherwise("PM").alias("am_pm"),
        m.cast("int").alias("minute"),
        s.cast("int").alias("second"),
        F.when(h < 6, "Night").when(h < 12, "Morning").when(h < 18, "Afternoon").otherwise("Evening").alias("day_part"),
    )


# ---------------------------------------------------------------- junk
class JunkDimensionBuilder:
    """Combine low-cardinality flags into one dimension.

    Rows are created for each *observed* combination (or the full cartesian
    product with ``all_combinations``). A hash of the attribute values is
    stored so incoming facts can be looked up quickly.
    """

    def __init__(self, ctx: EtlContext, dimension: str, attributes: Sequence[str], key_column: str) -> None:
        self.ctx = ctx
        self.dimension = dimension
        self.attributes = list(attributes)
        self.key_column = key_column
        self.keys = SurrogateKeyGenerator(ctx, dimension, key_column)

    def _hash(self) -> F.Column:
        return row_hash(self.attributes, "_junk_hash")

    def update(self, source: DataFrame, all_combinations: bool = False) -> int:
        combos = source.select(*self.attributes).distinct()
        if all_combinations:
            combos = None
            for a in self.attributes:
                vals = source.select(a).distinct()
                combos = vals if combos is None else combos.crossJoin(vals)
        combos = combos.withColumn("_junk_hash", self._hash())
        store = self.ctx.store
        if store.exists(self.dimension):
            combos = combos.join(store.read(self.dimension).select("_junk_hash"), "_junk_hash", "left_anti")
        n = combos.count()
        if n:
            keyed = self.keys.assign(combos, order_by=self.attributes)
            store.append(keyed.select(self.key_column, *self.attributes, "_junk_hash"), self.dimension)
        return n

    def lookup(self, fact: DataFrame, drop_attributes: bool = True) -> DataFrame:
        """Replace the flag columns of ``fact`` with the junk dimension key."""
        dim = self.ctx.store.read(self.dimension).select(self.key_column, "_junk_hash")
        out = fact.withColumn("_junk_hash", self._hash()).join(F.broadcast(dim), "_junk_hash", "left")
        out = out.withColumn(self.key_column, F.coalesce(F.col(self.key_column), F.lit(-1).cast("long")))
        out = out.drop("_junk_hash")
        return out.drop(*self.attributes) if drop_attributes else out


# --------------------------------------------------------------- mini
@dataclass
class Band:
    """Band a numeric attribute: ``Band("income", [(0, 50000, "Low"), ...])``.

    Ranges are half-open ``[low, high)``; ``high`` None means unbounded.
    """

    column: str
    ranges: List[Tuple[Optional[float], Optional[float], str]]
    target: Optional[str] = None
    default: str = "Unknown"

    def expr(self) -> F.Column:
        c = F.col(self.column)
        e = None
        for lo, hi, label in self.ranges:
            cond = F.lit(True)
            if lo is not None:
                cond = cond & (c >= lo)
            if hi is not None:
                cond = cond & (c < hi)
            e = F.when(cond, label) if e is None else e.when(cond, label)
        return (e.otherwise(self.default) if e is not None else F.lit(self.default)).alias(self.target or f"{self.column}_band")


class MiniDimensionBuilder(JunkDimensionBuilder):
    """Mini-dimension of banded, frequently changing attributes.

    The base dimension keeps only its slowly changing attributes; the fact
    table (and optionally the base dimension as a Type-1 "current mini key")
    carries the mini-dimension key.
    """

    def __init__(self, ctx: EtlContext, dimension: str, bands: Sequence[Band], key_column: str,
                 passthrough: Sequence[str] = ()) -> None:
        self.bands = list(bands)
        attrs = [b.target or f"{b.column}_band" for b in self.bands] + list(passthrough)
        self.passthrough = list(passthrough)
        super().__init__(ctx, dimension, attrs, key_column)

    def banded(self, df: DataFrame) -> DataFrame:
        return df.select("*", *[b.expr() for b in self.bands])

    def update(self, source: DataFrame, all_combinations: bool = False) -> int:
        return super().update(self.banded(source), all_combinations)

    def lookup(self, df: DataFrame, drop_attributes: bool = True) -> DataFrame:
        out = super().lookup(self.banded(df), drop_attributes=drop_attributes)
        return out


# ----------------------------------------------------------- shrunken
def build_shrunken_dimension(ctx: EtlContext, base: DataFrame, attributes: Sequence[str], dimension: str,
                             key_column: str) -> DataFrame:
    """Create a conformed roll-up (e.g. Month from Date, Brand from Product).

    The attribute names/values are taken verbatim from the base dimension so
    the shrunken dimension conforms to it. Existing keys are preserved.
    """
    attrs = list(attributes)
    combos = base.select(*attrs).distinct().where(" AND ".join(f"`{a}` IS NOT NULL" for a in attrs))
    store = ctx.store
    if store.exists(dimension):
        existing = store.read(dimension)
        new = combos.join(existing.select(*attrs), attrs, "left_anti")
        if new.limit(1).count():
            keyed = SurrogateKeyGenerator(ctx, dimension, key_column).assign(new, order_by=attrs)
            store.append(keyed.select(key_column, *attrs), dimension)
    else:
        keyed = SurrogateKeyGenerator(ctx, dimension, key_column).assign(combos, order_by=attrs)
        store.overwrite(keyed.select(key_column, *attrs), dimension)
    return store.read(dimension)
