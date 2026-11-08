"""Subsystem 8 – Conforming System.

Harmonises data from heterogeneous sources so that dimensions and facts are
*conformed* across business processes:

* conformed **dimension** attributes: same names, same domains, same codes
  (via value mapping tables and standardisation rules)
* conformed **facts**: same units, currencies and definitions
* a conformance **check** between a local dimension and the enterprise master
* **drill-across**: combining several fact tables on their conformed
  dimensions without double counting.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from typing import Callable, Dict, List, Optional, Sequence, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass
class ConformingRule:
    """A transformation applied by the :class:`Conformer`."""

    apply: Callable[[DataFrame], DataFrame]
    description: str = ""


class Conformer:
    """Chainable set of rules that conform a source DataFrame.

    >>> conformer = (Conformer()
    ...     .rename({"cust_no": "customer_id"})
    ...     .standardize(["city"], case="upper")
    ...     .map_values("gender", {"M": "Male", "F": "Female"}, default="Unknown")
    ...     .convert_units("weight_lb", "weight_kg", 0.45359237)
    ...     .enforce_schema(conformed_schema))
    >>> conformed = conformer.apply(source_df)
    """

    def __init__(self) -> None:
        self.rules: List[ConformingRule] = []

    def add(self, fn: Callable[[DataFrame], DataFrame], description: str = "") -> "Conformer":
        self.rules.append(ConformingRule(fn, description))
        return self

    def rename(self, mapping: Dict[str, str]) -> "Conformer":
        def fn(df: DataFrame) -> DataFrame:
            for old, new in mapping.items():
                if old in df.columns:
                    df = df.withColumnRenamed(old, new)
            return df
        return self.add(fn, f"rename {mapping}")

    def standardize(self, columns: Sequence[str], case: Optional[str] = "upper", trim: bool = True,
                    empty_as_null: bool = True) -> "Conformer":
        def fn(df: DataFrame) -> DataFrame:
            for c in columns:
                col = F.col(c).cast("string")
                if trim:
                    col = F.trim(F.regexp_replace(col, r"\s+", " "))
                if case == "upper":
                    col = F.upper(col)
                elif case == "lower":
                    col = F.lower(col)
                elif case == "title":
                    col = F.initcap(col)
                if empty_as_null:
                    col = F.nullif(col, F.lit(""))
                df = df.withColumn(c, col)
            return df
        return self.add(fn, f"standardize {list(columns)}")

    def map_values(self, column: str, mapping: Union[Dict, DataFrame], default=None, target: Optional[str] = None,
                   keep_unmapped: bool = False, source_col: str = "source_value",
                   conformed_col: str = "conformed_value") -> "Conformer":
        """Translate source codes to conformed codes.

        ``mapping`` is a dict or a mapping DataFrame (``source_value``,
        ``conformed_value``) – typically maintained by data stewards.
        """
        target = target or column

        def fn(df: DataFrame) -> DataFrame:
            if isinstance(mapping, dict):
                m = F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])
                mapped = m[F.col(column)]
            else:
                lk = mapping.select(F.col(source_col).alias("__src"), F.col(conformed_col).alias("__conf"))
                df = df.join(F.broadcast(lk), F.col(column) == F.col("__src"), "left").drop("__src")
                mapped = F.col("__conf")
            fallback = F.col(column) if keep_unmapped else F.lit(default)
            df = df.withColumn("__mapped", F.coalesce(mapped, fallback))
            df = df.drop("__conf") if "__conf" in df.columns else df
            return df.withColumn(target, F.col("__mapped")).drop("__mapped")
        return self.add(fn, f"map {column}")

    def derive(self, column: str, expr: Union[str, Column]) -> "Conformer":
        return self.add(lambda df: df.withColumn(column, F.expr(expr) if isinstance(expr, str) else expr),
                        f"derive {column}")

    def convert_units(self, column: str, target: Optional[str] = None, factor: Union[float, Column] = 1.0,
                      drop_source: bool = False) -> "Conformer":
        """Conformed facts: ``target = column * factor`` (e.g. lb -> kg)."""
        target = target or column

        def fn(df: DataFrame) -> DataFrame:
            f = factor if isinstance(factor, Column) else F.lit(factor)
            df = df.withColumn(target, F.col(column) * f)
            return df.drop(column) if drop_source and target != column else df
        return self.add(fn, f"convert {column}")

    def convert_currency(self, amount: str, currency: str, rates: DataFrame, target: str,
                         date_column: Optional[str] = None, to_currency: str = "USD") -> "Conformer":
        """Convert amounts using a rate table (``from_currency``, [``rate_date``], ``rate``)."""
        def fn(df: DataFrame) -> DataFrame:
            r = rates.select(*[F.col(c).alias(f"__{c}") for c in rates.columns])
            cond = F.col(currency) == F.col("__from_currency")
            if date_column:
                cond = cond & (F.col(date_column) == F.col("__rate_date"))
            out = df.join(F.broadcast(r), cond, "left")
            rate = F.when(F.col(currency) == to_currency, F.lit(1.0)).otherwise(F.col("__rate"))
            out = out.withColumn(target, F.col(amount) * rate)
            return out.drop(*[f"__{c}" for c in rates.columns])
        return self.add(fn, f"currency {amount}->{target}")

    def enforce_schema(self, schema: T.StructType, drop_extra: bool = True) -> "Conformer":
        """Cast to the conformed schema, adding missing columns as NULL."""
        def fn(df: DataFrame) -> DataFrame:
            cols = [
                (F.col(f.name).cast(f.dataType) if f.name in df.columns else F.lit(None).cast(f.dataType)).alias(f.name)
                for f in schema.fields
            ]
            extra = [] if drop_extra else [c for c in df.columns if c not in schema.fieldNames()]
            return df.select(*cols, *extra)
        return self.add(fn, "enforce schema")

    def apply(self, df: DataFrame) -> DataFrame:
        return reduce(lambda acc, rule: rule.apply(acc), self.rules, df)


# ---------------------------------------------------------- conformance
@dataclass
class ConformanceReport:
    dimension: str
    key_columns: List[str]
    attribute_mismatches: Dict[str, int] = field(default_factory=dict)
    keys_missing_in_master: int = 0
    conformed: bool = True


def check_conformance(local_dim: DataFrame, master_dim: DataFrame, key_columns: Sequence[str],
                      attributes: Optional[Sequence[str]] = None, dimension: str = "") -> ConformanceReport:
    """Verify a local (possibly shrunken) dimension conforms to the master.

    Conformance means: identical attribute names and every (key, attribute)
    value present locally also exists in the master dimension.
    """
    attrs = list(attributes or [c for c in local_dim.columns if c in master_dim.columns and c not in key_columns])
    report = ConformanceReport(dimension, list(key_columns))
    missing = local_dim.select(*key_columns).distinct().join(master_dim.select(*key_columns).distinct(),
                                                             list(key_columns), "left_anti").count()
    report.keys_missing_in_master = missing
    joined = local_dim.select(*key_columns, *attrs).alias("l").join(
        master_dim.select(*key_columns, *attrs).alias("m"), list(key_columns), "inner")
    if attrs:
        row = joined.agg(*[F.sum(F.when(~F.col(f"l.{a}").eqNullSafe(F.col(f"m.{a}")), 1).otherwise(0)).alias(a)
                           for a in attrs]).first()
        report.attribute_mismatches = {a: int(row[a] or 0) for a in attrs if row[a]}
    report.conformed = missing == 0 and not report.attribute_mismatches
    return report


def drill_across(facts: Dict[str, DataFrame], conformed_columns: Sequence[str],
                 measures: Dict[str, Dict[str, Column]]) -> DataFrame:
    """Kimball drill-across: aggregate each fact to the conformed grain, then
    full-outer-join the results on the conformed attributes.

    ``measures`` = ``{"sales": {"revenue": F.sum("amount")}, "inventory": {...}}``
    """
    results = []
    for name, df in facts.items():
        aggs = [expr.alias(alias) for alias, expr in measures.get(name, {}).items()]
        results.append(df.groupBy(*conformed_columns).agg(*aggs))
    out = results[0]
    for r in results[1:]:
        out = out.join(r, list(conformed_columns), "full_outer")
    return out
