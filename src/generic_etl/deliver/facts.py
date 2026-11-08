"""Subsystem 13 – Fact Table Builders.

Loaders for Kimball's three fundamental fact table grains:

* :class:`TransactionFactBuilder` – one row per event, insert-only,
  idempotent re-runs via degenerate/transaction ids or batch replacement
* :class:`PeriodicSnapshotFactBuilder` – one row per entity per period,
  each period's partition is replaced atomically
* :class:`AccumulatingSnapshotFactBuilder` – one row per pipeline instance
  (order, claim...) updated as milestones are reached, with lag measures
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext


class _FactBuilder:
    def __init__(self, ctx: EtlContext, table: str, partition_by: Optional[Sequence[str]] = None,
                 audit_column: str = "audit_key") -> None:
        self.ctx = ctx
        self.table = table
        self.partition_by = list(partition_by or [])
        self.audit_column = audit_column

    def _stamp(self, df: DataFrame, audit_key: Optional[int]) -> DataFrame:
        if audit_key is not None:
            df = df.withColumn(self.audit_column, F.lit(int(audit_key)).cast("long"))
        return df


class TransactionFactBuilder(_FactBuilder):
    """Append-only transaction grain.

    ``unique_keys`` (e.g. order_number + line_number degenerate dims) make
    re-runs idempotent – rows already present are skipped.
    """

    def __init__(self, ctx: EtlContext, table: str, unique_keys: Sequence[str] = (),
                 partition_by: Optional[Sequence[str]] = None, audit_column: str = "audit_key") -> None:
        super().__init__(ctx, table, partition_by, audit_column)
        self.unique_keys = list(unique_keys)

    def load(self, facts: DataFrame, audit_key: Optional[int] = None) -> int:
        df = self._stamp(facts, audit_key)
        store = self.ctx.store
        if self.unique_keys:
            df = df.dropDuplicates(self.unique_keys)
            if store.exists(self.table):
                df = df.join(store.read(self.table).select(*self.unique_keys), self.unique_keys, "left_anti")
        df = df.cache()
        n = df.count()
        if n:
            if store.exists(self.table):
                store.append(df, self.table)
            else:
                store.write(df, self.table, mode="overwrite", partition_by=self.partition_by or None)
        return n


class PeriodicSnapshotFactBuilder(_FactBuilder):
    """Periodic snapshot grain: the period's partition is fully replaced."""

    def __init__(self, ctx: EtlContext, table: str, period_column: str, grain: Sequence[str] = (),
                 audit_column: str = "audit_key") -> None:
        super().__init__(ctx, table, [period_column], audit_column)
        self.period_column = period_column
        self.grain = list(grain)

    @staticmethod
    def build_from_transactions(transactions: DataFrame, period: Union[str, Column], period_column: str,
                                group_by: Sequence[str], measures: Dict[str, Column]) -> DataFrame:
        """Aggregate a transaction fact into periodic snapshot rows."""
        p = F.expr(period) if isinstance(period, str) else period
        return (transactions.withColumn(period_column, p)
                .groupBy(period_column, *group_by)
                .agg(*[e.alias(n) for n, e in measures.items()]))

    @staticmethod
    def carry_forward(previous: DataFrame, current: DataFrame, grain: Sequence[str], period_column: str,
                      period_value, zero_measures: Sequence[str] = ()) -> DataFrame:
        """Fill entities absent this period with the previous balance
        (semi-additive balances) or zero (flow measures)."""
        missing = previous.join(current.select(*grain), list(grain), "left_anti")
        missing = missing.withColumn(period_column, F.lit(period_value).cast(current.schema[period_column].dataType))
        for m in zero_measures:
            missing = missing.withColumn(m, F.lit(0).cast(current.schema[m].dataType))
        return current.unionByName(missing.select(*current.columns))

    def load(self, snapshot: DataFrame, audit_key: Optional[int] = None) -> int:
        df = self._stamp(snapshot, audit_key)
        if self.grain:
            dup = df.groupBy(self.period_column, *self.grain).count().where("count > 1").limit(1).count()
            if dup:
                raise ValueError(f"snapshot violates grain {[self.period_column] + self.grain}")
        df = df.cache()
        n = df.count()
        self.ctx.store.overwrite_partitions(df, self.table, [self.period_column])
        return n


class AccumulatingSnapshotFactBuilder(_FactBuilder):
    """Accumulating snapshot: rows are revisited as milestones occur.

    ``milestones`` are date (or yyyymmdd date-key) columns; once set they are
    never cleared by a later update with NULL. ``lags`` defines duration
    measures, e.g. ``{"days_order_to_ship": ("order_date_key", "ship_date_key")}``.
    """

    def __init__(self, ctx: EtlContext, table: str, grain: Sequence[str], milestones: Sequence[str],
                 lags: Optional[Dict[str, Tuple[str, str]]] = None, milestone_is_date_key: bool = False,
                 audit_column: str = "audit_key", unknown_date_key: int = -1) -> None:
        super().__init__(ctx, table, None, audit_column)
        self.grain = list(grain)
        self.milestones = list(milestones)
        self.lags = dict(lags or {})
        self.date_key = milestone_is_date_key
        self.unknown_date_key = unknown_date_key

    def _as_date(self, c: str) -> Column:
        col = F.col(c)
        if self.date_key:
            return F.when(col > 0, F.to_date(col.cast("string"), "yyyyMMdd"))
        return col.cast("date")

    def _with_lags(self, df: DataFrame) -> DataFrame:
        for name, (start, end) in self.lags.items():
            df = df.withColumn(name, F.datediff(self._as_date(end), self._as_date(start)))
        return df

    def _is_unset(self, c: Column) -> Column:
        return c.isNull() | (c == F.lit(self.unknown_date_key)) if self.date_key else c.isNull()

    def load(self, updates: DataFrame, audit_key: Optional[int] = None) -> Dict[str, int]:
        store = self.ctx.store
        upd = self._stamp(updates.dropDuplicates(self.grain), audit_key)
        if not store.exists(self.table):
            out = self._with_lags(upd)
            n = out.count()
            store.write(out, self.table, mode="overwrite")
            return {"inserted": n, "updated": 0}
        existing = store.read(self.table)
        cols = [c for c in existing.columns if c not in self.grain]
        for c in upd.columns:
            if c not in existing.columns and c not in self.grain:
                cols.append(c)
        u = upd.select(*self.grain, *[F.col(c).alias(f"__u_{c}") for c in upd.columns if c not in self.grain])
        e = existing.join(u, self.grain, "inner")
        merged = []
        for c in cols:
            new = F.col(f"__u_{c}") if f"__u_{c}" in e.columns else None
            old = F.col(c) if c in existing.columns else None
            if new is None:
                merged.append(old.alias(c))
            elif old is None:
                merged.append(new.alias(c))
            elif c in self.milestones:
                # never move a reached milestone back to "unknown"
                merged.append(F.when(self._is_unset(new), old).otherwise(new).alias(c))
            else:
                merged.append(F.coalesce(new, old).alias(c))
        updated = self._with_lags(e.select(*self.grain, *merged))
        inserted = self._with_lags(upd.join(existing.select(*self.grain), self.grain, "left_anti"))
        n_upd, n_ins = updated.count(), inserted.count()
        store.upsert(updated.unionByName(inserted, allowMissingColumns=True), self.table, self.grain)
        return {"inserted": n_ins, "updated": n_upd}
