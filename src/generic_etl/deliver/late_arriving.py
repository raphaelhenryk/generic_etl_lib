"""Subsystem 16 – Late Arriving Data Handler.

Two Kimball scenarios:

* **Late arriving dimension members** ("early arriving facts"): a fact
  references a natural key the dimension does not know yet. An *inferred
  member* row is inserted (``is_inferred = TRUE``) so the fact can be loaded
  with a valid key; when the real dimension record arrives the SCD manager
  overwrites the inferred row in place instead of versioning it.
* **Late arriving facts / backdated dimension changes**: facts are looked up
  against the dimension version valid *at the event date*; when a Type-2
  change arrives late, a version is inserted into the middle of the history
  and the affected facts are re-keyed.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Sequence, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext
from .scd import SCDConfig
from .surrogate_keys import SurrogateKeyGenerator


class LateArrivingDimensionHandler:
    def __init__(self, ctx: EtlContext, config: SCDConfig) -> None:
        self.ctx = ctx
        self.cfg = config

    def create_inferred_members(self, natural_keys: DataFrame, audit_key: Optional[int] = None) -> int:
        """Insert placeholder rows for natural keys missing from the dimension."""
        cfg = self.cfg
        store = self.ctx.store
        dim = store.read(cfg.dimension)
        nks = list(cfg.natural_keys)
        missing = (natural_keys.select(*nks).distinct()
                   .where(" AND ".join(f"`{k}` IS NOT NULL" for k in nks))
                   .join(dim.select(*nks), nks, "left_anti"))
        if missing.limit(1).count() == 0:
            return 0
        cols = []
        for f in dim.schema.fields:
            if f.name in nks:
                cols.append(F.col(f.name).cast(f.dataType))
            elif f.name == cfg.surrogate_key:
                continue
            elif f.name == cfg.effective_from:
                cols.append(F.lit("1900-01-01").cast(f.dataType).alias(f.name))
            elif f.name == cfg.effective_to:
                cols.append(F.lit(cfg.end_of_time).cast(f.dataType).alias(f.name))
            elif f.name in (cfg.is_current, cfg.is_inferred):
                cols.append(F.lit(True).alias(f.name))
            elif f.name == cfg.is_deleted:
                cols.append(F.lit(False).alias(f.name))
            elif f.name == cfg.audit_column:
                cols.append(F.lit(audit_key).cast(f.dataType).alias(f.name))
            else:
                cols.append(F.lit(None).cast(f.dataType).alias(f.name))
        rows = missing.select(*cols)
        keyed = SurrogateKeyGenerator(self.ctx, cfg.dimension, cfg.surrogate_key).assign(rows, order_by=nks)
        n = keyed.count()
        store.append(keyed.select(*dim.columns), cfg.dimension)
        return n

    def inferred_members(self) -> DataFrame:
        cfg = self.cfg
        return self.ctx.store.read(cfg.dimension).where(F.col(cfg.is_inferred))

    # --------------------------------------------- backdated type-2 change
    def insert_backdated_version(self, record: Dict[str, object], effective: Union[str, date, datetime]) -> Dict[str, object]:
        """Insert a Type-2 version effective in the past.

        The version that was valid on ``effective`` is split: it now ends at
        ``effective`` and the new version runs until that version's original
        end. Returns ``{"old_key", "new_key", "valid_from", "valid_to"}`` so
        facts in that window can be re-keyed with :meth:`rekey_facts`.
        """
        cfg = self.cfg
        store = self.ctx.store
        eff = F.lit(str(effective)).cast(cfg.date_type)
        dim = store.read(cfg.dimension)
        cond = None
        for k in cfg.natural_keys:
            c = F.col(k) == F.lit(record[k])
            cond = c if cond is None else cond & c
        container = dim.where(cond & (F.col(cfg.effective_from) <= eff) & (F.col(cfg.effective_to) > eff)).first()
        if container is None:
            raise ValueError(f"no version of {record} valid at {effective}")
        old = container.asDict()
        new_row = dict(old)
        new_row.update(record)
        new_row[cfg.effective_from] = None  # set below with proper type
        new_df = self.ctx.spark.createDataFrame([tuple(new_row.get(f.name) for f in dim.schema.fields)],
                                                T.StructType([T.StructField(f.name, f.dataType, True)
                                                              for f in dim.schema.fields]))
        new_df = (new_df.withColumn(cfg.effective_from, eff)
                  .withColumn(cfg.is_inferred, F.lit(False))
                  .drop(cfg.surrogate_key))
        for a, cur in cfg.type6.items():
            if cur in new_df.columns:
                new_df = new_df.withColumn(cur, F.lit(old[cur]))
        keyed = SurrogateKeyGenerator(self.ctx, cfg.dimension, cfg.surrogate_key).assign(new_df)
        new_key = int(keyed.first()[cfg.surrogate_key])
        updated_old = (dim.where(F.col(cfg.surrogate_key) == old[cfg.surrogate_key])
                       .withColumn(cfg.effective_to, eff)
                       .withColumn(cfg.is_current, F.lit(False)))
        store.upsert(updated_old.unionByName(keyed.select(*dim.columns)), cfg.dimension, [cfg.surrogate_key])
        return {"old_key": old[cfg.surrogate_key], "new_key": new_key,
                "valid_from": str(effective), "valid_to": str(old[cfg.effective_to])}

    def rekey_facts(self, fact_table: str, fact_key_column: str, fact_date_column: str,
                    change: Dict[str, object], date_is_key: bool = False) -> int:
        """Point facts in the backdated window to the new dimension version."""
        store = self.ctx.store
        facts = store.read(fact_table)
        d = F.col(fact_date_column)
        if date_is_key:
            d = F.to_date(d.cast("string"), "yyyyMMdd")
        in_window = ((F.col(fact_key_column) == F.lit(change["old_key"])) &
                     (d >= F.lit(change["valid_from"]).cast("date")) &
                     (d < F.lit(change["valid_to"]).cast("date")))
        n = facts.where(in_window).count()
        if n:
            store.overwrite(facts.withColumn(fact_key_column,
                                             F.when(in_window, F.lit(change["new_key"]).cast("long"))
                                             .otherwise(F.col(fact_key_column))), fact_table)
        return n


class LateArrivingFactHandler:
    """Helpers for facts that arrive after their period was loaded."""

    @staticmethod
    def split_late(facts: DataFrame, event_column: str, cutoff: Union[str, date, datetime]) -> Dict[str, DataFrame]:
        late = F.col(event_column) < F.lit(str(cutoff)).cast(facts.schema[event_column].dataType)
        return {"on_time": facts.where(~late), "late": facts.where(late)}

    @staticmethod
    def affected_periods(facts: DataFrame, period_expr: Union[str, F.Column]) -> List[object]:
        """Distinct periods (e.g. month keys) whose snapshots/aggregates must be rebuilt."""
        e = F.expr(period_expr) if isinstance(period_expr, str) else period_expr
        return [r[0] for r in facts.select(e.alias("p")).distinct().collect()]
