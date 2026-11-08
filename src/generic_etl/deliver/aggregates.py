"""Subsystem 19 – Aggregate Builder.

Builds and incrementally maintains pre-computed summary (aggregate) fact
tables, optionally rolling up through dimension attributes (shrunken
dimensions), plus a tiny *aggregate navigator* choosing the smallest
aggregate that can answer a query.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext

_ADDITIVE = {"sum", "count", "min", "max"}


@dataclass
class DimensionRollup:
    """Join the fact to ``dimension`` on ``fact_key`` = ``dim_key`` and bring ``attributes``."""

    dimension: str
    fact_key: str
    dim_key: str
    attributes: Sequence[str]


@dataclass
class AggregateDefinition:
    name: str
    source_table: str
    group_by: Sequence[str]
    measures: Dict[str, Tuple[str, str]]  # alias -> (function, column)  e.g. {"revenue": ("sum", "amount")}
    rollups: Sequence[DimensionRollup] = field(default_factory=list)
    filter: Optional[str] = None


class AggregateBuilder:
    def __init__(self, ctx: EtlContext, definition: AggregateDefinition) -> None:
        self.ctx = ctx
        self.d = definition
        for alias, (fn, _) in definition.measures.items():
            if fn.lower() not in _ADDITIVE:
                raise ValueError(f"measure {alias}: only additive functions {_ADDITIVE} can be aggregated "
                                 f"incrementally; store sum and count to derive averages")

    def _source(self, df: Optional[DataFrame] = None) -> DataFrame:
        src = df if df is not None else self.ctx.store.read(self.d.source_table)
        if self.d.filter:
            src = src.where(self.d.filter)
        for r in self.d.rollups:
            dim = self.ctx.store.read(r.dimension).select(F.col(r.dim_key).alias(r.fact_key), *r.attributes)
            src = src.join(F.broadcast(dim), r.fact_key, "left")
        return src

    def _aggregate(self, src: DataFrame) -> DataFrame:
        aggs = [F.expr(f"{fn}({col})").alias(alias) for alias, (fn, col) in self.d.measures.items()]
        aggs.append(F.count(F.lit(1)).alias("_source_rows"))
        return src.groupBy(*self.d.group_by).agg(*aggs)

    def build(self) -> int:
        agg = self._aggregate(self._source())
        self.ctx.store.overwrite(agg, self.d.name)
        return self.ctx.store.read(self.d.name).count()

    def refresh(self, changed_facts: DataFrame, refresh_keys: Optional[Sequence[str]] = None) -> int:
        """Recompute only the groups touched by ``changed_facts``.

        ``refresh_keys`` (subset of ``group_by``, default all) identifies the
        slices to rebuild, e.g. ``["month_key"]``.
        """
        store = self.ctx.store
        if not store.exists(self.d.name):
            return self.build()
        keys = list(refresh_keys or self.d.group_by)
        touched = self._source(changed_facts).select(*keys).distinct()
        full = self._source().join(touched, keys, "left_semi")
        recomputed = self._aggregate(full)
        existing = store.read(self.d.name).join(touched, keys, "left_anti")
        store.overwrite(existing.unionByName(recomputed), self.d.name)
        return recomputed.count()


def choose_aggregate(required_columns: Sequence[str], definitions: Sequence[AggregateDefinition],
                     sizes: Optional[Dict[str, int]] = None) -> Optional[str]:
    """Aggregate navigator: smallest aggregate containing all required columns."""
    req = set(required_columns)
    candidates = []
    for d in definitions:
        cols = set(d.group_by) | set(d.measures)
        if req <= cols:
            candidates.append((sizes.get(d.name, len(d.group_by)) if sizes else len(d.group_by), d.name))
    return min(candidates)[1] if candidates else None
