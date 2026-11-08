"""Subsystem 15 – Multi-Valued Dimension Bridge Table Builder.

When a fact is associated with *several* members of a dimension (diagnoses
per hospital visit, account holders per account, salespeople per deal) the
fact carries a **group key** pointing at a bridge table:

    fact.group_key -> bridge(group_key, dimension_key, weighting_factor)

Identical member sets share a group, so the bridge stays small. Weighting
factors (default ``1 / members``, or supplied allocations) let users produce
correctly allocated totals; unweighted joins produce "impact" reports.
"""
from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext
from .surrogate_keys import SurrogateKeyGenerator


class BridgeTableBuilder:
    def __init__(self, ctx: EtlContext, bridge_table: str, dimension_key: str,
                 group_key: str = "group_key", weight_column: str = "weighting_factor") -> None:
        self.ctx = ctx
        self.bridge_table = bridge_table
        self.dimension_key = dimension_key
        self.group_key = group_key
        self.weight_column = weight_column

    def build(self, associations: DataFrame, fact_keys: Sequence[str],
              weight: Optional[str] = None) -> DataFrame:
        """Create/reuse groups for each fact's member set.

        ``associations``: one row per (fact natural key(s), dimension_key[, weight]).
        Returns ``fact_keys + group_key`` to join back to the fact rows.
        """
        fk = list(fact_keys)
        members = associations.select(*fk, self.dimension_key,
                                      *( [F.col(weight).alias("__w")] if weight else [] ))
        member_struct = (F.struct(F.col(self.dimension_key).alias("k"), F.col("__w").alias("w"))
                         if weight else F.struct(F.col(self.dimension_key).alias("k")))
        sets = members.groupBy(*fk).agg(F.array_sort(F.collect_set(member_struct)).alias("__members"))
        sets = sets.withColumn("group_hash", F.sha2(F.to_json(F.col("__members")), 256))

        store = self.ctx.store
        groups = sets.select("group_hash", "__members").dropDuplicates(["group_hash"])
        if store.exists(self.bridge_table):
            known = store.read(self.bridge_table).select("group_hash", self.group_key).distinct()
            new_groups = groups.join(known, "group_hash", "left_anti")
        else:
            known = None
            new_groups = groups
        if new_groups.limit(1).count():
            keyed = SurrogateKeyGenerator(self.ctx, self.bridge_table, self.group_key).assign(
                new_groups, order_by=["group_hash"])
            exploded = keyed.select(self.group_key, "group_hash", F.explode("__members").alias("m"),
                                    F.size("__members").alias("__n"))
            w = F.col("m.w").cast("double") if weight else (F.lit(1.0) / F.col("__n"))
            rows = exploded.select(self.group_key, F.col("m.k").alias(self.dimension_key),
                                   w.alias(self.weight_column), "group_hash")
            if known is None:
                store.write(rows, self.bridge_table, mode="overwrite")
            else:
                store.append(rows, self.bridge_table)
        all_groups = store.read(self.bridge_table).select("group_hash", self.group_key).distinct()
        return sets.join(all_groups, "group_hash").select(*fk, self.group_key)

    def validate_weights(self, tolerance: float = 1e-6) -> int:
        """Number of groups whose weights do not sum to 1."""
        b = self.ctx.store.read(self.bridge_table)
        return (b.groupBy(self.group_key).agg(F.sum(self.weight_column).alias("s"))
                .where(F.abs(F.col("s") - 1.0) > tolerance).count())

    def allocate(self, fact: DataFrame, measures: Sequence[str]) -> DataFrame:
        """Explode facts through the bridge with weighted measures."""
        b = self.ctx.store.read(self.bridge_table).drop("group_hash")
        out = fact.join(b, self.group_key)
        for m in measures:
            out = out.withColumn(f"{m}_allocated", F.col(m) * F.col(self.weight_column))
        return out
