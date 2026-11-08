"""Subsystem 14 – Surrogate Key Pipeline.

Replaces the natural keys of incoming fact rows with dimension surrogate
keys, guaranteeing referential integrity:

* current-row lookups (Type 1 semantics) or **point-in-time** lookups
  against Type-2 history using the fact's event date
* configurable handling of misses: map to the *Unknown* row, reject to
  suspense, or create an **inferred member** (late arriving dimension)
* NULL natural keys map to the *Not Applicable* row
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext
from .scd import SCDConfig
from .surrogate_keys import NOT_APPLICABLE_KEY, UNKNOWN_KEY


class ReferentialIntegrityError(RuntimeError):
    pass


@dataclass
class DimensionLookup:
    """How to resolve one dimension key of the fact table.

    ``key_map`` maps fact column -> dimension natural key column.
    ``on_missing``: ``unknown`` | ``reject`` | ``infer`` | ``fail``.
    """

    dimension: str
    key_map: Dict[str, str]
    surrogate_key: str
    fact_key_column: Optional[str] = None
    as_of_column: Optional[str] = None
    on_missing: str = "unknown"
    drop_natural_keys: bool = True
    effective_from: str = "effective_from"
    effective_to: str = "effective_to"
    is_current: Optional[str] = "is_current"
    scd_config: Optional[SCDConfig] = None  # required for on_missing == "infer"
    as_of_is_date_key: bool = False

    @property
    def output(self) -> str:
        return self.fact_key_column or self.surrogate_key


@dataclass
class KeyPipelineResult:
    facts: DataFrame
    rejected: DataFrame
    inferred: Dict[str, int] = field(default_factory=dict)
    unknown: Dict[str, int] = field(default_factory=dict)


class SurrogateKeyPipeline:
    def __init__(self, ctx: EtlContext, lookups: Sequence[DimensionLookup]) -> None:
        self.ctx = ctx
        self.lookups = list(lookups)

    def _resolve(self, fact: DataFrame, lk: DimensionLookup, idx: int) -> DataFrame:
        dim = self.ctx.store.read(lk.dimension)
        tag = f"__d{idx}"
        nk_cols = list(lk.key_map.values())
        sel = [F.col(lk.surrogate_key).alias(f"{tag}_sk")] + [F.col(c).alias(f"{tag}_{c}") for c in nk_cols]
        cond = None
        for fcol, dcol in lk.key_map.items():
            c = F.col(fcol) == F.col(f"{tag}_{dcol}")
            cond = c if cond is None else cond & c
        if lk.as_of_column:
            sel += [F.col(lk.effective_from).alias(f"{tag}_from"), F.col(lk.effective_to).alias(f"{tag}_to")]
            d = dim.where(F.col(lk.surrogate_key) > 0).select(*sel)
            ev = F.col(lk.as_of_column)
            if lk.as_of_is_date_key:
                ev = F.to_date(ev.cast("string"), "yyyyMMdd")
            ev = ev.cast(dim.schema[lk.effective_from].dataType)
            cond = cond & (ev >= F.col(f"{tag}_from")) & (ev < F.col(f"{tag}_to"))
        else:
            d = dim.where(F.col(lk.surrogate_key) > 0)
            if lk.is_current and lk.is_current in dim.columns:
                d = d.where(F.col(lk.is_current))
            d = d.select(*sel)
        out = fact.join(d, cond, "left")
        null_nk = None
        for fcol in lk.key_map:
            c = F.col(fcol).isNull()
            null_nk = c if null_nk is None else null_nk | c
        out = out.withColumn(f"{tag}_miss", F.col(f"{tag}_sk").isNull() & ~null_nk)
        out = out.withColumn(lk.output, F.when(null_nk, F.lit(NOT_APPLICABLE_KEY))
                             .otherwise(F.coalesce(F.col(f"{tag}_sk"), F.lit(UNKNOWN_KEY))).cast("long"))
        drop = [c for c in out.columns if c.startswith(tag + "_") and c != f"{tag}_miss"]
        return out.drop(*drop)

    def run(self, facts: DataFrame, audit_key: Optional[int] = None) -> KeyPipelineResult:
        from .late_arriving import LateArrivingDimensionHandler

        inferred: Dict[str, int] = {}
        for lk in self.lookups:
            if lk.on_missing == "infer":
                if lk.scd_config is None:
                    raise ValueError(f"lookup {lk.dimension}: on_missing='infer' needs scd_config")
                nks = facts.select(*[F.col(f).alias(d) for f, d in lk.key_map.items()])
                inferred[lk.dimension] = LateArrivingDimensionHandler(self.ctx, lk.scd_config) \
                    .create_inferred_members(nks, audit_key)

        out = facts
        for i, lk in enumerate(self.lookups):
            out = self._resolve(out, lk, i)
        out = out.cache()

        miss_cols = [f"__d{i}_miss" for i in range(len(self.lookups))]
        unknown = {}
        if miss_cols:
            counts = out.agg(*[F.sum(F.col(c).cast("int")).alias(c) for c in miss_cols]).first()
            unknown = {lk.dimension: int(counts[c] or 0) for lk, c in zip(self.lookups, miss_cols)}
        for lk, c in zip(self.lookups, miss_cols):
            if lk.on_missing == "fail" and unknown.get(lk.dimension):
                raise ReferentialIntegrityError(f"{unknown[lk.dimension]} facts without {lk.dimension} key")

        reject = F.lit(False)
        for lk, c in zip(self.lookups, miss_cols):
            if lk.on_missing == "reject":
                reject = reject | F.col(c)
        drop_nk = [f for lk in self.lookups if lk.drop_natural_keys for f in lk.key_map
                   if f not in {l.as_of_column for l in self.lookups}]
        good = out.where(~reject).drop(*miss_cols, *drop_nk)
        bad = out.where(reject).withColumn("_reject_reason", F.concat_ws(",", *[
            F.when(F.col(c), F.lit(f"missing_{lk.dimension}")) for lk, c in zip(self.lookups, miss_cols)
        ])).drop(*miss_cols)
        return KeyPipelineResult(good, bad, inferred, unknown)

    @staticmethod
    def check_integrity(ctx: EtlContext, fact: DataFrame, checks: Dict[str, str]) -> Dict[str, int]:
        """Count fact keys not present in their dimension: ``{fact_col: "dim_table.sk_col"}``."""
        result = {}
        for fcol, ref in checks.items():
            table, sk = ref.rsplit(".", 1)
            dim = ctx.store.read(table).select(F.col(sk).alias(fcol)).distinct()
            result[fcol] = fact.join(dim, fcol, "left_anti").count()
        return result
