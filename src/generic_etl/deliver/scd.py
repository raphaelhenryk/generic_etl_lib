"""Subsystem 9 – Slowly Changing Dimension (SCD) Manager.

Implements Kimball's time-variance techniques per attribute:

* **Type 0** – retain original (value never changes after insert)
* **Type 1** – overwrite (on *all* rows of the natural key, history included)
* **Type 2** – add a new row; expire the previous one (``effective_from`` /
  ``effective_to`` half-open interval and ``is_current`` flag)
* **Type 3** – add a "prior value" column on the current row
* **Type 6** (1+2+3 hybrid) – Type 2 attribute with an extra *current value*
  column overwritten on every historical row

Also handles **inferred members** (placeholder rows created by the late
arriving dimension handler – they are overwritten in place, not versioned)
and optional **delete detection** for full snapshots.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Sequence, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..core.context import EtlContext
from .surrogate_keys import SurrogateKeyGenerator, default_rows, nullable_schema


@dataclass
class SCDConfig:
    dimension: str
    natural_keys: Sequence[str]
    surrogate_key: str
    type0: Sequence[str] = field(default_factory=list)
    type1: Optional[Sequence[str]] = None  # None => every other incoming attribute
    type2: Sequence[str] = field(default_factory=list)
    type3: Dict[str, str] = field(default_factory=dict)   # column -> prior_value column
    type6: Dict[str, str] = field(default_factory=dict)   # type2 column -> current_value column
    effective_from: str = "effective_from"
    effective_to: str = "effective_to"
    is_current: str = "is_current"
    is_inferred: str = "is_inferred"
    is_deleted: str = "is_deleted"
    date_type: str = "date"  # or "timestamp"
    end_of_time: str = "9999-12-31"
    initial_effective: Optional[str] = None  # effective_from for the very first load
    handle_deletes: str = "ignore"  # ignore | expire | flag
    add_default_rows: bool = True
    audit_column: Optional[str] = "audit_key"


@dataclass
class SCDResult:
    inserted: int
    new_versions: int
    updated: int
    expired: int

    def as_dict(self) -> Dict[str, int]:
        return self.__dict__.copy()


def _eq(a: Column, b: Column) -> Column:
    return a.eqNullSafe(b)


class SCDManager:
    """Apply incoming (already cleaned & conformed) records to a dimension."""

    def __init__(self, ctx: EtlContext, config: SCDConfig) -> None:
        self.ctx = ctx
        self.cfg = config
        self.keys = SurrogateKeyGenerator(ctx, config.dimension, config.surrogate_key)

    # -------------------------------------------------------------- helpers
    def _attributes(self, incoming: DataFrame):
        cfg = self.cfg
        reserved = set(cfg.natural_keys) | {cfg.surrogate_key, cfg.effective_from, cfg.effective_to,
                                            cfg.is_current, cfg.is_inferred, cfg.is_deleted}
        reserved |= set(cfg.type3.values()) | set(cfg.type6.values())
        if cfg.audit_column:
            reserved.add(cfg.audit_column)
        t2 = list(cfg.type2)
        t3 = list(cfg.type3)
        t0 = list(cfg.type0)
        if cfg.type1 is None:
            t1 = [c for c in incoming.columns if c not in reserved and c not in t2 + t3 + t0 and not c.startswith("_")]
        else:
            t1 = list(cfg.type1)
        return t0, t1, t2, t3

    def _lit_date(self, value) -> Column:
        return F.lit(str(value)).cast(self.cfg.date_type)

    def _column_order(self, incoming: DataFrame) -> List[str]:
        cfg = self.cfg
        t0, t1, t2, t3 = self._attributes(incoming)
        cols = [cfg.surrogate_key, *cfg.natural_keys]
        for c in t0 + t1 + t2 + t3:
            if c not in cols:
                cols.append(c)
        cols += list(cfg.type3.values()) + list(cfg.type6.values())
        cols += [cfg.effective_from, cfg.effective_to, cfg.is_current, cfg.is_inferred]
        if cfg.handle_deletes == "flag":
            cols.append(cfg.is_deleted)
        if cfg.audit_column:
            cols.append(cfg.audit_column)
        return cols

    def _new_rows(self, src: DataFrame, effective: Column, prior: Dict[str, Column], audit_key) -> DataFrame:
        cfg = self.cfg
        out = src
        for col, prior_col in cfg.type3.items():
            out = out.withColumn(prior_col, prior.get(col, F.lit(None)).cast(src.schema[col].dataType))
        for col, cur_col in cfg.type6.items():
            out = out.withColumn(cur_col, F.col(col))
        out = (out.withColumn(cfg.effective_from, effective)
               .withColumn(cfg.effective_to, self._lit_date(cfg.end_of_time))
               .withColumn(cfg.is_current, F.lit(True))
               .withColumn(cfg.is_inferred, F.lit(False)))
        if cfg.handle_deletes == "flag":
            out = out.withColumn(cfg.is_deleted, F.lit(False))
        if cfg.audit_column:
            out = out.withColumn(cfg.audit_column, F.lit(audit_key).cast("long"))
        return out

    # ---------------------------------------------------------------- apply
    def apply(self, incoming: DataFrame, as_of: Union[str, date, datetime, None] = None,
              audit_key: Optional[int] = None) -> SCDResult:
        cfg = self.cfg
        store = self.ctx.store
        nks = list(cfg.natural_keys)
        t0, t1, t2, t3 = self._attributes(incoming)
        attrs = list(dict.fromkeys(t0 + t1 + t2 + t3))
        as_of_col = self._lit_date(as_of) if as_of is not None else F.current_date().cast(cfg.date_type)
        src = incoming.select(*nks, *attrs).dropDuplicates(nks)
        order = self._column_order(incoming)

        if not store.exists(cfg.dimension):
            first_eff = self._lit_date(cfg.initial_effective) if cfg.initial_effective else as_of_col
            rows = self._new_rows(src, first_eff, {}, audit_key)
            keyed = self.keys.assign(rows, order_by=nks).select(*order)
            if cfg.add_default_rows:
                schema = nullable_schema(keyed.schema)
                defaults = (self.ctx.spark.createDataFrame(default_rows(schema, cfg.surrogate_key, nks), schema)
                            .withColumn(cfg.effective_from, self._lit_date("1900-01-01"))
                            .withColumn(cfg.effective_to, self._lit_date(cfg.end_of_time)))
                keyed = defaults.unionByName(keyed)
            n = keyed.where(F.col(cfg.surrogate_key) > 0).count()
            store.overwrite(keyed, cfg.dimension)
            return SCDResult(inserted=n, new_versions=0, updated=0, expired=0)

        existing = store.read(cfg.dimension)
        for c in order:  # schema evolution: add missing management columns
            if c not in existing.columns:
                dtype = src.schema[c].dataType if c in src.columns else "string"
                existing = existing.withColumn(c, F.lit(None).cast(dtype))
        real = existing.where(F.col(cfg.surrogate_key) > 0)
        current = real.where(F.col(cfg.is_current))

        inc = src.select(*nks, *[F.col(a).alias(f"__i_{a}") for a in attrs]).withColumn("__hit", F.lit(True))
        e = real.join(inc, nks, "left")
        hit = F.coalesce(F.col("__hit"), F.lit(False))
        inferred = F.coalesce(F.col(cfg.is_inferred), F.lit(False))
        is_cur = F.col(cfg.is_current)

        t2_changed = F.lit(False)
        for a in t2:
            t2_changed = t2_changed | ~_eq(F.col(a), F.col(f"__i_{a}"))
        e = e.withColumn("__t2", hit & is_cur & ~inferred & t2_changed)
        e = e.withColumn("__inf", hit & is_cur & inferred)
        e = e.withColumn("__gone", ~hit & is_cur)

        # ---- updated version of every existing row
        upd = e
        prior_for_new = {}
        for a in t1:
            upd = upd.withColumn(a, F.when(hit, F.col(f"__i_{a}")).otherwise(F.col(a)))
        for a in t0 + t2:
            # Type 0 / Type 2 values only change for inferred placeholders
            upd = upd.withColumn(a, F.when(F.col("__inf"), F.col(f"__i_{a}")).otherwise(F.col(a)))
        for a, prior_col in cfg.type3.items():
            changed3 = hit & is_cur & ~_eq(F.col(a), F.col(f"__i_{a}"))
            prior_for_new[a] = F.when(changed3, F.col(a)).otherwise(F.col(prior_col))
            upd = upd.withColumn("__p3_" + a, prior_for_new[a])
            upd = upd.withColumn(prior_col, F.when(changed3 & ~F.col("__t2") & ~F.col("__inf"), F.col(a))
                                 .otherwise(F.col(prior_col)))
            upd = upd.withColumn(a, F.when(changed3 & ~F.col("__t2"), F.col(f"__i_{a}")).otherwise(F.col(a)))
        for a, cur_col in cfg.type6.items():
            upd = upd.withColumn(cur_col, F.when(hit, F.col(f"__i_{a}")).otherwise(F.col(cur_col)))
        upd = (upd.withColumn(cfg.effective_to, F.when(F.col("__t2"), as_of_col).otherwise(F.col(cfg.effective_to)))
               .withColumn(cfg.is_current, F.when(F.col("__t2"), F.lit(False)).otherwise(F.col(cfg.is_current)))
               .withColumn(cfg.is_inferred, F.when(F.col("__inf"), F.lit(False)).otherwise(F.col(cfg.is_inferred))))

        # ---- deletes (full snapshot only)
        expired_deletes = F.lit(False)
        if cfg.handle_deletes in ("expire", "flag"):
            gone = F.col("__gone")
            if cfg.handle_deletes == "expire":
                expired_deletes = gone
                upd = (upd.withColumn(cfg.effective_to, F.when(gone, as_of_col).otherwise(F.col(cfg.effective_to)))
                       .withColumn(cfg.is_current, F.when(gone, F.lit(False)).otherwise(F.col(cfg.is_current))))
            else:
                upd = upd.withColumn(cfg.is_deleted, F.when(gone, F.lit(True))
                                     .when(hit, F.lit(False)).otherwise(F.col(cfg.is_deleted)))

        compare = [c for c in order if c not in (cfg.surrogate_key, cfg.audit_column)]
        before = existing.select(cfg.surrogate_key, *[F.col(c).alias(f"__b_{c}") for c in compare])
        upd = upd.join(before, cfg.surrogate_key)
        diff = F.lit(False)
        for c in compare:
            diff = diff | ~_eq(F.col(c), F.col(f"__b_{c}"))
        upd = upd.withColumn("__diff", diff)
        if cfg.audit_column:
            upd = upd.withColumn(cfg.audit_column, F.when(F.col("__diff"), F.lit(audit_key).cast("long"))
                                 .otherwise(F.col(cfg.audit_column)))
        upd = upd.cache()
        updates = upd.where(F.col("__diff")).select(*order)

        # ---- new versions for type 2 changes
        changed = upd.where(F.col("__t2"))
        nv_src = changed.select(*nks, *[F.col(f"__i_{a}").alias(a) for a in attrs],
                                *[F.col("__p3_" + a).alias("__prior_" + a) for a in cfg.type3])
        for a in t0:  # Type 0 keeps the original value in the new version too
            nv_src = nv_src.drop(a).join(changed.select(*nks, F.col(a)), nks)
        nv = self._new_rows(nv_src.select(*nks, *attrs, *["__prior_" + a for a in cfg.type3]), as_of_col,
                            {a: F.col("__prior_" + a) for a in cfg.type3}, audit_key)
        nv = nv.drop(*["__prior_" + a for a in cfg.type3])

        # ---- brand new natural keys
        new_keys = src.join(current.select(*nks), nks, "left_anti")
        new_rows = self._new_rows(new_keys, as_of_col, {}, audit_key)

        inserts = nv.unionByName(new_rows).select(*[c for c in order if c != cfg.surrogate_key])
        inserts = self.keys.assign(inserts, order_by=nks).select(*order)

        n_updated = updates.count()
        n_expired = upd.where(F.col("__t2") | expired_deletes).count()
        n_new_versions = nv.count()
        n_inserted = new_rows.count()
        delta = updates.unionByName(inserts)
        store.upsert(delta, cfg.dimension, keys=[cfg.surrogate_key])
        upd.unpersist()
        return SCDResult(inserted=n_inserted, new_versions=n_new_versions,
                         updated=n_updated - n_expired, expired=n_expired)

    # --------------------------------------------------------------- queries
    def current(self) -> DataFrame:
        cfg = self.cfg
        return self.ctx.store.read(cfg.dimension).where(F.col(cfg.is_current))

    def as_of(self, when: Union[str, date, datetime]) -> DataFrame:
        """Dimension rows valid at ``when`` (half-open interval)."""
        cfg = self.cfg
        t = self._lit_date(when)
        return self.ctx.store.read(cfg.dimension).where(
            (F.col(cfg.effective_from) <= t) & (F.col(cfg.effective_to) > t) | (F.col(cfg.surrogate_key) < 0))

    def history(self, **natural_key) -> DataFrame:
        cfg = self.cfg
        df = self.ctx.store.read(cfg.dimension)
        for k, v in natural_key.items():
            df = df.where(F.col(k) == v)
        return df.orderBy(cfg.effective_from)

    def validate(self) -> Dict[str, int]:
        """Integrity checks: one current row per key, no overlapping ranges."""
        cfg = self.cfg
        df = self.ctx.store.read(cfg.dimension).where(F.col(cfg.surrogate_key) > 0)
        multi_current = (df.where(F.col(cfg.is_current)).groupBy(*cfg.natural_keys).count()
                         .where("count > 1").count())
        w = Window.partitionBy(*cfg.natural_keys).orderBy(cfg.effective_from)
        overlaps = (df.withColumn("__next", F.lead(cfg.effective_from).over(w))
                    .where(F.col("__next").isNotNull() & (F.col(cfg.effective_to) > F.col("__next"))).count())
        dup_sk = df.groupBy(cfg.surrogate_key).count().where("count > 1").count()
        return {"multiple_current": multi_current, "overlapping_ranges": overlaps, "duplicate_keys": dup_sk}
