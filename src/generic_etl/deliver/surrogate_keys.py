"""Subsystem 10 – Surrogate Key Generator.

Creates meaningless, integer surrogate keys independently for every
dimension. A key registry table keeps the high-water mark per dimension so
keys are never reused – even if rows are later deleted – and keys stay dense
and monotonically increasing across parallel-safe batches.

Kimball also recommends *reserved* keys for special rows (unknown, not
applicable, not-yet-arrived); see :data:`SPECIAL_KEYS` and
:func:`default_rows`.
"""
from __future__ import annotations

from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from ..core.context import EtlContext, utcnow

SK_REGISTRY = "etl_surrogate_key_registry"
SK_REGISTRY_SCHEMA = T.StructType([
    T.StructField("dimension", T.StringType()),
    T.StructField("high_water_mark", T.LongType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("assigned_at", T.TimestampType()),
])

UNKNOWN_KEY = -1
NOT_APPLICABLE_KEY = -2
LATE_ARRIVING_KEY = -3
SPECIAL_KEYS: Dict[int, str] = {UNKNOWN_KEY: "Unknown", NOT_APPLICABLE_KEY: "Not Applicable",
                                LATE_ARRIVING_KEY: "Not Yet Available"}


class SurrogateKeyGenerator:
    """Assigns surrogate keys for one dimension."""

    def __init__(self, ctx: EtlContext, dimension: str, key_column: str, start: int = 1,
                 method: str = "zip") -> None:
        self.ctx = ctx
        self.dimension = dimension
        self.key_column = key_column
        self.start = start
        self.method = method

    def high_water_mark(self) -> int:
        """Largest key ever assigned (registry or table, whichever is larger)."""
        hwm = self.start - 1
        meta = self.ctx.meta_store
        if meta.exists(SK_REGISTRY):
            v = meta.read(SK_REGISTRY).where(F.col("dimension") == self.dimension).agg(
                F.max("high_water_mark")).first()[0]
            if v is not None:
                hwm = max(hwm, int(v))
        store = self.ctx.store
        if store.exists(self.dimension):
            dim = store.read(self.dimension)
            if self.key_column in dim.columns:
                v = dim.agg(F.max(self.key_column)).first()[0]
                if v is not None:
                    hwm = max(hwm, int(v))
        return hwm

    def _commit(self, hwm: int) -> None:
        self.ctx.meta_store.append_rows(SK_REGISTRY, [{
            "dimension": self.dimension, "high_water_mark": int(hwm),
            "batch_id": self.ctx.batch_id, "assigned_at": utcnow()}], SK_REGISTRY_SCHEMA)

    def assign(self, df: DataFrame, order_by: Optional[Sequence[str]] = None,
               offset: Optional[int] = None, commit: bool = True) -> DataFrame:
        """Return ``df`` with a new ``key_column`` = hwm + 1, hwm + 2, ..."""
        base = self.high_water_mark() if offset is None else offset
        if order_by:
            df = df.orderBy(*order_by)
        keyed = None
        if self.method == "zip":
            try:
                schema = T.StructType(df.schema.fields + [T.StructField(self.key_column, T.LongType(), True)])
                rdd = df.rdd.zipWithIndex().map(lambda r: tuple(r[0]) + (r[1] + base + 1,))
                keyed = self.ctx.spark.createDataFrame(rdd, schema)
            except Exception:  # Spark Connect / serverless: no RDD API
                keyed = None
        if keyed is None:
            order = [F.col(c) for c in order_by] if order_by else [F.monotonically_increasing_id()]
            keyed = df.withColumn(self.key_column, (F.row_number().over(Window.orderBy(*order)) + F.lit(base)).cast("long"))
        keyed = keyed.cache()
        if commit:
            new_max = keyed.agg(F.max(self.key_column)).first()[0]
            if new_max is not None:
                self._commit(int(new_max))
        return keyed


def default_rows(dim_schema: T.StructType, key_column: str,
                 natural_key_columns: Sequence[str] = (),
                 keys: Optional[Dict[int, str]] = None) -> list:
    """Build Kimball special rows (Unknown / N/A / Not yet available).

    String attributes receive the label, natural keys a ``"#<key>"`` marker,
    other columns ``None``. Returned as tuples matching ``dim_schema``.
    """
    rows = []
    for k, label in (keys or SPECIAL_KEYS).items():
        values = []
        for f in dim_schema.fields:
            if f.name == key_column:
                values.append(k)
            elif f.name in natural_key_columns and isinstance(f.dataType, T.StringType):
                values.append(f"#{k}")
            elif isinstance(f.dataType, T.StringType):
                values.append(label)
            elif isinstance(f.dataType, T.BooleanType):
                values.append(f.name in ("is_current", "_is_current"))
            else:
                values.append(None)
        rows.append(tuple(values))
    return rows


def nullable_schema(schema: T.StructType) -> T.StructType:
    return T.StructType([T.StructField(f.name, f.dataType, True, f.metadata) for f in schema.fields])


def ensure_default_rows(ctx: EtlContext, dimension: str, key_column: str,
                        natural_key_columns: Sequence[str] = ()) -> None:
    """Insert the special rows into an existing dimension if missing."""
    store = ctx.store
    dim = store.read(dimension)
    rows = default_rows(dim.schema, key_column, natural_key_columns)
    existing = {r[0] for r in dim.select(key_column).where(F.col(key_column) < 0).collect()}
    missing = [r for r in rows if r[[f.name for f in dim.schema.fields].index(key_column)] not in existing]
    if missing:
        store.append(ctx.spark.createDataFrame(missing, nullable_schema(dim.schema)), dimension)
