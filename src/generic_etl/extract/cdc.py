"""Subsystem 2 – Change Data Capture (CDC) System.

Isolates new / changed / deleted source rows since the previous load using
one of the classic strategies:

* :class:`TimestampCDC`   – audit columns (``last_updated``) / ascending ids
* :class:`SnapshotDiffCDC` – full snapshot comparison with row hashes
* :class:`DeltaChangeFeedCDC` – Delta Lake Change Data Feed (log scraping)

Watermarks are persisted in a :class:`WatermarkStore` so each run continues
where the last successful one stopped.
"""
from __future__ import annotations

from typing import Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from ..core.store import row_hash

CDC_OP = "_cdc_op"  # I = insert, U = update, D = delete
INSERT, UPDATE, DELETE = "I", "U", "D"

WATERMARK_TABLE = "etl_cdc_watermark"
WATERMARK_SCHEMA = T.StructType(
    [
        T.StructField("source_name", T.StringType()),
        T.StructField("watermark_column", T.StringType()),
        T.StructField("watermark_value", T.StringType()),
        T.StructField("batch_id", T.StringType()),
        T.StructField("updated_at", T.TimestampType()),
    ]
)


class WatermarkStore:
    """Persists the high-water mark per source (append-only history)."""

    def __init__(self, ctx: EtlContext, table: str = WATERMARK_TABLE) -> None:
        self.ctx = ctx
        self.table = table

    def get(self, source_name: str) -> Optional[str]:
        store = self.ctx.meta_store
        if not store.exists(self.table):
            return None
        row = (
            store.read(self.table)
            .where(F.col("source_name") == source_name)
            .orderBy(F.desc("updated_at"))
            .limit(1)
            .collect()
        )
        return row[0]["watermark_value"] if row else None

    def set(self, source_name: str, column: str, value) -> None:
        if value is None:
            return
        self.ctx.meta_store.append_rows(
            self.table,
            [
                {
                    "source_name": source_name,
                    "watermark_column": column,
                    "watermark_value": str(value),
                    "batch_id": self.ctx.batch_id,
                    "updated_at": utcnow(),
                }
            ],
            WATERMARK_SCHEMA,
        )


class TimestampCDC:
    """Incremental extraction on an ever-increasing column.

    Typical use::

        cdc = TimestampCDC(ctx, "crm.customers", "updated_at")
        changes, new_mark = cdc.extract(source_df)
        ... load ...
        cdc.commit(new_mark)          # only after a successful load
    """

    def __init__(
        self,
        ctx: EtlContext,
        source_name: str,
        column: str,
        lookback: Optional[str] = None,
        initial_value: Optional[str] = None,
    ) -> None:
        self.ctx = ctx
        self.source_name = source_name
        self.column = column
        self.lookback = lookback  # e.g. "INTERVAL 1 HOUR" to catch late commits
        self.initial_value = initial_value
        self.watermarks = WatermarkStore(ctx)

    def extract(self, source: DataFrame) -> Tuple[DataFrame, Optional[str]]:
        last = self.watermarks.get(self.source_name) or self.initial_value
        changes = source
        if last is not None:
            dtype = source.schema[self.column].dataType
            bound = F.lit(last).cast(dtype)
            if self.lookback:
                bound = bound - F.expr(self.lookback)
            changes = source.where(F.col(self.column) > bound)
        new_mark = changes.agg(F.max(self.column).cast("string")).first()[0]
        return changes.withColumn(CDC_OP, F.lit(UPDATE)), new_mark or last

    def commit(self, watermark: Optional[str]) -> None:
        self.watermarks.set(self.source_name, self.column, watermark)


class SnapshotDiffCDC:
    """Compare today's full extract with the previous snapshot.

    Use when the source has no reliable audit columns. The previous snapshot is
    stored in the staging area (``snapshot_table``) and replaced on commit.
    """

    def __init__(
        self,
        ctx: EtlContext,
        snapshot_table: str,
        keys: Sequence[str],
        compare_columns: Optional[Sequence[str]] = None,
        detect_deletes: bool = True,
    ) -> None:
        self.ctx = ctx
        self.snapshot_table = snapshot_table
        self.keys = list(keys)
        self.compare_columns = list(compare_columns) if compare_columns else None
        self.detect_deletes = detect_deletes

    def _hashed(self, df: DataFrame) -> DataFrame:
        cols = self.compare_columns or [c for c in df.columns if c not in self.keys and not c.startswith("_")]
        return df.withColumn("_cdc_hash", row_hash(cols, "_cdc_hash"))

    def diff(self, current: DataFrame, previous: Optional[DataFrame] = None) -> DataFrame:
        """Return changed rows of ``current`` tagged I/U, plus deleted keys tagged D."""
        store = self.ctx.store
        if previous is None:
            previous = store.read(self.snapshot_table) if store.exists(self.snapshot_table) else None
        cur = self._hashed(current)
        if previous is None:
            return cur.drop("_cdc_hash").withColumn(CDC_OP, F.lit(INSERT))
        prev = self._hashed(previous).select(*self.keys, F.col("_cdc_hash").alias("_prev_hash"))
        joined = cur.join(prev, self.keys, "left")
        changed = joined.where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_cdc_hash")))
        changed = changed.withColumn(
            CDC_OP, F.when(F.col("_prev_hash").isNull(), F.lit(INSERT)).otherwise(F.lit(UPDATE))
        ).drop("_prev_hash", "_cdc_hash")
        if not self.detect_deletes:
            return changed
        deleted = (
            previous.join(current.select(*self.keys), self.keys, "left_anti")
            .withColumn(CDC_OP, F.lit(DELETE))
        )
        return changed.unionByName(deleted.select(*changed.columns), allowMissingColumns=True)

    def commit(self, current: DataFrame) -> None:
        self.ctx.store.overwrite(current, self.snapshot_table)


class DeltaChangeFeedCDC:
    """Read the Delta Lake change data feed of a table since the last version.

    Requires ``delta.enableChangeDataFeed = true`` on the source table.
    """

    def __init__(self, ctx: EtlContext, source_table: str, source_name: Optional[str] = None) -> None:
        self.ctx = ctx
        self.source_table = source_table
        self.source_name = source_name or source_table
        self.watermarks = WatermarkStore(ctx)

    def extract(self) -> Tuple[DataFrame, Optional[str]]:
        last = self.watermarks.get(self.source_name)
        start = int(last) + 1 if last is not None else 0
        reader = self.ctx.spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start)
        df = reader.load(self.source_table) if "/" in self.source_table else reader.table(self.source_table)
        df = df.where(F.col("_change_type") != "update_preimage")
        df = df.withColumn(
            CDC_OP,
            F.when(F.col("_change_type") == "insert", INSERT)
            .when(F.col("_change_type") == "delete", DELETE)
            .otherwise(UPDATE),
        )
        new_mark = df.agg(F.max("_commit_version")).first()[0]
        return df, str(new_mark) if new_mark is not None else last

    def commit(self, watermark: Optional[str]) -> None:
        self.watermarks.set(self.source_name, "_commit_version", watermark)


def latest_change_per_key(changes: DataFrame, keys: Sequence[str], order_column: str) -> DataFrame:
    """Collapse multiple changes of the same key in one batch to the latest one."""
    from pyspark.sql.window import Window

    w = Window.partitionBy(*keys).orderBy(F.col(order_column).desc())
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
