"""Subsystem 21 – Data Propagation Manager.

Ships clean, conformed presentation data to downstream applications and
business partners under an explicit contract: column selection, row
filters, masking, format and a manifest (row count, checksum, schema) so the
receiver can verify the delivery. Every propagation is logged.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

PROPAGATION_LOG = "etl_propagation_log"
PROPAGATION_SCHEMA = T.StructType([
    T.StructField("export_name", T.StringType()),
    T.StructField("partner", T.StringType()),
    T.StructField("target", T.StringType()),
    T.StructField("format", T.StringType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("checksum", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("exported_at", T.TimestampType()),
])


@dataclass
class ExportSpec:
    name: str
    target: str                      # path, JDBC table, or catalog table
    format: str = "parquet"          # parquet | csv | json | delta | jdbc | table
    partner: str = "internal"
    columns: Optional[Sequence[str]] = None
    rename: Dict[str, str] = field(default_factory=dict)
    filter: Optional[str] = None
    mask: Dict[str, str] = field(default_factory=dict)   # column -> hash | redact | partial
    mode: str = "overwrite"
    single_file: bool = False
    options: Dict[str, str] = field(default_factory=dict)
    write_manifest: bool = True


def dataset_checksum(df: DataFrame) -> str:
    """Order-independent checksum of a DataFrame's content."""
    h = F.xxhash64(*[F.col(f"`{c}`") for c in df.columns])
    row = df.agg(F.sum(h.cast("decimal(38,0)")).alias("s"), F.count(F.lit(1)).alias("n")).first()
    return f"{row['n']}:{row['s']}"


class DataPropagationManager:
    def __init__(self, ctx: EtlContext) -> None:
        self.ctx = ctx

    @staticmethod
    def _mask(df: DataFrame, mask: Dict[str, str]) -> DataFrame:
        from ..manage.security import mask_column

        for c, how in mask.items():
            df = df.withColumn(c, mask_column(F.col(c), how))
        return df

    def prepare(self, df: DataFrame, spec: ExportSpec) -> DataFrame:
        if spec.filter:
            df = df.where(spec.filter)
        if spec.columns:
            df = df.select(*spec.columns)
        df = self._mask(df, spec.mask)
        for old, new in spec.rename.items():
            df = df.withColumnRenamed(old, new)
        return df

    def export(self, df: DataFrame, spec: ExportSpec) -> Dict[str, object]:
        out = self.prepare(df, spec).cache()
        rows = out.count()
        checksum = dataset_checksum(out)
        if spec.format == "jdbc":
            out.write.format("jdbc").option("dbtable", spec.target).options(**spec.options).mode(spec.mode).save()
        elif spec.format == "table":
            out.write.mode(spec.mode).options(**spec.options).saveAsTable(spec.target)
        else:
            w = out.coalesce(1) if spec.single_file else out
            opts = dict(spec.options)
            if spec.format == "csv":
                opts.setdefault("header", "true")
            w.write.format(spec.format).mode(spec.mode).options(**opts).save(spec.target)
            if spec.write_manifest:
                manifest = {"export": spec.name, "partner": spec.partner, "rows": rows, "checksum": checksum,
                            "batch_id": self.ctx.batch_id, "exported_at": utcnow().isoformat(),
                            "schema": json.loads(out.schema.json())}
                self.ctx.spark.createDataFrame([(json.dumps(manifest),)], "value string").coalesce(1) \
                    .write.mode("overwrite").text(spec.target.rstrip("/") + "_manifest")
        self.ctx.meta_store.append_rows(PROPAGATION_LOG, [{
            "export_name": spec.name, "partner": spec.partner, "target": spec.target, "format": spec.format,
            "row_count": rows, "checksum": checksum, "batch_id": self.ctx.batch_id, "exported_at": utcnow()}],
            PROPAGATION_SCHEMA)
        out.unpersist()
        return {"export": spec.name, "rows": rows, "checksum": checksum, "target": spec.target}

    @staticmethod
    def delta_sharing_sql(share: str, table: str, recipient: Optional[str] = None,
                          alias: Optional[str] = None) -> List[str]:
        """SQL statements to publish a table through Databricks Delta Sharing."""
        stmts = [f"CREATE SHARE IF NOT EXISTS {share}",
                 f"ALTER SHARE {share} ADD TABLE {table}" + (f" AS {alias}" if alias else "")]
        if recipient:
            stmts.append(f"GRANT SELECT ON SHARE {share} TO RECIPIENT {recipient}")
        return stmts
