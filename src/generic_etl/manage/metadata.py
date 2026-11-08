"""Subsystem 34 – Metadata Repository Manager.

A single place for the three kinds of metadata Kimball describes:

* **business** – descriptions, owners, stewards, tags, sensitivity class
* **technical** – schemas, formats, locations, row counts (captured over time)
* **process** – runs, row counts, audit rows, quality, lineage (read from
  the operational tables written by the other subsystems)

Business descriptions can be pushed to Unity Catalog as table / column
comments and tags so they show up in Catalog Explorer.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

BUSINESS_TABLE = "etl_metadata_business"
TECHNICAL_TABLE = "etl_metadata_technical"

BUSINESS_SCHEMA = T.StructType([
    T.StructField("object_name", T.StringType()),
    T.StructField("column_name", T.StringType()),   # "" for the table itself
    T.StructField("object_type", T.StringType()),   # dimension | fact | staging | aggregate | bridge ...
    T.StructField("description", T.StringType()),
    T.StructField("owner", T.StringType()),
    T.StructField("steward", T.StringType()),
    T.StructField("classification", T.StringType()),  # public | internal | confidential | pii
    T.StructField("tags", T.MapType(T.StringType(), T.StringType())),
    T.StructField("updated_at", T.TimestampType()),
])
TECHNICAL_SCHEMA = T.StructType([
    T.StructField("object_name", T.StringType()),
    T.StructField("location", T.StringType()),
    T.StructField("format", T.StringType()),
    T.StructField("schema_json", T.StringType()),
    T.StructField("column_count", T.IntegerType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("storage_version", T.LongType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("captured_at", T.TimestampType()),
])


@dataclass
class ObjectDefinition:
    name: str
    object_type: str
    description: str = ""
    owner: str = ""
    steward: str = ""
    classification: str = "internal"
    tags: Dict[str, str] = field(default_factory=dict)
    columns: Dict[str, str] = field(default_factory=dict)            # column -> description
    column_classification: Dict[str, str] = field(default_factory=dict)


class MetadataRepository:
    def __init__(self, ctx: EtlContext) -> None:
        self.ctx = ctx
        self.meta = ctx.meta_store

    # ----------------------------------------------------------- business
    def register(self, obj: ObjectDefinition) -> None:
        now = utcnow()
        rows = [(obj.name, "", obj.object_type, obj.description, obj.owner, obj.steward, obj.classification,
                 obj.tags, now)]
        for col, desc in obj.columns.items():
            rows.append((obj.name, col, "column", desc, obj.owner, obj.steward,
                         obj.column_classification.get(col, obj.classification), {}, now))
        for col, cls in obj.column_classification.items():
            if col not in obj.columns:
                rows.append((obj.name, col, "column", "", obj.owner, obj.steward, cls, {}, now))
        df = self.ctx.spark.createDataFrame(rows, BUSINESS_SCHEMA)
        self.meta.upsert(df, BUSINESS_TABLE, ["object_name", "column_name"])

    def business(self, name: Optional[str] = None) -> DataFrame:
        df = self.meta.read_or_empty(BUSINESS_TABLE, BUSINESS_SCHEMA)
        return df.where(F.col("object_name") == name) if name else df

    def sensitive_columns(self, classes: Sequence[str] = ("pii", "confidential")) -> DataFrame:
        return self.business().where((F.col("column_name") != "") & F.col("classification").isin(list(classes)))

    # ---------------------------------------------------------- technical
    def capture(self, name: str, count_rows: bool = True) -> Dict[str, object]:
        store = self.ctx.store
        df = store.read(name)
        rec = {"object_name": name, "location": store.location(name), "format": store.format,
               "schema_json": df.schema.json(), "column_count": len(df.columns),
               "row_count": df.count() if count_rows else None,
               "storage_version": store.current_version(name), "batch_id": self.ctx.batch_id,
               "captured_at": utcnow()}
        self.meta.append_rows(TECHNICAL_TABLE, [rec], TECHNICAL_SCHEMA)
        return rec

    def technical(self, name: Optional[str] = None, latest: bool = True) -> DataFrame:
        df = self.meta.read_or_empty(TECHNICAL_TABLE, TECHNICAL_SCHEMA)
        if name:
            df = df.where(F.col("object_name") == name)
        if latest:
            from pyspark.sql.window import Window

            w = Window.partitionBy("object_name").orderBy(F.desc("captured_at"))
            df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
        return df

    def schema_changes(self, name: str) -> List[Dict[str, object]]:
        """Schema evolution history of an object (column adds / drops between captures)."""
        snaps = self.technical(name, latest=False).orderBy("captured_at").collect()
        changes, prev = [], None
        for s in snaps:
            cols = {f["name"]: f["type"] for f in json.loads(s["schema_json"])["fields"]}
            if prev is not None and cols != prev:
                changes.append({"captured_at": s["captured_at"], "added": sorted(set(cols) - set(prev)),
                                "removed": sorted(set(prev) - set(cols))})
            prev = cols
        return changes

    # ------------------------------------------------------------ process
    def process(self, name: str) -> Dict[str, object]:
        """Latest operational facts about an object from the other subsystems."""
        out: Dict[str, object] = {}
        m = self.meta
        if m.exists("etl_audit_dim"):
            row = (m.read("etl_audit_dim").where(F.col("target_table") == name)
                   .orderBy(F.desc("process_end")).first())
            out["last_audit"] = row.asDict() if row else None
        if m.exists("etl_error_event_fact"):
            out["error_events"] = m.read("etl_error_event_fact").where(F.col("table_name") == name).count()
        if m.exists("etl_lineage_edge"):
            e = m.read("etl_lineage_edge").where(F.col("target_column").isNull())
            out["upstream"] = sorted({r["source"] for r in e.where(F.col("target") == name).collect()})
            out["downstream"] = sorted({r["target"] for r in e.where(F.col("source") == name).collect()})
        return out

    def describe(self, name: str) -> Dict[str, object]:
        biz = [r.asDict() for r in self.business(name).collect()]
        tech = self.technical(name).first()
        return {"business": biz, "technical": tech.asDict() if tech else None, "process": self.process(name)}

    def search(self, text: str) -> DataFrame:
        t = f"%{text.lower()}%"
        return self.business().where(F.lower("object_name").like(t) | F.lower("column_name").like(t) |
                                     F.lower("description").like(t))

    # -------------------------------------------------------- Unity Catalog
    def comment_statements(self, name: str) -> List[str]:
        """``COMMENT ON`` / ``ALTER COLUMN COMMENT`` / ``SET TAGS`` for UC."""
        ref = self.ctx.store.sql_ref(name)
        stmts = []
        for r in self.business(name).collect():
            desc = (r["description"] or "").replace("'", "\\'")
            if not r["column_name"]:
                if desc:
                    stmts.append(f"COMMENT ON TABLE {ref} IS '{desc}'")
                tags = dict(r["tags"] or {})
                tags.setdefault("classification", r["classification"])
                if tags:
                    kv = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
                    stmts.append(f"ALTER TABLE {ref} SET TAGS ({kv})")
            else:
                if desc:
                    stmts.append(f"ALTER TABLE {ref} ALTER COLUMN `{r['column_name']}` COMMENT '{desc}'")
                if r["classification"] in ("pii", "confidential"):
                    stmts.append(f"ALTER TABLE {ref} ALTER COLUMN `{r['column_name']}` "
                                 f"SET TAGS ('classification' = '{r['classification']}')")
        return stmts

    def publish_to_catalog(self, name: str, dry_run: bool = False) -> List[str]:
        stmts = self.comment_statements(name)
        if not dry_run:
            for s in stmts:
                self.ctx.spark.sql(s)
        return stmts
