"""Subsystem 33 – Compliance Manager.

Supports regulatory requirements (SOX, GDPR/LGPD, HIPAA, BCBS 239...):

* **chain of custody** – every extract / transform / load / export of a
  dataset is recorded with a content fingerprint in a *hash chain*; any
  tampering with the log breaks :meth:`ComplianceManager.verify_chain`
* **access log** – who read which dataset, when and why
* **retention** – purge rows older than a policy and record the purge
* **right to erasure** – delete a data subject from all registered tables
  and keep proof of the deletion
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from ..deliver.propagation import dataset_checksum

CUSTODY_TABLE = "etl_chain_of_custody"
ACCESS_LOG = "etl_access_log"

CUSTODY_SCHEMA = T.StructType([
    T.StructField("sequence", T.LongType()),
    T.StructField("dataset", T.StringType()),
    T.StructField("action", T.StringType()),
    T.StructField("actor", T.StringType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("fingerprint", T.StringType()),
    T.StructField("details", T.StringType()),
    T.StructField("recorded_at", T.TimestampType()),
    T.StructField("previous_hash", T.StringType()),
    T.StructField("record_hash", T.StringType()),
])
ACCESS_SCHEMA = T.StructType([
    T.StructField("dataset", T.StringType()),
    T.StructField("accessed_by", T.StringType()),
    T.StructField("purpose", T.StringType()),
    T.StructField("columns", T.StringType()),
    T.StructField("accessed_at", T.TimestampType()),
])

GENESIS = "0" * 64


def _hash_record(rec: Dict[str, object]) -> str:
    payload = {k: (str(v) if v is not None else None) for k, v in rec.items() if k != "record_hash"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


@dataclass
class RetentionPolicy:
    table: str
    date_column: str
    keep_days: int


class ComplianceManager:
    def __init__(self, ctx: EtlContext) -> None:
        self.ctx = ctx
        self.store = ctx.meta_store

    # ------------------------------------------------- chain of custody
    def _last(self):
        if not self.store.exists(CUSTODY_TABLE):
            return 0, GENESIS
        row = self.store.read(CUSTODY_TABLE).orderBy(F.desc("sequence")).select("sequence", "record_hash").first()
        return (int(row["sequence"]), row["record_hash"]) if row else (0, GENESIS)

    def record_custody(self, dataset: str, action: str, df: Optional[DataFrame] = None,
                       details: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        seq, prev = self._last()
        fingerprint = dataset_checksum(df) if df is not None else None
        rows = int(fingerprint.split(":")[0]) if fingerprint else None
        rec = {"sequence": seq + 1, "dataset": dataset, "action": action.upper(), "actor": self.ctx.user,
               "job_name": self.ctx.job_name, "batch_id": self.ctx.batch_id, "row_count": rows,
               "fingerprint": fingerprint, "details": json.dumps(details or {}, default=str, sort_keys=True),
               "recorded_at": utcnow().replace(microsecond=0), "previous_hash": prev}
        rec["record_hash"] = _hash_record(rec)
        self.store.append_rows(CUSTODY_TABLE, [rec], CUSTODY_SCHEMA)
        return rec

    def custody_trail(self, dataset: Optional[str] = None) -> DataFrame:
        df = self.store.read_or_empty(CUSTODY_TABLE, CUSTODY_SCHEMA)
        return (df.where(F.col("dataset") == dataset) if dataset else df).orderBy("sequence")

    def verify_chain(self) -> Dict[str, object]:
        """Recompute the hash chain; returns the first broken sequence if any."""
        prev = GENESIS
        n = 0
        for r in self.custody_trail().collect():
            rec = r.asDict()
            n += 1
            if rec["previous_hash"] != prev or _hash_record(rec) != rec["record_hash"]:
                return {"valid": False, "broken_at": rec["sequence"], "records": n}
            prev = rec["record_hash"]
        return {"valid": True, "broken_at": None, "records": n}

    def verify_dataset(self, dataset: str, df: DataFrame) -> bool:
        """Does ``df`` still match the fingerprint of its last custody record?"""
        last = self.custody_trail(dataset).orderBy(F.desc("sequence")).first()
        return bool(last) and last["fingerprint"] == dataset_checksum(df)

    # ------------------------------------------------------- access log
    def log_access(self, dataset: str, purpose: str, columns: Sequence[str] = (),
                   user: Optional[str] = None) -> None:
        self.store.append_rows(ACCESS_LOG, [{"dataset": dataset, "accessed_by": user or self.ctx.user,
                                             "purpose": purpose, "columns": ",".join(columns),
                                             "accessed_at": utcnow()}], ACCESS_SCHEMA)

    def read_with_audit(self, table: str, purpose: str, columns: Sequence[str] = ()) -> DataFrame:
        self.log_access(table, purpose, columns)
        df = self.ctx.store.read(table)
        return df.select(*columns) if columns else df

    def access_report(self, dataset: Optional[str] = None) -> DataFrame:
        df = self.store.read_or_empty(ACCESS_LOG, ACCESS_SCHEMA)
        df = df.where(F.col("dataset") == dataset) if dataset else df
        return df.groupBy("dataset", "accessed_by", "purpose").agg(
            F.count("*").alias("accesses"), F.max("accessed_at").alias("last_access"))

    # --------------------------------------------------------- retention
    def apply_retention(self, policy: RetentionPolicy, now=None) -> int:
        now = now or utcnow()
        cutoff = now - timedelta(days=policy.keep_days)
        store = self.ctx.store
        df = store.read(policy.table)
        old = F.col(policy.date_column) < F.lit(cutoff).cast(df.schema[policy.date_column].dataType)
        n = df.where(old).count()
        if n:
            if store.is_delta:
                self.ctx.spark.sql(f"DELETE FROM {store.sql_ref(policy.table)} WHERE "
                                   f"`{policy.date_column}` < '{cutoff:%Y-%m-%d %H:%M:%S}'")
            else:
                store.overwrite(df.where(~old | F.col(policy.date_column).isNull()), policy.table)
            self.record_custody(policy.table, "RETENTION_PURGE",
                                details={"rows": n, "cutoff": cutoff, "keep_days": policy.keep_days})
        return n

    def erase_subject(self, key_column: str, key_value, tables: Sequence[str],
                      reason: str = "data subject erasure request") -> Dict[str, int]:
        """Delete every row for a data subject; proof is kept in the custody chain
        (only a hash of the key is logged, never the personal data)."""
        store = self.ctx.store
        counts = {}
        for t in tables:
            df = store.read(t)
            if key_column not in df.columns:
                continue
            hit = F.col(key_column) == F.lit(key_value)
            n = df.where(hit).count()
            if n:
                store.overwrite(df.where(~hit | F.col(key_column).isNull()), t)
            counts[t] = n
        subject_hash = hashlib.sha256(str(key_value).encode()).hexdigest()
        self.record_custody(",".join(tables), "ERASURE", details={"subject_hash": subject_hash,
                                                                 "reason": reason, "rows": counts})
        return counts
