"""Subsystem 26 – Version Migration System.

Promotes ETL pipelines through environments (dev -> test -> prod):

* per-environment configuration (catalog / schema / paths / parameters)
* promotion **gates** – callables that must pass (tests, data-quality
  thresholds, code version registered, approvals)
* **schema compatibility** check of each table between environments
* optional DDL generation to evolve target schemas
* every promotion is logged with who, what version and gate results
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, current_user, utcnow
from ..core.store import TableStore

PROMOTION_LOG = "etl_promotion_log"
PROMOTION_SCHEMA = T.StructType([
    T.StructField("pipeline", T.StringType()),
    T.StructField("version_tag", T.StringType()),
    T.StructField("from_env", T.StringType()),
    T.StructField("to_env", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("gate_results", T.MapType(T.StringType(), T.StringType())),
    T.StructField("promoted_by", T.StringType()),
    T.StructField("promoted_at", T.TimestampType()),
])

DEFAULT_ORDER = ("dev", "test", "prod")


@dataclass
class EnvironmentConfig:
    name: str
    database: Optional[str] = None
    base_path: Optional[str] = None
    meta_database: Optional[str] = None
    params: Dict[str, object] = field(default_factory=dict)
    fmt: Optional[str] = None

    def store(self, spark: SparkSession) -> TableStore:
        return TableStore(spark, database=self.database, base_path=self.base_path, fmt=self.fmt)

    def context(self, spark: SparkSession, job_name: str, **kw) -> EtlContext:
        meta = TableStore(spark, database=self.meta_database, fmt=self.fmt) if self.meta_database else None
        return EtlContext(spark=spark, store=self.store(spark), job_name=job_name, environment=self.name,
                          params=dict(self.params), meta_store=meta, **kw)


class PromotionError(RuntimeError):
    pass


Gate = Callable[[], Tuple[bool, str]]


def schema_compatibility(source: T.StructType, target: T.StructType) -> Dict[str, List[str]]:
    """Compare schemas: new columns are additive, removed/retyped break consumers."""
    s = {f.name: f.dataType.simpleString() for f in source.fields}
    t = {f.name: f.dataType.simpleString() for f in target.fields}
    return {"added": sorted(set(s) - set(t)), "removed": sorted(set(t) - set(s)),
            "type_changed": sorted(c for c in set(s) & set(t) if s[c] != t[c])}


def evolution_ddl(table_ref: str, diff: Dict[str, List[str]], source: T.StructType) -> List[str]:
    types = {f.name: f.dataType.simpleString() for f in source.fields}
    return [f"ALTER TABLE {table_ref} ADD COLUMNS (`{c}` {types[c]})" for c in diff["added"]]


class MigrationManager:
    def __init__(self, spark: SparkSession, environments: Sequence[EnvironmentConfig],
                 order: Sequence[str] = DEFAULT_ORDER, log_store: Optional[TableStore] = None) -> None:
        self.spark = spark
        self.envs = {e.name: e for e in environments}
        self.order = list(order)
        self.log_store = log_store or self.envs[self.order[-1]].store(spark)

    def next_env(self, env: str) -> str:
        i = self.order.index(env)
        if i + 1 >= len(self.order):
            raise PromotionError(f"{env} is the last environment")
        return self.order[i + 1]

    def compare_tables(self, tables: Sequence[str], from_env: str, to_env: str) -> Dict[str, Dict[str, List[str]]]:
        src, dst = self.envs[from_env].store(self.spark), self.envs[to_env].store(self.spark)
        out = {}
        for t in tables:
            if not src.exists(t):
                continue
            if not dst.exists(t):
                out[t] = {"added": src.read(t).columns, "removed": [], "type_changed": [], "new_table": ["yes"]}
            else:
                out[t] = schema_compatibility(src.read(t).schema, dst.read(t).schema)
        return out

    def promote(self, pipeline: str, version_tag: str, from_env: str, to_env: Optional[str] = None,
                gates: Optional[Dict[str, Gate]] = None, tables: Sequence[str] = (),
                allow_breaking: bool = False, apply_ddl: bool = False) -> Dict[str, object]:
        to_env = to_env or self.next_env(from_env)
        if self.order.index(to_env) != self.order.index(from_env) + 1:
            raise PromotionError(f"cannot skip environments: {from_env} -> {to_env}")
        results: Dict[str, str] = {}
        ok = True
        for name, gate in (gates or {}).items():
            try:
                passed, msg = gate()
            except Exception as exc:  # noqa: BLE001
                passed, msg = False, f"{type(exc).__name__}: {exc}"
            results[name] = ("PASS: " if passed else "FAIL: ") + str(msg)
            ok = ok and passed
        diffs = self.compare_tables(tables, from_env, to_env)
        for t, d in diffs.items():
            breaking = d["removed"] or d["type_changed"]
            results[f"schema:{t}"] = ("BREAKING " if breaking else "OK ") + str(d)
            if breaking and not allow_breaking:
                ok = False
        status = "PROMOTED" if ok else "REJECTED"
        if ok and apply_ddl:
            src, dst = self.envs[from_env].store(self.spark), self.envs[to_env].store(self.spark)
            for t, d in diffs.items():
                if "new_table" in d or not d["added"]:
                    continue
                for stmt in evolution_ddl(dst.sql_ref(t), d, src.read(t).schema):
                    self.spark.sql(stmt)
        self.log_store.append_rows(PROMOTION_LOG, [{
            "pipeline": pipeline, "version_tag": version_tag, "from_env": from_env, "to_env": to_env,
            "status": status, "gate_results": results, "promoted_by": current_user(), "promoted_at": utcnow()}],
            PROMOTION_SCHEMA)
        if not ok:
            raise PromotionError(f"promotion of {pipeline}@{version_tag} to {to_env} rejected: {results}")
        return {"pipeline": pipeline, "version": version_tag, "to_env": to_env, "gates": results}

    def deployed_version(self, pipeline: str, env: str) -> Optional[str]:
        df = self.log_store.read_or_empty(PROMOTION_LOG, PROMOTION_SCHEMA)
        row = (df.where((F.col("pipeline") == pipeline) & (F.col("to_env") == env) & (F.col("status") == "PROMOTED"))
               .orderBy(F.desc("promoted_at")).first())
        return row["version_tag"] if row else None
