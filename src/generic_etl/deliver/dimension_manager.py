"""Subsystem 17 – Dimension Manager System.

The single authority that owns *conformed* dimensions: it registers their
definitions, runs their SCD processing, publishes numbered versions and
replicates them to every fact table provider (other schemas, catalogs or
workspaces).
"""
from __future__ import annotations

import json
from dataclasses import asdict
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from ..core.store import TableStore
from .scd import SCDConfig, SCDManager, SCDResult

REGISTRY = "etl_dimension_registry"
PUBLICATIONS = "etl_dimension_publication"

REGISTRY_SCHEMA = T.StructType([
    T.StructField("dimension", T.StringType()),
    T.StructField("owner", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("scd_config", T.StringType()),
    T.StructField("registered_at", T.TimestampType()),
])
PUBLICATION_SCHEMA = T.StructType([
    T.StructField("dimension", T.StringType()),
    T.StructField("version", T.LongType()),
    T.StructField("storage_version", T.LongType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("target", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("published_at", T.TimestampType()),
])


class DimensionManager:
    def __init__(self, ctx: EtlContext) -> None:
        self.ctx = ctx
        self.configs: Dict[str, SCDConfig] = {}

    # ----------------------------------------------------------- registry
    def register(self, config: SCDConfig, owner: str = "", description: str = "") -> SCDManager:
        self.configs[config.dimension] = config
        cfg_json = json.dumps(asdict(config), default=list)
        row = [(config.dimension, owner, description, cfg_json, utcnow())]
        self.ctx.meta_store.upsert(self.ctx.spark.createDataFrame(row, REGISTRY_SCHEMA), REGISTRY, ["dimension"])
        return SCDManager(self.ctx, config)

    def registered(self) -> DataFrame:
        return self.ctx.meta_store.read_or_empty(REGISTRY, REGISTRY_SCHEMA)

    def manager(self, dimension: str) -> SCDManager:
        if dimension not in self.configs:
            row = self.registered().where(F.col("dimension") == dimension).first()
            if row is None:
                raise KeyError(f"dimension {dimension} is not registered")
            self.configs[dimension] = SCDConfig(**json.loads(row["scd_config"]))
        return SCDManager(self.ctx, self.configs[dimension])

    # ------------------------------------------------------------ process
    def process(self, dimension: str, incoming: DataFrame, as_of=None, audit_key: Optional[int] = None,
                publish: bool = True) -> SCDResult:
        result = self.manager(dimension).apply(incoming, as_of=as_of, audit_key=audit_key)
        if publish:
            self.publish(dimension)
        return result

    # ------------------------------------------------------------ publish
    def latest_version(self, dimension: str, target: str = "master") -> int:
        pubs = self.ctx.meta_store.read_or_empty(PUBLICATIONS, PUBLICATION_SCHEMA)
        v = pubs.where((F.col("dimension") == dimension) & (F.col("target") == target)).agg(F.max("version")).first()[0]
        return int(v or 0)

    def publish(self, dimension: str) -> int:
        """Record a new published version of the master dimension."""
        version = self.latest_version(dimension) + 1
        store = self.ctx.store
        rows = store.read(dimension).count()
        self._log(dimension, version, store.current_version(dimension), rows, "master")
        return version

    def replicate(self, dimension: str, targets: Dict[str, TableStore]) -> Dict[str, int]:
        """Copy the current master dimension to each subscriber store."""
        master = self.ctx.store.read(dimension)
        version = self.latest_version(dimension) or self.publish(dimension)
        out = {}
        for name, target in targets.items():
            target.overwrite(master, dimension)
            self._log(dimension, version, target.current_version(dimension), master.count(), name)
            out[name] = version
        return out

    def _log(self, dimension: str, version: int, storage_version, rows: int, target: str) -> None:
        self.ctx.meta_store.append_rows(PUBLICATIONS, [{
            "dimension": dimension, "version": version, "storage_version": storage_version,
            "row_count": rows, "target": target, "batch_id": self.ctx.batch_id, "published_at": utcnow()}],
            PUBLICATION_SCHEMA)

    def publications(self, dimension: Optional[str] = None) -> DataFrame:
        df = self.ctx.meta_store.read_or_empty(PUBLICATIONS, PUBLICATION_SCHEMA)
        return df.where(F.col("dimension") == dimension) if dimension else df
