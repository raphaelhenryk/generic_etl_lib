"""Subsystem 20 – OLAP Cube Builder.

Spark has no MOLAP engine, so the "cube" is materialised relationally:
``CUBE`` / ``ROLLUP`` / ``GROUPING SETS`` aggregates with a ``grouping_id``
and per-level labels, plus an exported **semantic model** (JSON) describing
dimensions, hierarchies and measures that external OLAP / semantic-layer
tools (Power BI, SSAS Tabular, AtScale, Databricks metric views...) can
import.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext

ALL_MEMBER = "(All)"


@dataclass
class CubeDimension:
    name: str
    table: str
    key: str
    attributes: List[str]
    hierarchies: Dict[str, List[str]] = field(default_factory=dict)  # name -> levels top..bottom


@dataclass
class CubeMeasure:
    name: str
    column: str
    aggregation: str = "sum"
    format: Optional[str] = None


@dataclass
class CubeModel:
    name: str
    fact_table: str
    dimensions: List[CubeDimension]
    measures: List[CubeMeasure]
    fact_keys: Dict[str, str] = field(default_factory=dict)  # dimension -> fact FK column

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


class CubeBuilder:
    def __init__(self, ctx: EtlContext, model: CubeModel) -> None:
        self.ctx = ctx
        self.model = model

    def star(self) -> DataFrame:
        """Fact joined to all dimensions (denormalised view used to build the cube)."""
        store = self.ctx.store
        df = store.read(self.model.fact_table)
        for d in self.model.dimensions:
            fk = self.model.fact_keys.get(d.name, d.key)
            dim = store.read(d.table).select(F.col(d.key).alias(fk), *d.attributes)
            df = df.join(dim, fk, "left")
        return df

    def build(self, levels: Sequence[str], mode: str = "cube", grouping_sets: Optional[List[List[str]]] = None,
              table: Optional[str] = None, label_all: bool = True) -> DataFrame:
        """Materialise the cube over ``levels`` (dimension attributes).

        ``mode``: ``cube`` (all 2^n combinations), ``rollup`` (hierarchical)
        or ``grouping_sets`` (explicit list).
        """
        star = self.star()
        aggs = [F.expr(f"{m.aggregation}({m.column})").alias(m.name) for m in self.model.measures]
        aggs.append(F.grouping_id().alias("grouping_id"))
        lv = list(levels)
        if mode == "cube":
            out = star.cube(*lv).agg(*aggs)
        elif mode == "rollup":
            out = star.rollup(*lv).agg(*aggs)
        elif mode == "grouping_sets":
            if not grouping_sets:
                raise ValueError("grouping_sets mode needs grouping_sets")
            star.createOrReplaceTempView("__cube_star")
            sets = ", ".join("(" + ", ".join(f"`{c}`" for c in s) + ")" for s in grouping_sets)
            measures = ", ".join(f"{m.aggregation}(`{m.column}`) AS `{m.name}`" for m in self.model.measures)
            cols = ", ".join(f"`{c}`" for c in lv)
            out = self.ctx.spark.sql(
                f"SELECT {cols}, {measures}, grouping_id() AS grouping_id FROM __cube_star "
                f"GROUP BY {cols} GROUPING SETS ({sets})")
        else:
            raise ValueError(f"unknown mode {mode}")
        n = len(lv)
        for i, c in enumerate(lv):
            out = out.withColumn(f"{c}_is_all", F.expr(f"shiftright(grouping_id, {n - 1 - i}) & 1") == 1)
            if label_all:
                out = out.withColumn(c, F.when(F.col(f"{c}_is_all"), F.lit(ALL_MEMBER))
                                     .otherwise(F.col(c).cast("string")))
        if table:
            self.ctx.store.overwrite(out, table)
        return out

    @staticmethod
    def slice(cube: DataFrame, **members: str) -> DataFrame:
        """Pick one cell set: unspecified levels are taken at the (All) member."""
        level_cols = [c[:-7] for c in cube.columns if c.endswith("_is_all")]
        df = cube
        for lvl in level_cols:
            if lvl in members:
                df = df.where(F.col(lvl) == str(members[lvl]))
            else:
                df = df.where(F.col(f"{lvl}_is_all"))
        return df

    def export_model(self, path: Optional[str] = None) -> str:
        js = self.model.to_json()
        if path:
            self.ctx.spark.createDataFrame([(js,)], "model string").coalesce(1).write.mode("overwrite").text(path)
        return js
