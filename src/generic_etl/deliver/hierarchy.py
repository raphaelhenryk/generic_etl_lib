"""Subsystem 11 – Hierarchy Manager.

Handles the three hierarchy shapes found in dimensions:

* **fixed depth** (product -> brand -> category): flattened as columns,
  validated for many-to-one consistency
* **ragged / variable depth** with a known maximum: flattened into
  ``level_1 .. level_n`` columns, short branches filled down ("smeared")
* **unbounded recursive** (org charts, bills of material, account trees):
  a **hierarchy bridge table** – one row per ancestor/descendant pair with
  ``depth``, ``is_leaf`` and ``is_top`` flags – so plain SQL can roll up
  any sub-tree.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class HierarchyCycleError(ValueError):
    pass


class HierarchyManager:
    # ------------------------------------------------------- fixed depth
    @staticmethod
    def validate_fixed(df: DataFrame, levels: Sequence[str]) -> Dict[str, int]:
        """Each child level must roll up to exactly one parent value.

        ``levels`` are ordered from lowest (leaf) to highest. Returns the
        number of offending child values per level pair.
        """
        issues = {}
        for child, parent in zip(levels, levels[1:]):
            bad = df.groupBy(child).agg(F.countDistinct(parent).alias("n")).where("n > 1").count()
            if bad:
                issues[f"{child}->{parent}"] = bad
        return issues

    @staticmethod
    def flatten_fixed(df: DataFrame, levels: Sequence[str], path_column: str = "hierarchy_path",
                      separator: str = " > ") -> DataFrame:
        """Add a printable path (top > ... > leaf) for a fixed hierarchy."""
        return df.withColumn(path_column, F.concat_ws(separator, *[F.col(c) for c in reversed(levels)]))

    # ------------------------------------------------------ parent/child
    @staticmethod
    def _edges(df: DataFrame, child: str, parent: str) -> DataFrame:
        return df.select(F.col(child).alias("child"), F.col(parent).alias("parent")).distinct()

    @staticmethod
    def build_bridge(df: DataFrame, child: str, parent: str, max_depth: int = 50,
                     ancestor_col: str = "ancestor_key", descendant_col: str = "descendant_key") -> DataFrame:
        """Kimball hierarchy bridge from a parent/child (recursive) table.

        Output columns: ``ancestor_key``, ``descendant_key``, ``depth``
        (0 = self), ``is_leaf``, ``is_top``. Raises on cycles.
        """
        edges = HierarchyManager._edges(df, child, parent).cache()
        nodes = (edges.select(F.explode(F.array("child", "parent")).alias("node"))
                 .where(F.col("node").isNotNull()).distinct())
        level0 = nodes.select(F.col("node").alias("anc"), F.col("node").alias("desc"), F.lit(0).alias("depth"))
        level0 = level0.cache()
        levels = [level0]
        frontier = level0
        real_edges = edges.where(F.col("parent").isNotNull())
        for _ in range(max_depth):
            nxt = (frontier.alias("f").join(real_edges.alias("e"), F.col("f.desc") == F.col("e.parent"))
                   .select(F.col("f.anc").alias("anc"), F.col("e.child").alias("desc"),
                           (F.col("f.depth") + 1).alias("depth")))
            nxt = nxt.cache()  # (localCheckpoint + union hits a Catalyst constraint bug in Spark 4.0)
            if nxt.where(F.col("anc") == F.col("desc")).limit(1).count():
                raise HierarchyCycleError("cycle detected in hierarchy")
            if nxt.limit(1).count() == 0:
                break
            levels.append(nxt)
            frontier = nxt
        else:
            raise HierarchyCycleError(f"hierarchy deeper than max_depth={max_depth} (or cyclic)")
        paths = levels[0]
        for lv in levels[1:]:
            paths = paths.unionByName(lv)
        paths = paths.cache()
        parents = real_edges.select(F.col("parent").alias("n")).distinct().withColumn("has_child", F.lit(True))
        children = real_edges.select(F.col("child").alias("n")).distinct().withColumn("has_parent", F.lit(True))
        out = (paths.join(parents, F.col("desc") == F.col("n"), "left").drop("n")
               .join(children, F.col("anc") == F.col("n"), "left").drop("n"))
        return out.select(
            F.col("anc").alias(ancestor_col), F.col("desc").alias(descendant_col), "depth",
            F.col("has_child").isNull().alias("is_leaf"),
            F.col("has_parent").isNull().alias("is_top"),
        )

    @staticmethod
    def flatten_ragged(df: DataFrame, child: str, parent: str, max_depth: int = 10,
                       label: Optional[str] = None, level_prefix: str = "level_",
                       fill_down: bool = True) -> DataFrame:
        """Flatten a ragged hierarchy to ``level_1`` (top) .. ``level_n`` columns.

        With ``fill_down`` short branches repeat their lowest value in the
        remaining levels so every leaf can be grouped at every level.
        """
        lab = label or child
        nodes = df.select(F.col(child).alias("node"), F.col(parent).alias("parent"), F.col(lab).alias("label"))
        lookup = nodes.select(F.col("node").alias("__n"), F.col("parent").alias("__p"), F.col("label").alias("__l"))
        cur = nodes.select("node", F.array("label").alias("path"), F.col("parent").alias("next"))
        for _ in range(max_depth):
            if cur.where(F.col("next").isNotNull()).limit(1).count() == 0:
                break
            cur = (cur.join(lookup, F.col("next") == F.col("__n"), "left")
                   .select("node",
                           F.when(F.col("__n").isNotNull(), F.concat(F.array("__l"), F.col("path")))
                           .otherwise(F.col("path")).alias("path"),
                           F.col("__p").alias("next"))
                   .localCheckpoint(eager=True))
        else:
            if cur.where(F.col("next").isNotNull()).limit(1).count():
                raise HierarchyCycleError(f"hierarchy deeper than max_depth={max_depth} (or cyclic)")
        depth = F.size("path")
        cols = []
        for i in range(max_depth):
            v = F.when(depth > i, F.col("path")[i])
            if fill_down:
                v = F.coalesce(v, F.element_at("path", -1))
            cols.append(v.alias(f"{level_prefix}{i + 1}"))
        flat = cur.select(F.col("node").alias(child), depth.alias("depth"), *cols)
        return flat

    @staticmethod
    def rollup_with_bridge(fact: DataFrame, bridge: DataFrame, fact_key: str, measures: Dict[str, str],
                           ancestor_col: str = "ancestor_key", descendant_col: str = "descendant_key") -> DataFrame:
        """Aggregate fact measures to every ancestor via the bridge table."""
        j = bridge.join(fact, F.col(descendant_col) == F.col(fact_key))
        return j.groupBy(ancestor_col).agg(*[F.expr(f"{fn}({c})").alias(f"{fn}_{c}") for c, fn in measures.items()])
