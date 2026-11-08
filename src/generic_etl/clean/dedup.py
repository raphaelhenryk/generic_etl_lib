"""Subsystem 7 – Deduplication System.

Identifies records that represent the same real-world entity (customer,
product, supplier...) within or across sources and builds a *survivor*
("golden") record.

1. **standardise** the matching attributes (case, punctuation, whitespace)
2. **block** records into candidate groups to avoid an N² comparison
3. **match** pairs inside each block with exact and fuzzy comparators
   (Levenshtein, Soundex, token Jaccard) combined into a weighted score
4. **cluster** matching pairs transitively (connected components)
5. **survive** – choose attribute values for the golden record using
   per-column survivorship rules
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CLUSTER_ID = "_cluster_id"
IS_SURVIVOR = "_is_survivor"


def standardize(col: Column) -> Column:
    """Lower-case, strip accents/punctuation and collapse whitespace."""
    c = F.lower(F.trim(col.cast("string")))
    c = F.translate(c, "áàâãäéèêëíìîïóòôõöúùûüçñ", "aaaaaeeeeiiiiooooouuuucn")
    c = F.regexp_replace(c, r"[^a-z0-9 ]", " ")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


@dataclass
class MatchRule:
    """How to compare one attribute.

    ``method``: ``exact`` | ``levenshtein`` | ``soundex`` | ``jaccard`` | ``numeric``
    """

    column: str
    method: str = "exact"
    weight: float = 1.0
    standardize: bool = True
    tolerance: float = 0.0  # for ``numeric``: max relative difference

    def similarity(self, left: Column, right: Column) -> Column:
        if self.standardize and self.method != "numeric":
            left, right = standardize(left), standardize(right)
        if self.method == "exact":
            sim = F.when(left == right, 1.0).otherwise(0.0)
        elif self.method == "levenshtein":
            mx = F.greatest(F.length(left), F.length(right))
            sim = F.when(mx == 0, 1.0).otherwise(1 - F.levenshtein(left, right) / mx)
        elif self.method == "soundex":
            sim = F.when(F.soundex(left) == F.soundex(right), 1.0).otherwise(0.0)
        elif self.method == "jaccard":
            a, b = F.array_distinct(F.split(left, " ")), F.array_distinct(F.split(right, " "))
            union = F.size(F.array_union(a, b))
            sim = F.when(union == 0, 1.0).otherwise(F.size(F.array_intersect(a, b)) / union)
        elif self.method == "numeric":
            denom = F.greatest(F.abs(left), F.abs(right))
            sim = F.when(denom == 0, 1.0).otherwise(
                F.when(F.abs(left - right) / denom <= self.tolerance, 1.0).otherwise(0.0))
        else:
            raise ValueError(f"unknown match method {self.method}")
        return F.coalesce(sim.cast("double"), F.lit(0.0))


@dataclass
class Deduplicator:
    """Exact + fuzzy duplicate detection with survivorship.

    ``id_column`` must uniquely identify each input record.
    """

    id_column: str
    rules: Sequence[MatchRule] = field(default_factory=list)
    blocking_keys: Sequence[Column] = field(default_factory=list)
    threshold: float = 0.85
    max_iterations: int = 20

    # ------------------------------------------------------------- exact
    @staticmethod
    def exact(df: DataFrame, keys: Sequence[str], order_by: Sequence[Column] = (),
              standardize_keys: bool = True) -> DataFrame:
        """Cluster rows sharing the same (standardised) keys; mark the survivor."""
        key_exprs = [standardize(F.col(k)) if standardize_keys else F.col(k) for k in keys]
        clustered = df.withColumn(CLUSTER_ID, F.sha2(F.concat_ws("|", *[F.coalesce(k, F.lit("")) for k in key_exprs]), 256))
        w = Window.partitionBy(CLUSTER_ID).orderBy(*(list(order_by) or [F.lit(1)]))
        return clustered.withColumn(IS_SURVIVOR, F.row_number().over(w) == 1)

    # ------------------------------------------------------------- fuzzy
    def match_pairs(self, df: DataFrame) -> DataFrame:
        """Scored candidate pairs (``id_l``, ``id_r``, ``score``) above threshold."""
        if not self.rules:
            raise ValueError("fuzzy matching needs at least one MatchRule")
        cols = {r.column for r in self.rules}
        base = df.select(F.col(self.id_column).alias("_id"), *[F.col(c) for c in cols])
        blocks = list(self.blocking_keys) or [F.lit(1)]
        base = base.withColumn("_block", F.concat_ws("|", *[b.cast("string") for b in blocks]))
        l = base.select(*[F.col(c).alias(f"{c}__l") for c in base.columns])
        r = base.select(*[F.col(c).alias(f"{c}__r") for c in base.columns])
        pairs = l.join(r, (F.col("_block__l") == F.col("_block__r")) & (F.col("_id__l") < F.col("_id__r")))
        total_w = sum(x.weight for x in self.rules) or 1.0
        score = F.lit(0.0)
        for rule in self.rules:
            score = score + rule.similarity(F.col(f"{rule.column}__l"), F.col(f"{rule.column}__r")) * rule.weight
        pairs = pairs.withColumn("score", score / total_w)
        return pairs.where(F.col("score") >= self.threshold).select(
            F.col("_id__l").alias("id_l"), F.col("_id__r").alias("id_r"), "score")

    def cluster(self, df: DataFrame, pairs: Optional[DataFrame] = None) -> DataFrame:
        """Assign ``_cluster_id`` = smallest id in each connected component."""
        pairs = pairs if pairs is not None else self.match_pairs(df)
        ids = df.select(F.col(self.id_column).alias("id"))
        edges = pairs.select(F.col("id_l").alias("a"), F.col("id_r").alias("b"))
        edges = edges.unionByName(edges.select(F.col("b").alias("a"), F.col("a").alias("b"))).cache()
        labels = ids.withColumn("label", F.col("id"))
        for _ in range(self.max_iterations):
            prop = (edges.join(labels.withColumnRenamed("id", "a"), "a")
                    .groupBy(F.col("b").alias("id")).agg(F.min("label").alias("cand")))
            new = (labels.join(prop, "id", "left")
                   .select("id", F.least("label", F.coalesce("cand", "label")).alias("label")))
            new = new.localCheckpoint(eager=True) if hasattr(new, "localCheckpoint") else new.cache()
            changed = new.join(labels.withColumnRenamed("label", "old"), "id").where("label <> old").count()
            labels = new
            if changed == 0:
                break
        return df.join(labels.select(F.col("id").alias(self.id_column), F.col("label").alias(CLUSTER_ID)),
                       self.id_column, "left")

    def deduplicate(self, df: DataFrame, survivor_order: Sequence[Column] = ()) -> DataFrame:
        """Cluster with fuzzy rules and flag one survivor per cluster."""
        clustered = self.cluster(df)
        w = Window.partitionBy(CLUSTER_ID).orderBy(*(list(survivor_order) or [F.col(self.id_column)]))
        return clustered.withColumn(IS_SURVIVOR, F.row_number().over(w) == 1)

    # -------------------------------------------------------- survivorship
    @staticmethod
    def golden_records(clustered: DataFrame, rules: Dict[str, str], recency_column: Optional[str] = None,
                       source_priority: Optional[Dict[str, int]] = None, source_column: Optional[str] = None) -> DataFrame:
        """Build one golden record per cluster.

        ``rules`` maps a column to ``most_recent`` | ``most_frequent`` |
        ``longest`` | ``max`` | ``min`` | ``first_non_null`` | ``source_priority``.
        """
        aggs = [F.count(F.lit(1)).alias("_member_count")]
        rank_col = None
        if source_priority and source_column:
            mapping = F.create_map(*[x for k, v in source_priority.items() for x in (F.lit(k), F.lit(v))])
            clustered = clustered.withColumn("_src_rank", F.coalesce(mapping[F.col(source_column)], F.lit(10**6)))
            rank_col = "_src_rank"
        for col, rule in rules.items():
            c = F.col(col)
            if rule == "most_recent":
                if not recency_column:
                    raise ValueError("most_recent requires recency_column")
                aggs.append(F.max_by(c, F.when(c.isNotNull(), F.col(recency_column))).alias(col))
            elif rule == "longest":
                aggs.append(F.max_by(c, F.length(c.cast("string"))).alias(col))
            elif rule == "max":
                aggs.append(F.max(c).alias(col))
            elif rule == "min":
                aggs.append(F.min(c).alias(col))
            elif rule == "first_non_null":
                aggs.append(F.first(c, ignorenulls=True).alias(col))
            elif rule == "source_priority":
                if not rank_col:
                    raise ValueError("source_priority rule needs source_priority and source_column")
                aggs.append(F.min_by(c, F.when(c.isNotNull(), F.col(rank_col))).alias(col))
            elif rule == "most_frequent":
                aggs.append(F.mode(c).alias(col))
            else:
                raise ValueError(f"unknown survivorship rule {rule}")
        return clustered.groupBy(CLUSTER_ID).agg(*aggs)
