"""Subsystem 29 – Lineage and Dependency Analyzer.

Captures table-level and column-level edges (source -> target with the
transformation that links them) and answers both questions Kimball poses:

* **lineage** (backward): where did this report / column come from?
* **dependency / impact** (forward): what breaks if this source changes?

Edges can be registered explicitly or inferred from a DataFrame's physical
plan (input files / tables). On Unity Catalog, system lineage tables can be
imported with :meth:`LineageAnalyzer.import_unity_catalog`.
"""
from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

LINEAGE_TABLE = "etl_lineage_edge"
LINEAGE_SCHEMA = T.StructType([
    T.StructField("source", T.StringType()),
    T.StructField("source_column", T.StringType()),
    T.StructField("target", T.StringType()),
    T.StructField("target_column", T.StringType()),
    T.StructField("transformation", T.StringType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("recorded_at", T.TimestampType()),
])


def _node(table: str, column: Optional[str]) -> str:
    return f"{table}.{column}" if column else table


class LineageAnalyzer:
    def __init__(self, ctx: EtlContext) -> None:
        self.ctx = ctx

    # --------------------------------------------------------- capture
    def record(self, source: str, target: str, transformation: str = "",
               column_map: Optional[Dict[str, Sequence[str]]] = None) -> int:
        """Register ``source -> target``; ``column_map`` = {target_col: [source_cols]}."""
        now = utcnow()
        base = {"source": source, "target": target, "transformation": transformation,
                "job_name": self.ctx.job_name, "batch_id": self.ctx.batch_id, "recorded_at": now}
        rows = [dict(base, source_column=None, target_column=None)]
        for tcol, scols in (column_map or {}).items():
            for scol in ([scols] if isinstance(scols, str) else scols):
                rows.append(dict(base, source_column=scol, target_column=tcol))
        self.ctx.meta_store.append_rows(LINEAGE_TABLE, rows, LINEAGE_SCHEMA)
        return len(rows)

    @staticmethod
    def infer_sources(df: DataFrame) -> List[str]:
        """Best effort: tables / paths read by a DataFrame's plan."""
        sources: Set[str] = set()
        try:
            for f in df.inputFiles():
                sources.add(re.sub(r"/part-[^/]+$", "", f))
        except Exception:
            pass
        try:
            plan = df._jdf.queryExecution().analyzed().toString()
            sources |= set(re.findall(r"SubqueryAlias (?:spark_catalog\.)?([\w`.]+)", plan))
        except Exception:
            pass
        return sorted(sources)

    def record_dataframe(self, df: DataFrame, target: str, transformation: str = "") -> List[str]:
        srcs = self.infer_sources(df)
        for s in srcs:
            self.record(s, target, transformation)
        return srcs

    def import_unity_catalog(self, since: Optional[str] = None) -> int:
        """Load edges from ``system.access.table_lineage`` (Databricks UC)."""
        q = ("SELECT source_table_full_name AS source, target_table_full_name AS target, "
             "entity_type AS transformation FROM system.access.table_lineage "
             "WHERE source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL")
        if since:
            q += f" AND event_time >= '{since}'"
        df = self.ctx.spark.sql(q).distinct()
        rows = [r.asDict() for r in df.collect()]
        for r in rows:
            self.record(r["source"], r["target"], r["transformation"] or "unity_catalog")
        return len(rows)

    # --------------------------------------------------------- analysis
    def edges(self) -> DataFrame:
        return self.ctx.meta_store.read_or_empty(LINEAGE_TABLE, LINEAGE_SCHEMA)

    def _graph(self, columns: bool) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        df = self.edges()
        df = df.where(F.col("target_column").isNotNull()) if columns else df.where(F.col("target_column").isNull())
        fwd: Dict[str, Set[str]] = defaultdict(set)
        bwd: Dict[str, Set[str]] = defaultdict(set)
        for r in df.select("source", "source_column", "target", "target_column").distinct().collect():
            s = _node(r["source"], r["source_column"] if columns else None)
            t = _node(r["target"], r["target_column"] if columns else None)
            fwd[s].add(t)
            bwd[t].add(s)
        return fwd, bwd

    @staticmethod
    def _walk(graph: Dict[str, Set[str]], start: str, max_depth: int) -> Dict[str, int]:
        seen: Dict[str, int] = {}
        q = deque([(start, 0)])
        while q:
            node, d = q.popleft()
            if d >= max_depth:
                continue
            for nxt in graph.get(node, ()):
                if nxt not in seen:
                    seen[nxt] = d + 1
                    q.append((nxt, d + 1))
        return seen

    def lineage(self, node: str, column: bool = False, max_depth: int = 50) -> Dict[str, int]:
        """Upstream nodes of ``node`` with their distance."""
        _, bwd = self._graph(column)
        return self._walk(bwd, node, max_depth)

    def impact(self, node: str, column: bool = False, max_depth: int = 50) -> Dict[str, int]:
        """Downstream (dependent) nodes of ``node`` with their distance."""
        fwd, _ = self._graph(column)
        return self._walk(fwd, node, max_depth)

    def roots(self) -> List[str]:
        fwd, bwd = self._graph(False)
        return sorted(n for n in fwd if not bwd.get(n))

    def to_mermaid(self, column: bool = False) -> str:
        fwd, _ = self._graph(column)
        safe = lambda n: re.sub(r"[^A-Za-z0-9_]", "_", n)  # noqa: E731
        lines = ["graph LR"]
        for s, targets in sorted(fwd.items()):
            for t in sorted(targets):
                lines.append(f'  {safe(s)}["{s}"] --> {safe(t)}["{t}"]')
        return "\n".join(lines)
