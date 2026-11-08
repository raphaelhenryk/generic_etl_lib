"""Subsystem 25 – Version Control System.

The ETL source itself belongs in Git (Databricks Repos / Git folders).
This module ties every *run* to the exact code that produced it:

* resolves the current Git commit / branch / dirty state (or Databricks
  Repos metadata) and the installed library version
* fingerprints pipeline modules (SHA-256 of their source) so silent edits
  of notebooks / modules are detected
* stores immutable *release snapshots* (``etl_code_version``) and diffs them
"""
from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import os
import subprocess
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

CODE_VERSION_TABLE = "etl_code_version"
CODE_VERSION_SCHEMA = T.StructType([
    T.StructField("pipeline", T.StringType()),
    T.StructField("version_tag", T.StringType()),
    T.StructField("git_commit", T.StringType()),
    T.StructField("git_branch", T.StringType()),
    T.StructField("git_dirty", T.BooleanType()),
    T.StructField("module_hashes", T.MapType(T.StringType(), T.StringType())),
    T.StructField("library_version", T.StringType()),
    T.StructField("registered_by", T.StringType()),
    T.StructField("registered_at", T.TimestampType()),
])


def _git(args: List[str], cwd: Optional[str]) -> Optional[str]:
    try:
        out = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=10)
        return out.stdout.strip() if out.returncode == 0 else None
    except Exception:
        return None


def git_info(path: Optional[str] = None) -> Dict[str, object]:
    path = path or os.getcwd()
    commit = _git(["rev-parse", "HEAD"], path) or os.environ.get("GIT_COMMIT")
    branch = _git(["rev-parse", "--abbrev-ref", "HEAD"], path) or os.environ.get("GIT_BRANCH")
    status = _git(["status", "--porcelain"], path)
    return {"commit": commit, "branch": branch, "dirty": bool(status) if status is not None else None}


def module_fingerprint(module) -> str:
    """SHA-256 of a module's (or any object's) source code."""
    if isinstance(module, str):
        module = importlib.import_module(module)
    try:
        src = inspect.getsource(module)
    except (OSError, TypeError):
        src = repr(module)
    return hashlib.sha256(src.encode()).hexdigest()


class VersionControl:
    def __init__(self, ctx: EtlContext, pipeline: Optional[str] = None, repo_path: Optional[str] = None) -> None:
        self.ctx = ctx
        self.pipeline = pipeline or ctx.job_name
        self.repo_path = repo_path

    def snapshot(self, modules: Sequence = (), version_tag: Optional[str] = None) -> Dict[str, object]:
        """Register the code version of this pipeline and stamp it on the context."""
        from .. import __version__

        g = git_info(self.repo_path)
        hashes = {m if isinstance(m, str) else getattr(m, "__name__", repr(m)): module_fingerprint(m) for m in modules}
        tag = version_tag or (g["commit"][:12] if g["commit"] else
                              hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()[:12])
        rec = {"pipeline": self.pipeline, "version_tag": tag, "git_commit": g["commit"], "git_branch": g["branch"],
               "git_dirty": g["dirty"], "module_hashes": hashes, "library_version": __version__,
               "registered_by": self.ctx.user, "registered_at": utcnow()}
        self.ctx.meta_store.append_rows(CODE_VERSION_TABLE, [rec], CODE_VERSION_SCHEMA)
        self.ctx.code_version = tag
        return rec

    def history(self) -> DataFrame:
        df = self.ctx.meta_store.read_or_empty(CODE_VERSION_TABLE, CODE_VERSION_SCHEMA)
        return df.where(F.col("pipeline") == self.pipeline).orderBy(F.desc("registered_at"))

    def get(self, version_tag: str) -> Optional[Dict[str, object]]:
        row = self.history().where(F.col("version_tag") == version_tag).first()
        return row.asDict() if row else None

    def diff(self, old_tag: str, new_tag: str) -> Dict[str, List[str]]:
        a = (self.get(old_tag) or {}).get("module_hashes") or {}
        b = (self.get(new_tag) or {}).get("module_hashes") or {}
        return {"added": sorted(set(b) - set(a)), "removed": sorted(set(a) - set(b)),
                "changed": sorted(k for k in set(a) & set(b) if a[k] != b[k])}

    def detect_drift(self, modules: Sequence, version_tag: str) -> List[str]:
        """Modules whose current source differs from the registered version."""
        registered = (self.get(version_tag) or {}).get("module_hashes") or {}
        drift = []
        for m in modules:
            name = m if isinstance(m, str) else getattr(m, "__name__", repr(m))
            if registered.get(name) != module_fingerprint(m):
                drift.append(name)
        return drift
