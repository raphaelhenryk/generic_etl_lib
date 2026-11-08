"""Subsystem 24 – Recovery and Restart System.

* **Checkpointing** – each task's completion is recorded per *run id*, so a
  restarted job skips tasks that already succeeded and resumes at the
  failure point.
* **Rollback** – restore points capture the Delta version of every target
  table before a job writes; on failure the tables are restored atomically
  to their pre-job state (``RESTORE TABLE ... TO VERSION AS OF``). On
  non-Delta storage a physical snapshot copy is used instead.
* **Graceful halt** – a halt flag that long-running loops check.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow
from ..core.store import materialize

CHECKPOINT_TABLE = "etl_checkpoint"
CHECKPOINT_SCHEMA = T.StructType([
    T.StructField("job_name", T.StringType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("task_name", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("recorded_at", T.TimestampType()),
])


class JobHaltRequested(RuntimeError):
    pass


class RecoveryManager:
    def __init__(self, ctx: EtlContext, run_id: Optional[str] = None) -> None:
        self.ctx = ctx
        self.run_id = run_id or ctx.params.get("run_id") or ctx.batch_id
        self._restore_points: Dict[str, Optional[int]] = {}
        self._snapshots: Dict[str, str] = {}
        self._halt = False

    # ------------------------------------------------------- checkpoints
    def mark(self, task_name: str, status: str = "SUCCEEDED") -> None:
        self.ctx.meta_store.append_rows(CHECKPOINT_TABLE, [{
            "job_name": self.ctx.job_name, "run_id": self.run_id, "task_name": task_name, "status": status,
            "batch_id": self.ctx.batch_id, "recorded_at": utcnow()}], CHECKPOINT_SCHEMA)

    def completed_tasks(self) -> List[str]:
        store = self.ctx.meta_store
        if not store.exists(CHECKPOINT_TABLE):
            return []
        df = store.read(CHECKPOINT_TABLE).where(
            (F.col("job_name") == self.ctx.job_name) & (F.col("run_id") == self.run_id))
        from pyspark.sql.window import Window

        w = Window.partitionBy("task_name").orderBy(F.desc("recorded_at"))
        last = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
        return [r["task_name"] for r in last.where(F.col("status") == "SUCCEEDED").collect()]

    def is_done(self, task_name: str) -> bool:
        return task_name in self.completed_tasks()

    def reset(self, task_name: Optional[str] = None) -> None:
        """Force tasks (or the whole run) to re-execute on the next restart."""
        tasks = [task_name] if task_name else self.completed_tasks()
        for t in tasks:
            self.mark(t, "RESET")

    # ---------------------------------------------------------- rollback
    def capture_restore_points(self, tables: Sequence[str]) -> Dict[str, Optional[int]]:
        store = self.ctx.store
        for t in tables:
            if not store.exists(t):
                self._restore_points[t] = None  # created by this job -> drop on rollback
            elif store.is_delta:
                self._restore_points[t] = store.current_version(t)
            else:
                snap = f"{t}__restore_{self.ctx.batch_id[:8]}"
                store.write(materialize(store.read(t)), snap, mode="overwrite")
                self._snapshots[t] = snap
                self._restore_points[t] = -1
        return dict(self._restore_points)

    def rollback(self) -> List[str]:
        store = self.ctx.store
        restored = []
        for t, version in self._restore_points.items():
            if version is None:
                if store.exists(t):
                    store.drop(t)
            elif store.is_delta:
                if store.current_version(t) != version:
                    store.restore(t, version)
            else:
                store.overwrite(store.read(self._snapshots[t]), t)
            restored.append(t)
        self.discard_restore_points()
        return restored

    def discard_restore_points(self) -> None:
        for snap in self._snapshots.values():
            try:
                self.ctx.store.drop(snap)
            except Exception:
                pass
        self._snapshots.clear()
        self._restore_points.clear()

    # -------------------------------------------------------------- halt
    def request_halt(self) -> None:
        self._halt = True

    def check_halt(self) -> None:
        if self._halt:
            raise JobHaltRequested(f"halt requested for {self.ctx.job_name}")
