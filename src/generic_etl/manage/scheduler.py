"""Subsystem 22 – Job Scheduler.

A dependency-aware job runner: tasks form a DAG, run in topological waves
(optionally in parallel), with retries, pre-conditions (dependency checks
such as "source file arrived" or "upstream table loaded today"), restart from
the last checkpoint, monitoring and escalation hooks.

For production scheduling on Databricks the same job definition can be
exported to a Databricks Workflows (Jobs API 2.1) specification with
:func:`to_databricks_job`.
"""
from __future__ import annotations

import inspect
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from ..core.context import EtlContext

SUCCEEDED, FAILED, SKIPPED, UPSTREAM_FAILED, RESTORED = (
    "SUCCEEDED", "FAILED", "SKIPPED", "UPSTREAM_FAILED", "SKIPPED_ALREADY_DONE")


NON_CRITICAL_FAILED = "FAILED_NON_CRITICAL"


class DependencyCycleError(ValueError):
    pass


@dataclass
class Task:
    name: str
    fn: Callable[..., Any]
    depends_on: Sequence[str] = field(default_factory=list)
    retries: int = 0
    retry_delay_sec: float = 0.0
    condition: Optional[Callable[[EtlContext], bool]] = None  # skip when False
    critical: bool = True          # failure fails the job
    description: str = ""
    databricks: Dict[str, Any] = field(default_factory=dict)  # extra Jobs API task fields


@dataclass
class JobResult:
    job_name: str
    batch_id: str
    statuses: Dict[str, str]
    outputs: Dict[str, Any]
    errors: Dict[str, BaseException]

    @property
    def succeeded(self) -> bool:
        return not any(s in (FAILED, UPSTREAM_FAILED) for s in self.statuses.values())


class Job:
    def __init__(self, name: str, schedule: Optional[str] = None, timezone: str = "UTC") -> None:
        self.name = name
        self.schedule = schedule  # quartz cron, e.g. "0 0 2 * * ?"
        self.timezone = timezone
        self.tasks: Dict[str, Task] = {}

    def add(self, task: Task) -> Task:
        if task.name in self.tasks:
            raise ValueError(f"duplicate task {task.name}")
        self.tasks[task.name] = task
        return task

    def task(self, name: Optional[str] = None, depends_on: Sequence[str] = (), **kw) -> Callable:
        """Decorator: ``@job.task(depends_on=["extract"])``."""
        def deco(fn):
            self.add(Task(name or fn.__name__, fn, list(depends_on), **kw))
            return fn
        return deco

    def waves(self) -> List[List[str]]:
        """Topological levels; tasks in one wave are mutually independent."""
        for t in self.tasks.values():
            for d in t.depends_on:
                if d not in self.tasks:
                    raise ValueError(f"task {t.name} depends on unknown task {d}")
        remaining = {n: set(t.depends_on) for n, t in self.tasks.items()}
        done: set = set()
        out = []
        while remaining:
            ready = sorted(n for n, deps in remaining.items() if deps <= done)
            if not ready:
                raise DependencyCycleError(f"cycle among {sorted(remaining)}")
            out.append(ready)
            done |= set(ready)
            for n in ready:
                remaining.pop(n)
        return out

    def downstream(self, name: str) -> List[str]:
        children = {n for n, t in self.tasks.items() if name in t.depends_on}
        result = set(children)
        for c in children:
            result |= set(self.downstream(c))
        return sorted(result)


def _call(fn: Callable, ctx: EtlContext, outputs: Dict[str, Any]):
    params = inspect.signature(fn).parameters
    if len(params) >= 2:
        return fn(ctx, outputs)
    if len(params) == 1:
        return fn(ctx)
    return fn()


class JobScheduler:
    def __init__(self, ctx: EtlContext, monitor=None, recovery=None, escalation=None,
                 max_parallel: int = 1) -> None:
        self.ctx = ctx
        self.monitor = monitor
        self.recovery = recovery
        self.escalation = escalation
        self.max_parallel = max_parallel

    def _run_task(self, task: Task, outputs: Dict[str, Any]):
        attempt = 0
        while True:
            attempt += 1
            try:
                if self.monitor:
                    with self.monitor.track(task.name, attempt) as run:
                        value = _call(task.fn, self.ctx, outputs)
                        if isinstance(value, dict):
                            run.set(**{k: v for k, v in value.items() if k in ("rows_in", "rows_out", "rows_rejected")})
                        elif isinstance(value, int) and not isinstance(value, bool):
                            run.set(rows_out=value)
                else:
                    value = _call(task.fn, self.ctx, outputs)
                return value
            except Exception:
                if attempt > task.retries:
                    raise
                time.sleep(task.retry_delay_sec * attempt)

    def run(self, job: Job, only: Optional[Sequence[str]] = None) -> JobResult:
        statuses: Dict[str, str] = {}
        outputs: Dict[str, Any] = {}
        errors: Dict[str, BaseException] = {}
        done_before = set(self.recovery.completed_tasks()) if self.recovery else set()
        selected = set(only) if only else set(job.tasks)

        def execute(name: str):
            task = job.tasks[name]
            if name not in selected:
                return name, SKIPPED, None, None
            if any(statuses.get(d) in (FAILED, UPSTREAM_FAILED) for d in task.depends_on):
                return name, UPSTREAM_FAILED, None, None
            if any(statuses.get(d) == NON_CRITICAL_FAILED or (statuses.get(d) == SKIPPED and d in selected)
                   for d in task.depends_on):
                return name, SKIPPED, None, None
            if name in done_before:
                return name, RESTORED, None, None
            if task.condition is not None and not task.condition(self.ctx):
                return name, SKIPPED, None, None
            try:
                value = self._run_task(task, outputs)
                if self.recovery:
                    self.recovery.mark(name)
                return name, SUCCEEDED, value, None
            except Exception as exc:  # noqa: BLE001
                if self.escalation:
                    self.escalation.on_failure(name, exc, "CRITICAL" if task.critical else "ERROR")
                if self.recovery:
                    self.recovery.mark(name, FAILED)
                return name, FAILED, None, exc

        for wave in job.waves():
            if self.max_parallel > 1 and len(wave) > 1:
                with ThreadPoolExecutor(max_workers=self.max_parallel) as pool:
                    results = list(pool.map(execute, wave))
            else:
                results = [execute(n) for n in wave]
            for name, status, value, exc in results:
                statuses[name] = status
                if value is not None:
                    outputs[name] = value
                if exc is not None:
                    errors[name] = exc
                    if not job.tasks[name].critical:
                        statuses[name] = NON_CRITICAL_FAILED
        return JobResult(job.name, self.ctx.batch_id, statuses, outputs, errors)


# ---------------------------------------------------------- dependency checks
def table_loaded_since(ctx: EtlContext, table: str, timestamp_column: str, since: str) -> Callable[[EtlContext], bool]:
    """Pre-condition: ``table`` has rows with ``timestamp_column >= since``."""
    from pyspark.sql import functions as F

    def check(_ctx: EtlContext) -> bool:
        if not ctx.store.exists(table):
            return False
        return ctx.store.read(table).where(F.col(timestamp_column) >= F.lit(since)).limit(1).count() > 0
    return check


def path_exists(ctx: EtlContext, path: str) -> Callable[[EtlContext], bool]:
    """Pre-condition: a file / folder (e.g. a ``_SUCCESS`` marker) exists."""
    def check(_ctx: EtlContext) -> bool:
        jvm = ctx.spark._jvm
        p = jvm.org.apache.hadoop.fs.Path(path)
        return p.getFileSystem(ctx.spark._jsc.hadoopConfiguration()).exists(p)
    return check


def to_databricks_job(job: Job, task_spec: Callable[[Task], Dict[str, Any]],
                      job_cluster_key: Optional[str] = None, email_on_failure: Sequence[str] = (),
                      max_concurrent_runs: int = 1) -> Dict[str, Any]:
    """Export to a Databricks Jobs API 2.1 payload.

    ``task_spec(task)`` returns the executable part of each task, e.g.
    ``{"python_wheel_task": {"package_name": "my_etl", "entry_point": "run",
    "parameters": [task.name]}}`` or ``{"notebook_task": {...}}``.
    """
    tasks = []
    for wave in job.waves():
        for name in wave:
            t = job.tasks[name]
            spec = {"task_key": name, "depends_on": [{"task_key": d} for d in t.depends_on],
                    "max_retries": t.retries, "min_retry_interval_millis": int(t.retry_delay_sec * 1000),
                    "description": t.description or name}
            if job_cluster_key:
                spec["job_cluster_key"] = job_cluster_key
            spec.update(task_spec(t))
            spec.update(t.databricks)
            tasks.append(spec)
    payload: Dict[str, Any] = {"name": job.name, "tasks": tasks, "max_concurrent_runs": max_concurrent_runs}
    if job.schedule:
        payload["schedule"] = {"quartz_cron_expression": job.schedule, "timezone_id": job.timezone,
                               "pause_status": "UNPAUSED"}
    if email_on_failure:
        payload["email_notifications"] = {"on_failure": list(email_on_failure)}
    return payload
