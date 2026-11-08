"""Subsystem 4 – Data Cleaning System (quality screens).

Kimball's *screens* are tests applied to data flowing through the pipeline.
Three families exist:

* **column screens**    – single-column tests (nulls, ranges, domains, formats)
* **structure screens** – relationships between columns / tables
  (uniqueness, referential integrity, parent-child consistency)
* **business rule screens** – complex multi-column business logic

Each screen declares a *severity* and an *action*:

* ``PASS``   – just record the error event
* ``FLAG``   – keep the row but tag it (``_dq_flags``) and lower its score
* ``FIX``    – keep the row and replace the bad value with a correction
* ``REJECT`` – route the row to the reject (suspense) set
* ``HALT``   – stop the batch (raise :class:`DataQualityHaltError`)

Every failure produces an *error event* row (Subsystem 5).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..core.context import EtlContext


class Severity:
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

    WEIGHT = {INFO: 0.0, WARN: 0.1, ERROR: 0.5, FATAL: 1.0}


class Action:
    PASS = "PASS"
    FLAG = "FLAG"
    FIX = "FIX"
    REJECT = "REJECT"
    HALT = "HALT"


class DataQualityHaltError(RuntimeError):
    """Raised when a HALT screen fails; the batch must not be loaded."""

    def __init__(self, message: str, result: "ScreenResult") -> None:
        super().__init__(message)
        self.result = result


ColumnOrFn = Union[Column, Callable[[DataFrame], Column], str]


def _resolve(expr: ColumnOrFn, df: DataFrame) -> Column:
    if isinstance(expr, str):
        return F.expr(expr)
    if callable(expr) and not isinstance(expr, Column):
        return expr(df)
    return expr


@dataclass
class Screen:
    """A row-level screen. ``failure`` evaluates to TRUE for *bad* rows."""

    name: str
    failure: ColumnOrFn
    screen_type: str = "column"
    severity: str = Severity.ERROR
    action: str = Action.REJECT
    columns: Sequence[str] = field(default_factory=list)
    description: str = ""
    fix: Optional[ColumnOrFn] = None  # replacement value when action == FIX
    fix_column: Optional[str] = None

    def annotate(self, df: DataFrame, flag: str) -> DataFrame:
        return df.withColumn(flag, F.coalesce(_resolve(self.failure, df), F.lit(False)))

    def apply_fix(self, df: DataFrame, flag: str) -> DataFrame:
        target = self.fix_column or (self.columns[0] if self.columns else None)
        if self.action != Action.FIX or self.fix is None or target is None:
            return df
        return df.withColumn(target, F.when(F.col(flag), _resolve(self.fix, df)).otherwise(F.col(target)))


# ------------------------------------------------------------ column screens
class ColumnScreen:
    """Factory methods for common column screens."""

    @staticmethod
    def not_null(column: str, **kw) -> Screen:
        return Screen(f"{column}_not_null", F.col(column).isNull(), "column", columns=[column],
                      description=f"{column} must not be null", **kw)

    @staticmethod
    def not_blank(column: str, **kw) -> Screen:
        return Screen(f"{column}_not_blank", F.col(column).isNull() | (F.trim(F.col(column)) == ""),
                      "column", columns=[column], description=f"{column} must not be blank", **kw)

    @staticmethod
    def in_range(column: str, min_value=None, max_value=None, **kw) -> Screen:
        cond = F.lit(False)
        if min_value is not None:
            cond = cond | (F.col(column) < F.lit(min_value))
        if max_value is not None:
            cond = cond | (F.col(column) > F.lit(max_value))
        return Screen(f"{column}_in_range", cond, "column", columns=[column],
                      description=f"{column} between {min_value} and {max_value}", **kw)

    @staticmethod
    def in_set(column: str, allowed: Sequence, allow_null: bool = True, **kw) -> Screen:
        cond = ~F.col(column).isin(list(allowed))
        if not allow_null:
            cond = cond | F.col(column).isNull()
        return Screen(f"{column}_in_set", cond, "column", columns=[column],
                      description=f"{column} in {list(allowed)}", **kw)

    @staticmethod
    def matches(column: str, regex: str, **kw) -> Screen:
        return Screen(f"{column}_format", ~F.col(column).cast("string").rlike(regex), "column",
                      columns=[column], description=f"{column} matches /{regex}/", **kw)

    @staticmethod
    def length_between(column: str, min_len: int = 0, max_len: Optional[int] = None, **kw) -> Screen:
        ln = F.length(F.col(column))
        cond = ln < min_len
        if max_len is not None:
            cond = cond | (ln > max_len)
        return Screen(f"{column}_length", cond, "column", columns=[column],
                      description=f"len({column}) in [{min_len}, {max_len}]", **kw)

    @staticmethod
    def castable(column: str, data_type: str, **kw) -> Screen:
        cond = F.col(column).isNotNull() & F.expr(f"try_cast(`{column}` AS {data_type})").isNull()
        return Screen(f"{column}_is_{data_type}", cond, "column", columns=[column],
                      description=f"{column} castable to {data_type}", **kw)


# --------------------------------------------------------- structure screens
@dataclass
class UniqueScreen(Screen):
    """Rows whose key appears more than once in the batch."""

    def annotate(self, df: DataFrame, flag: str) -> DataFrame:
        w = Window.partitionBy(*self.columns)
        return df.withColumn(flag, F.count(F.lit(1)).over(w) > 1)


@dataclass
class ReferentialScreen(Screen):
    """Rows whose foreign key is not found in ``parent`` (orphans)."""

    parent: Optional[DataFrame] = None
    parent_columns: Sequence[str] = field(default_factory=list)

    def annotate(self, df: DataFrame, flag: str) -> DataFrame:
        pcols = list(self.parent_columns or self.columns)
        marker = f"{flag}__hit"
        parent = self.parent.select(
            *[F.col(p).alias(f"{flag}__{i}") for i, p in enumerate(pcols)]
        ).distinct().withColumn(marker, F.lit(True))
        cond = None
        for i, c in enumerate(self.columns):
            part = F.col(c) == F.col(f"{flag}__{i}")
            cond = part if cond is None else cond & part
        joined = df.join(parent, cond, "left")
        not_null = None
        for c in self.columns:
            part = F.col(c).isNotNull()
            not_null = part if not_null is None else not_null & part
        out = joined.withColumn(flag, not_null & F.col(marker).isNull())
        return out.drop(marker, *[f"{flag}__{i}" for i in range(len(pcols))])


class StructureScreen:
    @staticmethod
    def unique(columns: Sequence[str], **kw) -> Screen:
        kw.setdefault("severity", Severity.ERROR)
        kw.setdefault("action", Action.REJECT)
        return UniqueScreen(name=f"unique_{'_'.join(columns)}", failure=F.lit(False), screen_type="structure",
                            columns=list(columns), description=f"{columns} unique", **kw)

    @staticmethod
    def referential(columns: Sequence[str], parent: DataFrame, parent_columns: Optional[Sequence[str]] = None,
                    name: Optional[str] = None, **kw) -> Screen:
        return ReferentialScreen(name=name or f"fk_{'_'.join(columns)}", failure=F.lit(False),
                                 screen_type="structure", columns=list(columns), parent=parent,
                                 parent_columns=list(parent_columns or columns),
                                 description=f"{columns} references parent", **kw)

    @staticmethod
    def conditional_required(column: str, when: str, **kw) -> Screen:
        """``column`` must be populated when SQL condition ``when`` holds."""
        return Screen(f"{column}_required_when", F.expr(when) & F.col(column).isNull(), "structure",
                      columns=[column], description=f"{column} required when {when}", **kw)


class BusinessRuleScreen:
    @staticmethod
    def rule(name: str, valid_when: ColumnOrFn, columns: Sequence[str] = (), **kw) -> Screen:
        """``valid_when`` expresses the GOOD state; failures are its negation."""

        def failure(df: DataFrame) -> Column:
            return ~_resolve(valid_when, df)

        return Screen(name, failure, "business", columns=list(columns), **kw)


# ------------------------------------------------------- table-level checks
@dataclass
class TableScreen:
    """Batch-level test, e.g. row count tolerance vs. previous load."""

    name: str
    check: Callable[[DataFrame], bool]
    severity: str = Severity.FATAL
    action: str = Action.HALT
    description: str = ""
    screen_type: str = "structure"
    columns: Sequence[str] = field(default_factory=list)

    @staticmethod
    def row_count_between(min_rows: int = 1, max_rows: Optional[int] = None, **kw) -> "TableScreen":
        def check(df: DataFrame) -> bool:
            n = df.count()
            return n >= min_rows and (max_rows is None or n <= max_rows)

        return TableScreen("row_count_between", check, description=f"rows in [{min_rows}, {max_rows}]", **kw)


# ----------------------------------------------------------------- engine
@dataclass
class ScreenResult:
    clean: DataFrame          # rows to load (passed + flagged + fixed)
    rejected: DataFrame       # rows routed to suspense
    events: DataFrame         # error events (Subsystem 5 format)
    summary: List[Dict[str, object]]
    halted: bool = False

    @property
    def total_failures(self) -> int:
        return sum(int(s["failures"]) for s in self.summary)


DQ_FLAGS = "_dq_flags"
DQ_SCORE = "_dq_score"


class DataCleaningSystem:
    """Runs a suite of screens against a DataFrame."""

    def __init__(self, ctx: EtlContext, table_name: str, screens: Sequence[Union[Screen, TableScreen]],
                 record_key: Sequence[str] = (), capture_record: bool = True) -> None:
        self.ctx = ctx
        self.table_name = table_name
        self.screens = [s for s in screens if isinstance(s, Screen)]
        self.table_screens = [s for s in screens if isinstance(s, TableScreen)]
        self.record_key = list(record_key)
        self.capture_record = capture_record

    def run(self, df: DataFrame, raise_on_halt: bool = True) -> ScreenResult:
        from .errors import build_error_events, table_screen_event

        original_cols = df.columns
        flags = [f"__f{i}" for i in range(len(self.screens))]
        annotated = df
        for screen, flag in zip(self.screens, flags):
            annotated = screen.annotate(annotated, flag)
        annotated = annotated.cache()

        events = build_error_events(self.ctx, annotated, self.screens, flags, self.table_name,
                                    self.record_key, original_cols, self.capture_record)

        counts_row = annotated.agg(
            *[F.sum(F.col(f).cast("int")).alias(f) for f in flags], F.count(F.lit(1)).alias("__n")
        ).first() if flags else None
        summary = []
        for screen, flag in zip(self.screens, flags):
            summary.append({
                "screen": screen.name, "type": screen.screen_type, "severity": screen.severity,
                "action": screen.action, "failures": int((counts_row[flag] or 0) if counts_row else 0),
            })

        halted = False
        table_events = []
        for ts in self.table_screens:
            ok = bool(ts.check(df))
            summary.append({"screen": ts.name, "type": "table", "severity": ts.severity, "action": ts.action,
                            "failures": 0 if ok else 1})
            if not ok:
                table_events.append(table_screen_event(self.ctx, ts, self.table_name))
                halted = halted or ts.action == Action.HALT
        if table_events:
            events = events.unionByName(self.ctx.spark.createDataFrame(table_events, events.schema))
        halted = halted or any(s["action"] == Action.HALT and s["failures"] for s in summary)

        # fixes
        fixed = annotated
        for screen, flag in zip(self.screens, flags):
            fixed = screen.apply_fix(fixed, flag)

        reject_flags = [f for s, f in zip(self.screens, flags) if s.action in (Action.REJECT, Action.HALT)]
        is_reject = F.lit(False)
        for f in reject_flags:
            is_reject = is_reject | F.col(f)

        flag_names = F.array_compact(F.array(*[
            F.when(F.col(f), F.lit(s.name)) for s, f in zip(self.screens, flags)
        ])) if flags else F.array().cast("array<string>")
        penalty = F.lit(0.0)
        for s, f in zip(self.screens, flags):
            penalty = penalty + F.when(F.col(f), F.lit(Severity.WEIGHT.get(s.severity, 0.5))).otherwise(0.0)
        scored = fixed.withColumn(DQ_FLAGS, flag_names).withColumn(DQ_SCORE, F.greatest(F.lit(0.0), 1 - penalty))
        out_cols = original_cols + [DQ_FLAGS, DQ_SCORE]
        clean = scored.where(~is_reject).select(*out_cols)
        rejected = scored.where(is_reject).select(*out_cols)

        result = ScreenResult(clean, rejected, events, summary, halted)
        if halted and raise_on_halt:
            raise DataQualityHaltError(f"HALT screen failed on {self.table_name}: "
                                       f"{[s for s in summary if s['failures'] and s['action'] == Action.HALT]}",
                                       result)
        return result
