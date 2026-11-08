"""Subsystem 30 – Problem Escalation System.

Turns failures and data-quality incidents into notifications routed by
severity, and escalates to the next level when an incident repeats or
remains unresolved. Channels: logging, webhooks (Slack, Teams, PagerDuty,
Opsgenie-compatible JSON), e-mail (SMTP) or any callable.
"""
from __future__ import annotations

import json
import logging
import smtplib
import urllib.request
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import Callable, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext, utcnow

log = logging.getLogger("generic_etl.escalation")

INCIDENT_LOG = "etl_incident_log"
INCIDENT_SCHEMA = T.StructType([
    T.StructField("incident_key", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("task_name", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("level", T.IntegerType()),
    T.StructField("message", T.StringType()),
    T.StructField("channels", T.StringType()),
    T.StructField("raised_at", T.TimestampType()),
])

SEVERITIES = ["INFO", "WARN", "ERROR", "CRITICAL"]


@dataclass
class Incident:
    task_name: str
    message: str
    severity: str = "ERROR"
    details: Dict[str, object] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.task_name}:{self.severity}"


class Notifier:
    name = "notifier"

    def send(self, incident: Incident, ctx: EtlContext, level: int) -> None:  # pragma: no cover
        raise NotImplementedError


class LogNotifier(Notifier):
    name = "log"

    def send(self, incident: Incident, ctx: EtlContext, level: int) -> None:
        lvl = {"INFO": logging.INFO, "WARN": logging.WARNING}.get(incident.severity, logging.ERROR)
        log.log(lvl, "[%s L%d] %s/%s: %s", incident.severity, level, ctx.job_name, incident.task_name,
                incident.message)


class CallableNotifier(Notifier):
    def __init__(self, fn: Callable[[Incident, EtlContext, int], None], name: str = "callable") -> None:
        self.fn = fn
        self.name = name

    def send(self, incident: Incident, ctx: EtlContext, level: int) -> None:
        self.fn(incident, ctx, level)


class WebhookNotifier(Notifier):
    """POST JSON to a webhook (Slack / Teams incoming webhook, PagerDuty events...)."""

    def __init__(self, url: str, name: str = "webhook", payload: Optional[Callable[[Incident, EtlContext, int], dict]] = None,
                 timeout: int = 10) -> None:
        self.url = url
        self.name = name
        self.payload = payload
        self.timeout = timeout

    def build_payload(self, incident: Incident, ctx: EtlContext, level: int) -> dict:
        if self.payload:
            return self.payload(incident, ctx, level)
        return {"text": f":rotating_light: [{incident.severity} L{level}] {ctx.environment}/{ctx.job_name}"
                        f"/{incident.task_name}: {incident.message} (batch {ctx.batch_id})"}

    def send(self, incident: Incident, ctx: EtlContext, level: int) -> None:
        data = json.dumps(self.build_payload(incident, ctx, level), default=str).encode()
        req = urllib.request.Request(self.url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=self.timeout).read()


class EmailNotifier(Notifier):
    name = "email"

    def __init__(self, host: str, sender: str, recipients: Sequence[str], port: int = 587,
                 user: Optional[str] = None, password: Optional[str] = None, use_tls: bool = True) -> None:
        self.host, self.port, self.sender = host, port, sender
        self.recipients = list(recipients)
        self.user, self.password, self.use_tls = user, password, use_tls

    def send(self, incident: Incident, ctx: EtlContext, level: int) -> None:
        msg = EmailMessage()
        msg["Subject"] = f"[ETL {incident.severity} L{level}] {ctx.job_name}/{incident.task_name}"
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.set_content(f"{incident.message}\n\nbatch: {ctx.batch_id}\nenvironment: {ctx.environment}\n"
                        f"details: {json.dumps(incident.details, default=str, indent=2)}")
        with smtplib.SMTP(self.host, self.port, timeout=15) as s:
            if self.use_tls:
                s.starttls()
            if self.user:
                s.login(self.user, self.password or "")
            s.send_message(msg)


@dataclass
class EscalationLevel:
    """Notifiers used at this level, and the minimum severity routed to it."""

    notifiers: List[Notifier]
    min_severity: str = "INFO"


class ProblemEscalationSystem:
    """Route incidents to levels; repeated incidents climb the ladder.

    ``repeat_threshold``: an incident raised that many times (same task and
    severity, in the incident log) goes one level up.
    """

    def __init__(self, ctx: EtlContext, levels: Sequence[EscalationLevel], repeat_threshold: int = 3,
                 persist: bool = True) -> None:
        self.ctx = ctx
        self.levels = list(levels) or [EscalationLevel([LogNotifier()])]
        self.repeat_threshold = repeat_threshold
        self.persist = persist
        self._counts: Dict[str, int] = {}

    def _history_count(self, key: str) -> int:
        if not self.persist or not self.ctx.meta_store.exists(INCIDENT_LOG):
            return 0
        return self.ctx.meta_store.read(INCIDENT_LOG).where(F.col("incident_key") == key).count()

    def level_for(self, incident: Incident) -> int:
        sev = SEVERITIES.index(incident.severity) if incident.severity in SEVERITIES else 2
        level = 0
        for i, lvl in enumerate(self.levels):
            if sev >= SEVERITIES.index(lvl.min_severity):
                level = i
        repeats = self._counts.get(incident.key, 0) + self._history_count(incident.key)
        if self.repeat_threshold and repeats + 1 >= self.repeat_threshold:
            level = min(level + 1, len(self.levels) - 1)
        return level

    def raise_incident(self, incident: Incident) -> int:
        level = self.level_for(incident)
        sent = []
        for i in range(level + 1):
            for n in self.levels[i].notifiers:
                if SEVERITIES.index(incident.severity if incident.severity in SEVERITIES else "ERROR") < \
                        SEVERITIES.index(self.levels[i].min_severity):
                    continue
                try:
                    n.send(incident, self.ctx, level)
                    sent.append(n.name)
                except Exception as exc:  # never let alerting break the pipeline
                    log.error("notifier %s failed: %s", n.name, exc)
        self._counts[incident.key] = self._counts.get(incident.key, 0) + 1
        if self.persist:
            self.ctx.meta_store.append_rows(INCIDENT_LOG, [{
                "incident_key": incident.key, "batch_id": self.ctx.batch_id, "job_name": self.ctx.job_name,
                "task_name": incident.task_name, "severity": incident.severity, "level": level,
                "message": incident.message[:4000], "channels": ",".join(sent), "raised_at": utcnow()}],
                INCIDENT_SCHEMA)
        return level

    def on_failure(self, task_name: str, exc: BaseException, severity: str = "ERROR") -> int:
        return self.raise_incident(Incident(task_name, f"{type(exc).__name__}: {exc}", severity))

    def incidents(self) -> DataFrame:
        return self.ctx.meta_store.read_or_empty(INCIDENT_LOG, INCIDENT_SCHEMA)
