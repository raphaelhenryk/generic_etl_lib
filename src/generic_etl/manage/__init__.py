"""Category 4 – Managing the ETL Environment (subsystems 22-34)."""
from .backup import BackupManager
from .compliance import ComplianceManager, RetentionPolicy
from .escalation import (
    CallableNotifier,
    EmailNotifier,
    EscalationLevel,
    Incident,
    LogNotifier,
    Notifier,
    ProblemEscalationSystem,
    WebhookNotifier,
)
from .lineage import LineageAnalyzer
from .metadata import MetadataRepository, ObjectDefinition
from .migration import EnvironmentConfig, MigrationManager, PromotionError, schema_compatibility
from .monitor import WorkflowMonitor
from .parallel import ParallelExecutor, enable_adaptive_execution, optimal_partitions, repartition_for_write
from .recovery import JobHaltRequested, RecoveryManager
from .scheduler import (
    DependencyCycleError,
    Job,
    JobResult,
    JobScheduler,
    Task,
    path_exists,
    table_loaded_since,
    to_databricks_job,
)
from .security import RolePolicy, SecurityManager, mask_column
from .sort import SortSystem
from .versioning import VersionControl, git_info, module_fingerprint

__all__ = [
    "Job", "Task", "JobScheduler", "JobResult", "DependencyCycleError", "to_databricks_job",
    "table_loaded_since", "path_exists",
    "BackupManager",
    "RecoveryManager", "JobHaltRequested",
    "VersionControl", "git_info", "module_fingerprint",
    "MigrationManager", "EnvironmentConfig", "PromotionError", "schema_compatibility",
    "WorkflowMonitor",
    "SortSystem",
    "LineageAnalyzer",
    "ProblemEscalationSystem", "EscalationLevel", "Incident", "Notifier", "LogNotifier", "WebhookNotifier",
    "EmailNotifier", "CallableNotifier",
    "ParallelExecutor", "optimal_partitions", "repartition_for_write", "enable_adaptive_execution",
    "SecurityManager", "RolePolicy", "mask_column",
    "ComplianceManager", "RetentionPolicy",
    "MetadataRepository", "ObjectDefinition",
]
