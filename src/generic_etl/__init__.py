"""generic_etl – Kimball's 34 ETL subsystems for Apache Spark / Databricks.

Packages map to Kimball's four categories:

* :mod:`generic_etl.extract`  – Extracting (subsystems 1-3)
* :mod:`generic_etl.clean`    – Cleaning and Conforming (4-8)
* :mod:`generic_etl.deliver`  – Delivering / presentation (9-21)
* :mod:`generic_etl.manage`   – Managing the ETL environment (22-34)

Every component receives an :class:`~generic_etl.core.EtlContext` which
carries the Spark session, the :class:`~generic_etl.core.TableStore` and the
current batch identity.
"""
__version__ = "0.1.0"

from .core import EtlContext, TableStore  # noqa: E402

SUBSYSTEMS = {
    1: ("Data Profiling System", "generic_etl.extract.profiling.DataProfiler"),
    2: ("Change Data Capture System", "generic_etl.extract.cdc"),
    3: ("Extract System", "generic_etl.extract.extract.ExtractSystem"),
    4: ("Data Cleaning System", "generic_etl.clean.quality.DataCleaningSystem"),
    5: ("Error Event Schema", "generic_etl.clean.errors.ErrorEventHandler"),
    6: ("Audit Dimension Assembler", "generic_etl.clean.audit.AuditDimensionAssembler"),
    7: ("Deduplication System", "generic_etl.clean.dedup.Deduplicator"),
    8: ("Conforming System", "generic_etl.clean.conform.Conformer"),
    9: ("Slowly Changing Dimension Manager", "generic_etl.deliver.scd.SCDManager"),
    10: ("Surrogate Key Generator", "generic_etl.deliver.surrogate_keys.SurrogateKeyGenerator"),
    11: ("Hierarchy Manager", "generic_etl.deliver.hierarchy.HierarchyManager"),
    12: ("Special Dimensions Manager", "generic_etl.deliver.special_dims"),
    13: ("Fact Table Builders", "generic_etl.deliver.facts"),
    14: ("Surrogate Key Pipeline", "generic_etl.deliver.key_pipeline.SurrogateKeyPipeline"),
    15: ("Multi-Valued Dimension Bridge Table Builder", "generic_etl.deliver.bridge.BridgeTableBuilder"),
    16: ("Late Arriving Data Handler", "generic_etl.deliver.late_arriving"),
    17: ("Dimension Manager System", "generic_etl.deliver.dimension_manager.DimensionManager"),
    18: ("Fact Table Provider System", "generic_etl.deliver.fact_provider.FactTableProvider"),
    19: ("Aggregate Builder", "generic_etl.deliver.aggregates.AggregateBuilder"),
    20: ("OLAP Cube Builder", "generic_etl.deliver.olap.CubeBuilder"),
    21: ("Data Propagation Manager", "generic_etl.deliver.propagation.DataPropagationManager"),
    22: ("Job Scheduler", "generic_etl.manage.scheduler.JobScheduler"),
    23: ("Backup System", "generic_etl.manage.backup.BackupManager"),
    24: ("Recovery and Restart System", "generic_etl.manage.recovery.RecoveryManager"),
    25: ("Version Control System", "generic_etl.manage.versioning.VersionControl"),
    26: ("Version Migration System", "generic_etl.manage.migration.MigrationManager"),
    27: ("Workflow Monitor", "generic_etl.manage.monitor.WorkflowMonitor"),
    28: ("Sort System", "generic_etl.manage.sort.SortSystem"),
    29: ("Lineage and Dependency Analyzer", "generic_etl.manage.lineage.LineageAnalyzer"),
    30: ("Problem Escalation System", "generic_etl.manage.escalation.ProblemEscalationSystem"),
    31: ("Parallelizing / Pipelining System", "generic_etl.manage.parallel.ParallelExecutor"),
    32: ("Security System", "generic_etl.manage.security.SecurityManager"),
    33: ("Compliance Manager", "generic_etl.manage.compliance.ComplianceManager"),
    34: ("Metadata Repository Manager", "generic_etl.manage.metadata.MetadataRepository"),
}

__all__ = ["EtlContext", "TableStore", "SUBSYSTEMS", "__version__"]
