# generic_etl — Kimball's 34 ETL subsystems for Spark / Databricks

`generic_etl` is a PySpark library that implements the **34 ETL subsystems** described by Ralph
Kimball (*The Data Warehouse ETL Toolkit*, *The Kimball Group Reader*). It is built for Databricks
(Delta Lake, Unity Catalog, Workflows) and also runs on plain Apache Spark with parquet. That makes
local development and unit testing easy.

```
src/generic_etl/
├── core/      EtlContext (run identity) + TableStore (Delta/parquet, catalog/path)
├── extract/   Category 1 – Extracting                (subsystems 1-3)
├── clean/     Category 2 – Cleaning and Conforming   (subsystems 4-8)
├── deliver/   Category 3 – Delivering                (subsystems 9-21)
└── manage/    Category 4 – Managing the ETL environment (subsystems 22-34)
```

## Installation

```bash
pip install .            # on Databricks the runtime already provides pyspark / delta
```

```bash
pip install ".[dev]"     # local development: pyspark + pytest
```

On Databricks: build a wheel (`python -m build`), upload it to a Volume or workspace files, and
attach it to the cluster or job environment.

## Core concepts

```python
from generic_etl import EtlContext

# Databricks / Unity Catalog
ctx = EtlContext.create(spark, database="main.retail", meta_database="main.etl_meta",
                        job_name="retail_nightly", environment="prod")

# Local / path based (parquet when Delta is not installed)
ctx = EtlContext.create(spark, base_path="/tmp/dw", job_name="retail_nightly")
```

* `ctx.store` holds the warehouse tables. `ctx.meta_store` holds the ETL's own operational tables
  (`etl_*`: error events, audit, run log, lineage, watermarks and so on).
* Each run has a `batch_id`. Every subsystem stamps its records with it.
* `TableStore` uses Delta `MERGE`, `replaceWhere`, time travel, `RESTORE`, `CLONE` and `OPTIMIZE`
  when Delta is available. Otherwise it falls back to equivalent rewrite strategies.

## Subsystem map

### Category 1 – Extracting

| # | Subsystem | API |
|---|-----------|-----|
| 1 | Data Profiling | `extract.DataProfiler`: column stats in one pass, value and pattern frequencies, candidate keys, functional dependencies, FK discovery, anomaly report |
| 2 | Change Data Capture | `extract.TimestampCDC` (audit columns + persisted watermark), `SnapshotDiffCDC` (hash diff → I/U/D), `DeltaChangeFeedCDC` (log based, Delta CDF) |
| 3 | Extract System | `extract.ExtractSystem` + `FileSource` / `JdbcSource` (parallel partitioned reads) / `TableSource`; EBCDIC decoding, COBOL COMP-3, fixed-width text and binary records; lands to `stg_*` with lineage columns |

### Category 2 – Cleaning and Conforming

| # | Subsystem | API |
|---|-----------|-----|
| 4 | Data Cleaning | `clean.DataCleaningSystem` with `ColumnScreen`, `StructureScreen` (unique, referential, conditional), `BusinessRuleScreen`, `TableScreen`; actions PASS / FLAG / FIX / REJECT / HALT; per-row `_dq_flags` and `_dq_score` |
| 5 | Error Event Schema | `clean.ErrorEventHandler`: `etl_screen_dim` + `etl_error_event_fact` (one row per record × screen failure, with key and JSON snapshot); summaries and quality indicator |
| 6 | Audit Dimension | `clean.AuditDimensionAssembler`: one `etl_audit_dim` row per load (batch, code version, counts, DQ indicators); `attach()` stamps `audit_key` on rows |
| 7 | Deduplication | `clean.Deduplicator`: standardisation, blocking, Levenshtein / Soundex / Jaccard / numeric matching, transitive clustering, survivorship rules (most recent, longest, source priority, most frequent…) |
| 8 | Conforming | `clean.Conformer` (rename, standardise, value mapping tables, units, currency, conformed schema), `check_conformance`, `drill_across` |

### Category 3 – Delivering

| # | Subsystem | API |
|---|-----------|-----|
| 9 | SCD Manager | `deliver.SCDManager(SCDConfig)`: Type 0/1/2/3/6 per attribute, inferred-member overwrite, delete handling (expire / flag), `as_of()`, `history()`, `validate()` |
| 10 | Surrogate Key Generator | `deliver.SurrogateKeyGenerator`: registry high-water mark (keys never reused), reserved keys -1 Unknown, -2 N/A, -3 Not yet available |
| 11 | Hierarchy Manager | `deliver.HierarchyManager`: fixed-depth validation, ragged flattening with fill-down, recursive **hierarchy bridge** (ancestor/descendant/depth/leaf/top), cycle detection |
| 12 | Special Dimensions | `build_date_dimension` (calendar + fiscal + holidays), `build_time_dimension`, `JunkDimensionBuilder`, `MiniDimensionBuilder` + `Band`, `build_shrunken_dimension` |
| 13 | Fact Table Builders | `TransactionFactBuilder` (idempotent), `PeriodicSnapshotFactBuilder` (partition replace, carry-forward), `AccumulatingSnapshotFactBuilder` (milestones + lag measures) |
| 14 | Surrogate Key Pipeline | `deliver.SurrogateKeyPipeline` + `DimensionLookup`: current or point-in-time Type 2 lookups; misses go to unknown / reject / infer / fail |
| 15 | Bridge Table Builder | `deliver.BridgeTableBuilder`: group keys reused per member set, weighting factors, allocation |
| 16 | Late Arriving Data | `LateArrivingDimensionHandler` (inferred members, backdated Type 2 version insert + fact re-keying), `LateArrivingFactHandler` |
| 17 | Dimension Manager | `deliver.DimensionManager`: registry of conformed dims, processing, versioned publication, replication to subscriber stores |
| 18 | Fact Table Provider | `deliver.FactTableProvider`: checks dimension versions, runs key pipeline + builder, suspense table, load log, post-load listeners |
| 19 | Aggregate Builder | `deliver.AggregateBuilder`: aggregate tables with dimension roll-ups, incremental refresh of touched slices, `choose_aggregate` navigator |
| 20 | OLAP Cube Builder | `deliver.CubeBuilder`: CUBE / ROLLUP / GROUPING SETS with `(All)` members, `slice()`, JSON semantic model export |
| 21 | Data Propagation | `deliver.DataPropagationManager`: export contracts (filter, columns, masking, rename), manifest with checksum, JDBC/table/files, Delta Sharing SQL |

### Category 4 – Managing the ETL Environment

| # | Subsystem | API |
|---|-----------|-----|
| 22 | Job Scheduler | `manage.Job` / `Task` / `JobScheduler`: DAG waves, parallel waves, retries, pre-conditions, export to a Databricks Jobs API 2.1 payload (`to_databricks_job`) |
| 23 | Backup | `manage.BackupManager`: Delta DEEP/SHALLOW CLONE or copy, catalogue, restore, retention expiry |
| 24 | Recovery & Restart | `manage.RecoveryManager`: task checkpoints per run (restart skips done tasks), restore points + rollback (Delta RESTORE or snapshot), graceful halt |
| 25 | Version Control | `manage.VersionControl`: git commit/branch/dirty, module source fingerprints, release snapshots, diff and drift detection |
| 26 | Version Migration | `manage.MigrationManager` + `EnvironmentConfig`: dev → test → prod promotion gates, schema compatibility, DDL evolution, promotion log |
| 27 | Workflow Monitor | `manage.WorkflowMonitor`: `track()` context manager, `etl_run_log`, batch status, task statistics, duration anomalies |
| 28 | Sort System | `manage.SortSystem`: global / partition / range sorts, top-N, sorted file delivery, bucketing, Z-ORDER, liquid clustering |
| 29 | Lineage & Dependency | `manage.LineageAnalyzer`: table and column edges, `lineage()` backward, `impact()` forward, plan inference, UC system-table import, Mermaid graph |
| 30 | Problem Escalation | `manage.ProblemEscalationSystem`: severity routing, repeat-based escalation, log / webhook (Slack, Teams, PagerDuty) / e-mail / callable notifiers, incident log |
| 31 | Parallelizing / Pipelining | `manage.ParallelExecutor` (threads + fair-scheduler pools, `map`, `pipeline`), partition sizing, AQE settings |
| 32 | Security | `manage.SecurityManager`: UC GRANT/REVOKE from role policies, column masks, row filters, DataFrame masking, AES-GCM encryption with keys from secret scopes |
| 33 | Compliance | `manage.ComplianceManager`: tamper-evident hash-chained custody log, dataset fingerprints, access log, retention purge, right-to-erasure |
| 34 | Metadata Repository | `manage.MetadataRepository`: business, technical (schema history) and process metadata, search, UC comments and tags |

`generic_etl.SUBSYSTEMS` lists the same mapping programmatically.

## Example: SCD + surrogate key pipeline

```python
from generic_etl.deliver import SCDConfig, SCDManager, SurrogateKeyPipeline, DimensionLookup, TransactionFactBuilder

customer = SCDConfig(dimension="dim_customer", natural_keys=["customer_id"], surrogate_key="customer_key",
                     type2=["city", "segment"],                 # history
                     type3={"segment": "prior_segment"},        # prior value
                     type6={"city": "current_city"})            # current value on every row
                                                                # everything else: Type 1
SCDManager(ctx, customer).apply(clean_customers, as_of="2024-03-01", audit_key=audit_key)

keyed = SurrogateKeyPipeline(ctx, [
    DimensionLookup("dim_customer", {"customer_id": "customer_id"}, "customer_key",
                    as_of_column="order_date",                  # point-in-time Type 2 lookup
                    on_missing="infer", scd_config=customer),   # late-arriving dimension → inferred member
]).run(orders)
TransactionFactBuilder(ctx, "fact_sales", unique_keys=["order_no"]).load(keyed.facts, audit_key=audit_key)
```

## Example: quality screens → error events → audit dimension

```python
from generic_etl.clean import *

screens = [
    ColumnScreen.not_null("customer_id"),                                     # REJECT
    ColumnScreen.in_range("amount", min_value=0, action=Action.FIX, fix=F.lit(0)),
    StructureScreen.unique(["order_no"], action=Action.FLAG, severity=Severity.WARN),
    BusinessRuleScreen.rule("ship_after_order", "ship_date >= order_date"),
    TableScreen.row_count_between(min_rows=1),                                # HALT the batch
]
result = DataCleaningSystem(ctx, "stg_orders", screens, record_key=["order_no"]).run(staged)
ErrorEventHandler(ctx).record_result(result, screens)
audit_key = AuditDimensionAssembler(ctx).assemble("fact_sales", "pos", screen_result=result)
```

## Example: orchestration with restart and escalation

```python
from generic_etl.manage import *

job = Job("retail_nightly", schedule="0 0 2 * * ?")

@job.task()
def extract(ctx): ...

@job.task(depends_on=["extract"], retries=2)
def load_dims(ctx): ...

scheduler = JobScheduler(ctx, monitor=WorkflowMonitor(ctx),
                         recovery=RecoveryManager(ctx, run_id="2024-03-01"),   # rerun resumes at the failure
                         escalation=ProblemEscalationSystem(ctx, [
                             EscalationLevel([LogNotifier()]),
                             EscalationLevel([WebhookNotifier(slack_url)], min_severity="CRITICAL")]),
                         max_parallel=4)
result = scheduler.run(job)

# or hand it over to Databricks Workflows:
payload = to_databricks_job(job, lambda t: {"python_wheel_task": {
    "package_name": "my_etl", "entry_point": "run_task", "parameters": [t.name]}})
```

The full star-schema walk-through is in [`examples/star_schema_pipeline.py`](examples/star_schema_pipeline.py).

## Testing

```bash
pip install ".[dev]"
```

```bash
pytest
```

The suite (33 tests) runs on local Spark with parquet storage and covers every subsystem.

## Design notes and limitations

* **Dimensions are processed in full** (an incoming batch compared with the current dimension). The
  library then writes only the changed rows through a Delta `MERGE` keyed on the surrogate key. This
  suits dimensions with millions of rows. Very large, fast-changing dimensions should use a mini-dimension.
* **Without Delta**, upserts and partition replacements rewrite the table. Writes from one Python
  process are serialised, so parallel tasks can safely share the parquet log tables. Concurrent
  writers in *different* clusters need Delta.
* **Delta MERGE schema evolution:** to add columns to an existing Delta dimension through `upsert`,
  enable `spark.databricks.delta.schema.autoMerge.enabled`.
* **OLAP:** cubes are materialised relationally. The exported JSON model is meant for semantic-layer
  tools; the library does not build MOLAP storage.
* **Single writer:** the surrogate key registry and the compliance hash chain assume one writer per
  dimension or log at a time (which is how Kimball's Dimension Manager is organised).
