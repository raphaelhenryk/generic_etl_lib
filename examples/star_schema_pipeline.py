"""End-to-end example: a retail sales star schema built with all four categories.

Run locally:      python examples/star_schema_pipeline.py
On Databricks:    upload the wheel, then in a notebook
                  ctx = EtlContext.create(spark, database="main.retail", meta_database="main.etl_meta")
                  and call ``build_job(ctx)`` / ``JobScheduler(ctx, ...).run(job)``.
"""
from __future__ import annotations

import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from generic_etl.clean import (
    Action, AuditDimensionAssembler, ColumnScreen, Conformer, DataCleaningSystem, ErrorEventHandler, Severity,
    StructureScreen,
)
from generic_etl.core import EtlContext
from generic_etl.deliver import (
    AggregateBuilder, AggregateDefinition, DimensionLookup, DimensionManager, DimensionRollup, FactTableProvider,
    SCDConfig, SurrogateKeyPipeline, TransactionFactBuilder, build_date_dimension,
)
from generic_etl.extract import DataFrameSource, DataProfiler, ExtractSystem
from generic_etl.manage import (
    EscalationLevel, Job, JobScheduler, LineageAnalyzer, LogNotifier, ProblemEscalationSystem, RecoveryManager,
    VersionControl, WorkflowMonitor,
)

CUSTOMER = SCDConfig(dimension="dim_customer", natural_keys=["customer_id"], surrogate_key="customer_key",
                     type2=["city", "segment"])


def sample_sources(spark: SparkSession):
    customers = spark.createDataFrame(
        [("C1", " ann lee ", "paris", "Retail"), ("C2", "bob ray", "rome", "Corporate"),
         ("C3", "cy li", None, "Retail")], "cust_no string, name string, city string, segment string")
    sales = spark.createDataFrame(
        [("O1", "C1", "2024-03-01", 120.0), ("O2", "C2", "2024-03-02", 80.0),
         ("O3", "C9", "2024-03-02", 15.0), ("O4", "C1", "2024-03-03", -5.0)],
        "order_no string, customer_id string, order_date string, amount double")
    return customers, sales


def build_job(ctx: EtlContext) -> Job:
    spark = ctx.spark
    customers_src, sales_src = sample_sources(spark)
    extract = ExtractSystem(ctx)
    dm = DimensionManager(ctx)
    dm.register(CUSTOMER, owner="customer-mdm", description="Conformed customer dimension")
    errors = ErrorEventHandler(ctx)
    audit = AuditDimensionAssembler(ctx)
    lineage = LineageAnalyzer(ctx)
    job = Job("retail_sales_nightly", schedule="0 0 2 * * ?")

    @job.task()
    def extract_sources(c):
        DataProfiler(customers_src).profile().show(truncate=False)                # subsystem 1
        n = extract.land(DataFrameSource("customers", customers_src, "crm"))["rows"]  # subsystem 3
        n += extract.land(DataFrameSource("sales", sales_src, "pos"))["rows"]
        lineage.record("crm.customers", "stg_customers", "extract")              # subsystem 29
        lineage.record("pos.sales", "stg_sales", "extract")
        return {"rows_out": n}

    @job.task(depends_on=["extract_sources"])
    def load_date_dimension(c):
        if not c.store.exists("dim_date"):
            c.store.overwrite(build_date_dimension(spark, "2024-01-01", "2024-12-31"), "dim_date")  # subsystem 12
        return c.store.read("dim_date").count()

    @job.task(depends_on=["extract_sources"])
    def load_customer_dimension(c):
        staged = extract.read_staged(DataFrameSource("customers", customers_src, "crm"))
        conformed = (Conformer().rename({"cust_no": "customer_id"})                # subsystem 8
                     .standardize(["name"], case="title").standardize(["city"], case="upper")).apply(staged)
        screens = [ColumnScreen.not_null("city", action=Action.FIX, fix=F.lit("UNKNOWN"), severity=Severity.WARN)]
        res = DataCleaningSystem(c, "stg_customers", screens, record_key=["customer_id"]).run(conformed)  # 4
        errors.record_result(res, screens)                                         # subsystem 5
        key = audit.assemble("dim_customer", "crm", rows_extracted=staged.count(), screen_result=res)  # 6
        result = dm.process("dim_customer", res.clean, as_of="2024-03-01", audit_key=key)  # 9, 10, 17
        lineage.record("stg_customers", "dim_customer", "scd2")
        return {"rows_out": result.inserted + result.new_versions}

    @job.task(depends_on=["load_customer_dimension", "load_date_dimension"])
    def load_sales_fact(c):
        staged = extract.read_staged(DataFrameSource("sales", sales_src, "pos")) \
            .withColumn("order_date", F.to_date("order_date"))
        screens = [ColumnScreen.in_range("amount", min_value=0, action=Action.REJECT),
                   StructureScreen.unique(["order_no"])]
        res = DataCleaningSystem(c, "stg_sales", screens, record_key=["order_no"]).run(staged)
        errors.record_result(res, screens)
        key = audit.assemble("fact_sales", "pos", rows_extracted=staged.count(), screen_result=res)
        facts = res.clean.withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int")) \
            .drop("_dq_flags", "_dq_score", "_extract_ts", "_batch_id", "_source_system")
        pipeline = SurrogateKeyPipeline(c, [DimensionLookup(                       # subsystem 14 + 16
            "dim_customer", {"customer_id": "customer_id"}, "customer_key", as_of_column="order_date",
            on_missing="infer", scd_config=CUSTOMER)])
        provider = FactTableProvider(c, "fact_sales", pipeline,                    # subsystem 18 + 13
                                     TransactionFactBuilder(c, "fact_sales", unique_keys=["order_no"]), dm)
        out = provider.load(facts, audit_key=key)
        lineage.record("stg_sales", "fact_sales", "surrogate key pipeline")
        return {"rows_in": out.rows_in, "rows_out": out.rows_loaded, "rows_rejected": out.rows_rejected}

    @job.task(depends_on=["load_sales_fact"])
    def build_aggregates(c):
        d = AggregateDefinition("agg_sales_by_segment", "fact_sales", ["segment"], {"revenue": ("sum", "amount")},
                                rollups=[DimensionRollup("dim_customer", "customer_key", "customer_key", ["segment"])])
        lineage.record("fact_sales", d.name, "aggregate")
        return AggregateBuilder(c, d).build()                                       # subsystem 19

    return job


def main() -> None:
    spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    ctx = EtlContext.create(spark, base_path=tempfile.mkdtemp(prefix="dw_"), job_name="retail_sales_nightly")
    VersionControl(ctx).snapshot(version_tag="example")                            # subsystem 25
    job = build_job(ctx)
    monitor = WorkflowMonitor(ctx)                                                 # subsystem 27
    result = JobScheduler(ctx, monitor=monitor, recovery=RecoveryManager(ctx),     # subsystems 22, 24, 30
                          escalation=ProblemEscalationSystem(ctx, [EscalationLevel([LogNotifier()])]),
                          max_parallel=2).run(job)
    print(result.statuses)
    monitor.batch_status().show(truncate=False)
    ctx.store.read("dim_customer").orderBy("customer_key").show(truncate=False)
    ctx.store.read("fact_sales").show(truncate=False)
    ctx.store.read("agg_sales_by_segment").show()
    ErrorEventHandler(ctx).summary().show(truncate=False)
    print(LineageAnalyzer(ctx).to_mermaid())


if __name__ == "__main__":
    main()
