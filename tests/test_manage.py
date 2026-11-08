from datetime import timedelta

import pytest
from pyspark.sql import functions as F

from generic_etl.core import TableStore, utcnow
from generic_etl.manage import (
    BackupManager,
    CallableNotifier,
    ComplianceManager,
    EnvironmentConfig,
    EscalationLevel,
    Incident,
    Job,
    JobScheduler,
    LineageAnalyzer,
    MetadataRepository,
    MigrationManager,
    ObjectDefinition,
    ParallelExecutor,
    ProblemEscalationSystem,
    PromotionError,
    RecoveryManager,
    RetentionPolicy,
    RolePolicy,
    SecurityManager,
    SortSystem,
    Task,
    VersionControl,
    WorkflowMonitor,
    optimal_partitions,
    to_databricks_job,
)


def test_scheduler_monitor_recovery_escalation(ctx):
    alerts = []
    esc = ProblemEscalationSystem(ctx, [EscalationLevel([CallableNotifier(lambda i, c, l: alerts.append((i, l)))])])
    mon = WorkflowMonitor(ctx)
    rec = RecoveryManager(ctx, run_id="run-1")
    calls = []
    state = {"fail": True}

    job = Job("nightly", schedule="0 0 2 * * ?")

    @job.task()
    def extract(c):
        calls.append("extract")
        return {"rows_out": 10}

    @job.task(depends_on=["extract"])
    def load_dim(c):
        calls.append("load_dim")
        return 5

    @job.task(depends_on=["extract"])
    def load_fact(c):
        calls.append("load_fact")
        if state["fail"]:
            raise RuntimeError("boom")
        return 3

    job.add(Task("aggregates", lambda c, outputs: outputs["load_fact"] * 2, ["load_fact", "load_dim"]))
    assert job.waves() == [["extract"], ["load_dim", "load_fact"], ["aggregates"]]

    res = JobScheduler(ctx, mon, rec, esc, max_parallel=2).run(job)
    assert not res.succeeded
    assert res.statuses == {"extract": "SUCCEEDED", "load_dim": "SUCCEEDED", "load_fact": "FAILED",
                            "aggregates": "UPSTREAM_FAILED"}
    assert alerts and alerts[0][0].task_name == "load_fact"

    # restart: completed tasks are skipped, failed task resumes
    state["fail"] = False
    calls.clear()
    res2 = JobScheduler(ctx, mon, rec, esc).run(job)
    assert res2.succeeded and calls == ["load_fact"] and res2.outputs["aggregates"] == 6
    stats = {r.task_name: r for r in mon.task_statistics().collect()}
    assert stats["load_fact"].runs == 2 and stats["load_fact"].failure_rate == 0.5
    assert mon.batch_status().where("task_name = 'extract'").first().rows_out == 10

    spec = to_databricks_job(job, lambda t: {"python_wheel_task": {"package_name": "etl", "entry_point": t.name}})
    assert spec["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
    assert {"task_key": "load_fact"} in [d for t in spec["tasks"] if t["task_key"] == "aggregates" for d in t["depends_on"]]


def test_escalation_levels(ctx):
    sent = []
    esc = ProblemEscalationSystem(ctx, [
        EscalationLevel([CallableNotifier(lambda i, c, l: sent.append(("team", l)), "team")], "INFO"),
        EscalationLevel([CallableNotifier(lambda i, c, l: sent.append(("oncall", l)), "oncall")], "CRITICAL"),
    ], repeat_threshold=2)
    assert esc.raise_incident(Incident("t", "warn", "WARN")) == 0
    assert esc.raise_incident(Incident("t", "warn again", "WARN")) == 1  # repeated -> escalated
    assert esc.raise_incident(Incident("x", "down", "CRITICAL")) == 1
    assert esc.incidents().count() == 3


def test_rollback(ctx, spark):
    store = ctx.store
    store.overwrite(spark.createDataFrame([(1,)], "v int"), "fact_a")
    rec = RecoveryManager(ctx)
    rec.capture_restore_points(["fact_a", "fact_new"])
    store.append(spark.createDataFrame([(2,)], "v int"), "fact_a")
    store.overwrite(spark.createDataFrame([(9,)], "v int"), "fact_new")
    assert sorted(rec.rollback()) == ["fact_a", "fact_new"]
    assert [r.v for r in store.read("fact_a").collect()] == [1]
    assert not store.exists("fact_new")


def test_backup_and_restore(ctx, spark, tmp_path):
    ctx.store.overwrite(spark.createDataFrame([(1,), (2,)], "v int"), "dim_x")
    bm = BackupManager(ctx, TableStore(spark, base_path=str(tmp_path / "bkp")), retention_days=1)
    rec = bm.backup("dim_x")
    assert rec["row_count"] == 2 and rec["method"] == "copy"
    ctx.store.overwrite(spark.createDataFrame([(3,)], "v int"), "dim_x")
    assert bm.restore("dim_x") == 2
    assert bm.expire(now=utcnow() + timedelta(days=2)) == [rec["backup_id"]]
    assert bm.catalog("dim_x").count() == 0


def test_version_control_and_migration(ctx, spark, tmp_path):
    import generic_etl.deliver.scd as scd

    vc = VersionControl(ctx, "sales_pipeline")
    snap = vc.snapshot([scd], version_tag="v1")
    assert ctx.code_version == "v1" and "generic_etl.deliver.scd" in snap["module_hashes"]
    assert vc.detect_drift([scd], "v1") == []
    vc.snapshot([scd, "generic_etl.deliver.facts"], version_tag="v2")
    assert vc.diff("v1", "v2")["added"] == ["generic_etl.deliver.facts"]

    envs = [EnvironmentConfig(e, base_path=str(tmp_path / e)) for e in ("dev", "test", "prod")]
    mm = MigrationManager(spark, envs)
    envs[0].store(spark).overwrite(spark.createDataFrame([(1, "a")], "id int, name string"), "dim_x")
    envs[1].store(spark).overwrite(spark.createDataFrame([(1,)], "id bigint"), "dim_x")
    with pytest.raises(PromotionError):
        mm.promote("sales_pipeline", "v2", "dev", tables=["dim_x"])  # id type changed -> breaking
    out = mm.promote("sales_pipeline", "v2", "dev", tables=["dim_x"], allow_breaking=True,
                     gates={"unit_tests": lambda: (True, "42 passed")})
    assert out["to_env"] == "test" and mm.deployed_version("sales_pipeline", "test") == "v2"
    with pytest.raises(PromotionError):
        mm.promote("sales_pipeline", "v2", "dev", to_env="prod")
    with pytest.raises(PromotionError):
        mm.promote("sales_pipeline", "v2", "test", gates={"dq": lambda: (False, "score 0.7")})


def test_sort_and_parallel(ctx, spark):
    df = spark.createDataFrame([(3, "c"), (1, "a"), (2, "b")], "k int, v string")
    assert [r.k for r in SortSystem.sort(df, ["k"]).collect()] == [1, 2, 3]
    assert [r.k for r in SortSystem.sort(df, ["k"], descending=[True]).collect()] == [3, 2, 1]
    assert SortSystem.is_sorted(SortSystem.sort(df, ["k"]).coalesce(1), "k")
    assert SortSystem.top_n(df, 1, ["k"]).first().k == 1

    ex = ParallelExecutor(spark, max_workers=3)
    res = ex.map(lambda n: spark.range(n).count(), [10, 20, 30])
    assert {k: r.value for k, r in res.items()} == {10: 10, 20: 20, 30: 30}
    bad = ex.run_all({"ok": lambda: 1, "ko": lambda: 1 / 0})
    assert bad["ok"].ok and isinstance(bad["ko"].error, ZeroDivisionError)
    assert ex.pipeline([1, 2], [lambda x: x + 1, lambda x: x * 10])[2].value == 30
    assert optimal_partitions(1024 ** 3, 128) == 8


def test_lineage(ctx):
    la = LineageAnalyzer(ctx)
    la.record("crm.customers", "stg_customers", "extract", {"cust_name": "name"})
    la.record("stg_customers", "dim_customer", "scd2", {"customer_name": ["cust_name"]})
    la.record("dim_customer", "agg_sales", "aggregate")
    la.record("fact_sales", "agg_sales", "aggregate")
    assert la.lineage("agg_sales") == {"dim_customer": 1, "fact_sales": 1, "stg_customers": 2, "crm.customers": 3}
    assert la.impact("crm.customers") == {"stg_customers": 1, "dim_customer": 2, "agg_sales": 3}
    assert la.lineage("dim_customer.customer_name", column=True) == {"stg_customers.cust_name": 1,
                                                                     "crm.customers.name": 2}
    assert sorted(la.roots()) == ["crm.customers", "fact_sales"]
    assert "-->" in la.to_mermaid()


def test_security(ctx, spark):
    stmts = SecurityManager.grant_statements([RolePolicy("analysts", ["SELECT"], ["TABLE main.gold.fact_sales"]),
                                              RolePolicy("etl", ["ALL_PRIVILEGES"], ["SCHEMA main.staging"])])
    assert stmts == ["GRANT SELECT ON TABLE main.gold.fact_sales TO `analysts`",
                     "GRANT ALL PRIVILEGES ON SCHEMA main.staging TO `etl`"]
    assert "SET MASK" in SecurityManager.column_mask_statements("t", "ssn", "f_mask", "hr")[1]
    df = spark.createDataFrame([("4111111111111111", "ann@x.com", "secret")], "card string, email string, note string")
    m = SecurityManager.mask(df, {"card": "partial", "email": "email", "note": "hash"}).first()
    assert m.card == "************1111" and m.email == "a***@x.com" and len(m.note) == 64
    key = "0123456789abcdef"
    enc = SecurityManager.encrypt(df, ["note"], key)
    assert enc.first().note != "secret"
    assert SecurityManager.decrypt(enc, ["note"], key).first().note == "secret"


def test_compliance(ctx, spark):
    cm = ComplianceManager(ctx)
    df = spark.createDataFrame([(1, "ann", "2020-01-01"), (2, "bob", utcnow().strftime("%Y-%m-%d"))],
                               "id int, name string, d string").withColumn("d", F.to_date("d"))
    ctx.store.overwrite(df, "dim_person")
    cm.record_custody("dim_person", "load", df)
    assert cm.verify_dataset("dim_person", ctx.store.read("dim_person"))
    assert cm.apply_retention(RetentionPolicy("dim_person", "d", 365)) == 1
    assert cm.erase_subject("id", 2, ["dim_person"]) == {"dim_person": 1}
    assert ctx.store.read("dim_person").count() == 0
    assert cm.verify_chain() == {"valid": True, "broken_at": None, "records": 3}
    cm.log_access("dim_person", "audit review", ["id"])
    assert cm.access_report().first().accesses == 1
    # tamper with the log -> chain breaks
    trail = cm.custody_trail().withColumn("actor", F.when(F.col("sequence") == 2, "mallory").otherwise(F.col("actor")))
    ctx.meta_store.overwrite(trail, "etl_chain_of_custody")
    assert cm.verify_chain()["broken_at"] == 2


def test_metadata_repository(ctx, spark):
    ctx.store.overwrite(spark.createDataFrame([(1, "a")], "customer_key int, email string"), "dim_customer")
    md = MetadataRepository(ctx)
    md.register(ObjectDefinition("dim_customer", "dimension", "Conformed customer", owner="sales",
                                 columns={"customer_key": "Surrogate key", "email": "Contact e-mail"},
                                 column_classification={"email": "pii"}, tags={"domain": "sales"}))
    md.capture("dim_customer")
    ctx.store.overwrite(spark.createDataFrame([(1, "a", "x")], "customer_key int, email string, city string"),
                        "dim_customer")
    md.capture("dim_customer")
    assert md.schema_changes("dim_customer")[0]["added"] == ["city"]
    assert md.sensitive_columns().first().column_name == "email"
    assert md.search("e-mail").count() == 1
    d = md.describe("dim_customer")
    assert d["technical"]["column_count"] == 3 and len(d["business"]) == 3
    stmts = md.comment_statements("dim_customer")
    assert any(s.startswith("COMMENT ON TABLE") for s in stmts) and any("classification' = 'pii" in s for s in stmts)
