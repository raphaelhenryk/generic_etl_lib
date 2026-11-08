import json

import pytest
from pyspark.sql import functions as F

from generic_etl.core import TableStore
from generic_etl.deliver import (
    AccumulatingSnapshotFactBuilder,
    AggregateBuilder,
    AggregateDefinition,
    Band,
    BridgeTableBuilder,
    CubeBuilder,
    CubeDimension,
    CubeMeasure,
    CubeModel,
    DataPropagationManager,
    DimensionLookup,
    DimensionManager,
    DimensionRollup,
    ExportSpec,
    FactTableProvider,
    HierarchyCycleError,
    HierarchyManager,
    JunkDimensionBuilder,
    LateArrivingDimensionHandler,
    MiniDimensionBuilder,
    PeriodicSnapshotFactBuilder,
    SCDConfig,
    SCDManager,
    SurrogateKeyGenerator,
    SurrogateKeyPipeline,
    TransactionFactBuilder,
    build_date_dimension,
    build_shrunken_dimension,
    build_time_dimension,
    choose_aggregate,
)

CUST = "customer_id string, name string, city string, segment string"


def customer_cfg(**kw):
    base = dict(dimension="dim_customer", natural_keys=["customer_id"], surrogate_key="customer_key",
                type2=["city"], type3={"segment": "prior_segment"}, type6={"city": "current_city"})
    base.update(kw)
    return SCDConfig(**base)


def test_surrogate_keys_never_reused(ctx, spark):
    g = SurrogateKeyGenerator(ctx, "dim_x", "x_key")
    a = g.assign(spark.createDataFrame([("a",), ("b",)], "v string"), order_by=["v"])
    assert [r.x_key for r in a.orderBy("v").collect()] == [1, 2]
    b = g.assign(spark.createDataFrame([("c",)], "v string"))
    assert b.first().x_key == 3  # registry high-water mark, table never written


def test_scd_types(ctx, spark):
    m = SCDManager(ctx, customer_cfg())
    r1 = m.apply(spark.createDataFrame([("C1", "Ann", "Paris", "A"), ("C2", "Bob", "Rome", "B")], CUST), "2024-01-01")
    assert r1.inserted == 2
    r2 = m.apply(spark.createDataFrame([("C1", "Annie", "Lyon", "A"), ("C2", "Bob", "Rome", "C"),
                                        ("C3", "Cy", "Oslo", "A")], CUST), "2024-02-01", audit_key=7)
    assert (r2.inserted, r2.new_versions, r2.expired) == (1, 1, 1)
    c1 = m.history(customer_id="C1").collect()
    assert [r.city for r in c1] == ["Paris", "Lyon"]
    assert all(r.name == "Annie" and r.current_city == "Lyon" for r in c1)  # type 1 + type 6
    assert str(c1[0].effective_to) == "2024-02-01" and not c1[0].is_current
    c2 = m.current().where("customer_id = 'C2'").first()
    assert (c2.segment, c2.prior_segment, c2.audit_key) == ("C", "B", 7)  # type 3
    assert m.as_of("2024-01-15").where("customer_id = 'C1'").first().city == "Paris"
    assert m.validate() == {"multiple_current": 0, "overlapping_ranges": 0, "duplicate_keys": 0}
    # re-running the same data is a no-op
    r3 = m.apply(spark.createDataFrame([("C1", "Annie", "Lyon", "A")], CUST), "2024-03-01")
    assert (r3.inserted, r3.new_versions, r3.updated, r3.expired) == (0, 0, 0, 0)
    assert ctx.store.read("dim_customer").where("customer_key < 0").count() == 3  # default rows


def test_scd_deletes(ctx, spark):
    m = SCDManager(ctx, customer_cfg(handle_deletes="expire"))
    m.apply(spark.createDataFrame([("C1", "Ann", "Paris", "A"), ("C2", "Bob", "Rome", "B")], CUST), "2024-01-01")
    r = m.apply(spark.createDataFrame([("C1", "Ann", "Paris", "A")], CUST), "2024-02-01")
    assert r.expired == 1 and m.current().where("customer_key > 0").count() == 1


def test_key_pipeline_with_inferred_and_point_in_time(ctx, spark):
    cfg = customer_cfg()
    m = SCDManager(ctx, cfg)
    m.apply(spark.createDataFrame([("C1", "Ann", "Paris", "A")], CUST), "2024-01-01")
    m.apply(spark.createDataFrame([("C1", "Ann", "Lyon", "A")], CUST), "2024-02-01")
    facts = spark.createDataFrame([("C1", "2024-01-10", 1.0), ("C1", "2024-02-10", 2.0), ("C9", "2024-02-10", 3.0),
                                   (None, "2024-02-10", 4.0)], "customer_id string, sale_date string, amount double") \
        .withColumn("sale_date", F.to_date("sale_date"))
    pipe = SurrogateKeyPipeline(ctx, [DimensionLookup("dim_customer", {"customer_id": "customer_id"}, "customer_key",
                                                      as_of_column="sale_date", on_missing="infer", scd_config=cfg)])
    res = pipe.run(facts)
    assert res.inferred == {"dim_customer": 1}
    keys = {r.amount: r.customer_key for r in res.facts.collect()}
    cur = {r.city: r.customer_key for r in m.history(customer_id="C1").collect()}
    assert keys[1.0] == cur["Paris"] and keys[2.0] == cur["Lyon"] and keys[4.0] == -2 and keys[3.0] > 0
    assert "customer_id" not in res.facts.columns
    # the real C9 record arrives: inferred row is overwritten in place, not versioned
    m.apply(spark.createDataFrame([("C9", "Zed", "Nice", "B")], CUST), "2024-03-01")
    c9 = m.history(customer_id="C9").collect()
    assert len(c9) == 1 and c9[0].city == "Nice" and not c9[0].is_inferred and c9[0].customer_key == keys[3.0]

    rej = SurrogateKeyPipeline(ctx, [DimensionLookup("dim_customer", {"customer_id": "customer_id"}, "customer_key",
                                                     on_missing="reject")]).run(
        spark.createDataFrame([("C1", 1.0), ("NOPE", 2.0)], "customer_id string, amount double"))
    assert rej.facts.count() == 1 and rej.rejected.first()._reject_reason == "missing_dim_customer"


def test_backdated_change_and_rekey(ctx, spark):
    cfg = customer_cfg()
    m = SCDManager(ctx, cfg)
    m.apply(spark.createDataFrame([("C1", "Ann", "Paris", "A")], CUST), "2024-01-01")
    k = m.current().where("customer_id='C1'").first().customer_key
    ctx.store.overwrite(spark.createDataFrame([(k, "2024-01-05", 1.0), (k, "2024-03-05", 2.0)],
                                              "customer_key long, sale_date string, amount double")
                        .withColumn("sale_date", F.to_date("sale_date")), "fact_sales")
    h = LateArrivingDimensionHandler(ctx, cfg)
    change = h.insert_backdated_version({"customer_id": "C1", "city": "Lyon"}, "2024-03-01")
    assert h.rekey_facts("fact_sales", "customer_key", "sale_date", change) == 1
    assert m.validate()["overlapping_ranges"] == 0
    assert m.current().where("customer_id='C1'").first().city == "Lyon"


def test_fact_builders(ctx, spark):
    tx = TransactionFactBuilder(ctx, "fact_tx", unique_keys=["order_no"])
    df = spark.createDataFrame([("O1", 10.0), ("O2", 5.0)], "order_no string, amount double")
    assert tx.load(df, audit_key=1) == 2
    assert tx.load(df, audit_key=2) == 0  # idempotent

    ps = PeriodicSnapshotFactBuilder(ctx, "fact_balance", "month_key", grain=["account"])
    ps.load(spark.createDataFrame([(202401, "A", 1.0), (202401, "B", 2.0)], "month_key int, account string, bal double"))
    ps.load(spark.createDataFrame([(202402, "A", 3.0)], "month_key int, account string, bal double"))
    ps.load(spark.createDataFrame([(202401, "A", 9.0)], "month_key int, account string, bal double"))  # restate
    snap = {(r.month_key, r.account): r.bal for r in ctx.store.read("fact_balance").collect()}
    assert snap == {(202401, "A"): 9.0, (202402, "A"): 3.0}

    acc = AccumulatingSnapshotFactBuilder(ctx, "fact_order_pipeline", ["order_no"],
                                          ["order_date_key", "ship_date_key"],
                                          lags={"days_to_ship": ("order_date_key", "ship_date_key")},
                                          milestone_is_date_key=True)
    acc.load(spark.createDataFrame([("O1", 20240101, -1, 10.0)], "order_no string, order_date_key int, ship_date_key int, amount double"))
    st = acc.load(spark.createDataFrame([("O1", -1, 20240105, None), ("O2", 20240102, -1, 3.0)],
                                        "order_no string, order_date_key int, ship_date_key int, amount double"))
    assert st == {"inserted": 1, "updated": 1}
    o1 = ctx.store.read("fact_order_pipeline").where("order_no='O1'").first()
    assert (o1.order_date_key, o1.ship_date_key, o1.days_to_ship, o1.amount) == (20240101, 20240105, 4, 10.0)


def test_hierarchies(spark):
    org = spark.createDataFrame([("CEO", None), ("CFO", "CEO"), ("CTO", "CEO"), ("DEV", "CTO"), ("OPS", "CTO")],
                                "emp string, boss string")
    bridge = HierarchyManager.build_bridge(org, "emp", "boss")
    rows = {(r.ancestor_key, r.descendant_key): r for r in bridge.collect()}
    assert rows[("CEO", "DEV")].depth == 2 and rows[("DEV", "DEV")].is_leaf and rows[("CEO", "CEO")].is_top
    assert len(rows) == 5 + 4 + 2  # self + parent links + grandparent links
    flat = {r.emp: r for r in HierarchyManager.flatten_ragged(org, "emp", "boss", max_depth=3).collect()}
    assert (flat["DEV"].level_1, flat["DEV"].level_3) == ("CEO", "DEV")
    assert flat["CFO"].level_3 == "CFO"  # filled down
    with pytest.raises(HierarchyCycleError):
        HierarchyManager.build_bridge(spark.createDataFrame([("A", "B"), ("B", "A")], "c string, p string"), "c", "p")
    prod = spark.createDataFrame([("p1", "b1", "c1"), ("p2", "b1", "c2")], "product string, brand string, category string")
    assert HierarchyManager.validate_fixed(prod, ["product", "brand", "category"]) == {"brand->category": 1}


def test_special_dimensions(ctx, spark):
    d = build_date_dimension(spark, "2024-01-01", "2024-12-31", fiscal_year_start_month=7,
                             holidays={"2024-12-25": "Christmas"})
    assert d.count() == 367
    x = d.where("date_key = 20241225").first()
    assert x.is_holiday and not x.is_workday and x.fiscal_year == 2025 and x.fiscal_month == 6
    assert build_time_dimension(spark).count() == 1440

    facts = spark.createDataFrame([(1, "Y", "web"), (2, "N", "web"), (3, "Y", "web")], "id int, gift string, channel string")
    junk = JunkDimensionBuilder(ctx, "dim_order_flags", ["gift", "channel"], "flags_key")
    assert junk.update(facts) == 2 and junk.update(facts) == 0
    out = junk.lookup(facts)
    assert out.columns == ["id", "flags_key"] and out.where("flags_key < 0").count() == 0

    mini = MiniDimensionBuilder(ctx, "dim_demographics", [Band("income", [(0, 50000, "Low"), (50000, None, "High")])],
                                "demo_key")
    people = spark.createDataFrame([(1, 1000), (2, 90000)], "id int, income int")
    assert mini.update(people) == 2
    assert mini.lookup(people).select("demo_key").distinct().count() == 2

    month = build_shrunken_dimension(ctx, d.where("date_key > 0"), ["calendar_year", "calendar_month", "month_name"],
                                     "dim_month", "month_key")
    assert month.count() == 12


def test_bridge(ctx, spark):
    assoc = spark.createDataFrame([("V1", 10), ("V1", 20), ("V2", 20), ("V2", 10), ("V3", 30)],
                                  "visit string, diagnosis_key int")
    b = BridgeTableBuilder(ctx, "bridge_diagnosis", "diagnosis_key")
    groups = {r.visit: r.group_key for r in b.build(assoc, ["visit"]).collect()}
    assert groups["V1"] == groups["V2"] != groups["V3"]
    assert b.validate_weights() == 0
    b.build(spark.createDataFrame([("V4", 10), ("V4", 20)], "visit string, diagnosis_key int"), ["visit"])
    assert ctx.store.read("bridge_diagnosis").select("group_key").distinct().count() == 2  # group reused
    fact = spark.createDataFrame([(groups["V1"], 100.0)], "group_key long, charge double")
    assert b.allocate(fact, ["charge"]).agg(F.sum("charge_allocated")).first()[0] == 100.0


def test_dimension_manager_and_fact_provider(ctx, spark, tmp_path):
    dm = DimensionManager(ctx)
    cfg = customer_cfg()
    dm.register(cfg, owner="mdm", description="Conformed customer")
    dm.process("dim_customer", spark.createDataFrame([("C1", "Ann", "Paris", "A")], CUST), as_of="2024-01-01")
    assert dm.latest_version("dim_customer") == 1
    sub = TableStore(spark, base_path=str(tmp_path / "subscriber"))
    assert dm.replicate("dim_customer", {"sales_mart": sub}) == {"sales_mart": 1}
    assert sub.read("dim_customer").count() == ctx.store.read("dim_customer").count()
    assert dm.manager("dim_customer").cfg.type3 == {"segment": "prior_segment"}

    pipe = SurrogateKeyPipeline(ctx, [DimensionLookup("dim_customer", {"customer_id": "customer_id"},
                                                      "customer_key", on_missing="reject")])
    provider = FactTableProvider(ctx, "fact_sales", pipe, TransactionFactBuilder(ctx, "fact_sales"), dm)
    seen = []
    provider.on_loaded(lambda df: seen.append(df.count()))
    res = provider.load(spark.createDataFrame([("C1", 5.0), ("X", 1.0)], "customer_id string, amount double"),
                        audit_key=1, required_versions={"dim_customer": 1})
    assert (res.rows_loaded, res.rows_rejected, seen) == (1, 1, [1])
    assert ctx.store.read("fact_sales_suspense").count() == 1


def test_aggregates_and_cube(ctx, spark):
    ctx.store.overwrite(spark.createDataFrame([(1, "Books", "North"), (2, "Toys", "South")],
                                              "product_key int, category string, region string"), "dim_product")
    ctx.store.overwrite(spark.createDataFrame([(1, 202401, 10.0), (1, 202402, 5.0), (2, 202401, 7.0)],
                                              "product_key int, month_key int, amount double"), "fact_sales")
    d = AggregateDefinition("agg_sales_cat_month", "fact_sales", ["category", "month_key"],
                            {"revenue": ("sum", "amount")},
                            rollups=[DimensionRollup("dim_product", "product_key", "product_key", ["category"])])
    agg = AggregateBuilder(ctx, d)
    assert agg.build() == 3
    ctx.store.append(spark.createDataFrame([(1, 202402, 1.0)], "product_key int, month_key int, amount double"),
                     "fact_sales")
    agg.refresh(spark.createDataFrame([(1, 202402, 1.0)], "product_key int, month_key int, amount double"),
                refresh_keys=["month_key"])
    a = {(r.category, r.month_key): r.revenue for r in ctx.store.read(d.name).collect()}
    assert a[("Books", 202402)] == 6.0 and a[("Books", 202401)] == 10.0
    assert choose_aggregate(["category", "revenue"], [d]) == d.name
    with pytest.raises(ValueError):
        AggregateBuilder(ctx, AggregateDefinition("x", "fact_sales", ["month_key"], {"a": ("avg", "amount")}))

    model = CubeModel("sales", "fact_sales",
                      [CubeDimension("product", "dim_product", "product_key", ["category", "region"],
                                     {"geo": ["region"]})],
                      [CubeMeasure("revenue", "amount")])
    cb = CubeBuilder(ctx, model)
    cube = cb.build(["category", "region"], mode="cube", table="cube_sales")
    assert cube.count() == 2 + 2 + 2 + 1
    assert CubeBuilder.slice(cube).first().revenue == 23.0
    assert CubeBuilder.slice(cube, category="Books").first().revenue == 16.0
    assert json.loads(cb.export_model())["measures"][0]["name"] == "revenue"


def test_propagation(ctx, spark, tmp_path):
    df = spark.createDataFrame([(1, "ann@x.com", "US"), (2, "bob@y.com", "BR")], "id int, email string, country string")
    pm = DataPropagationManager(ctx)
    target = str(tmp_path / "out" / "partner_customers")
    res = pm.export(df, ExportSpec("partner_customers", target, "csv", partner="acme", filter="country = 'US'",
                                   mask={"email": "email"}, rename={"id": "customer_id"}, single_file=True))
    assert res["rows"] == 1
    out = spark.read.option("header", True).csv(target).first()
    assert out.customer_id == "1" and out.email == "a***@x.com"
    manifest = json.loads(spark.read.text(target + "_manifest").first().value)
    assert manifest["rows"] == 1 and manifest["partner"] == "acme"
    assert DataPropagationManager.delta_sharing_sql("s", "main.gold.t", "acme")[-1] == \
        "GRANT SELECT ON SHARE s TO RECIPIENT acme"
