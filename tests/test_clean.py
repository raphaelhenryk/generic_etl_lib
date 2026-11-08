import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from generic_etl.clean import (
    Action,
    AuditDimensionAssembler,
    BusinessRuleScreen,
    ColumnScreen,
    Conformer,
    DataCleaningSystem,
    DataQualityHaltError,
    Deduplicator,
    ErrorEventHandler,
    MatchRule,
    Severity,
    StructureScreen,
    TableScreen,
    check_conformance,
    drill_across,
)


@pytest.fixture()
def orders(spark):
    return spark.createDataFrame(
        [(1, "C1", 10.0, "NEW"), (2, None, 5.0, "NEW"), (3, "C2", -1.0, "BAD"), (3, "C9", 7.0, "SHIPPED")],
        "order_id int, customer_id string, amount double, status string",
    )


def test_screens_and_error_events(ctx, spark, orders):
    customers = spark.createDataFrame([("C1",), ("C2",)], "customer_id string")
    screens = [
        ColumnScreen.not_null("customer_id", action=Action.REJECT),
        ColumnScreen.in_range("amount", min_value=0, action=Action.FIX, fix=F.lit(0.0)),
        ColumnScreen.in_set("status", ["NEW", "SHIPPED"], action=Action.FLAG, severity=Severity.WARN),
        StructureScreen.unique(["order_id"], action=Action.FLAG, severity=Severity.WARN),
        StructureScreen.referential(["customer_id"], customers, action=Action.REJECT),
        BusinessRuleScreen.rule("shipped_has_amount", "status <> 'SHIPPED' OR amount > 0", ["status", "amount"],
                                action=Action.PASS, severity=Severity.INFO),
    ]
    dcs = DataCleaningSystem(ctx, "stg_orders", screens, record_key=["order_id"])
    res = dcs.run(orders)
    clean = {r.order_id: r for r in res.clean.collect()}
    assert set(clean) == {1, 3}  # order 2 null customer, order 3/C9 orphan -> rejected
    assert clean[3].amount == 0.0 and "status_in_set" in clean[3]._dq_flags
    assert res.rejected.count() == 2
    by_screen = {s["screen"]: s["failures"] for s in res.summary}
    assert by_screen["customer_id_not_null"] == 1 and by_screen["fk_customer_id"] == 1
    assert by_screen["unique_order_id"] == 2

    handler = ErrorEventHandler(ctx)
    handler.register_screens(screens)
    assert handler.record(res.events) == res.events.count() > 0
    qi = handler.quality_indicator()
    assert qi["indicator"] == "Errors"
    rec = handler.failing_records("stg_orders", "customer_id_not_null")
    assert rec[0]["order_id"] == 2


def test_halt_screen(ctx, orders):
    with pytest.raises(DataQualityHaltError):
        DataCleaningSystem(ctx, "t", [TableScreen.row_count_between(min_rows=100)]).run(orders)


def test_audit_dimension(ctx, orders):
    res = DataCleaningSystem(ctx, "t", [ColumnScreen.not_null("customer_id")]).run(orders)
    asm = AuditDimensionAssembler(ctx)
    k1 = asm.assemble("fact_orders", "erp", rows_extracted=4, screen_result=res)
    k2 = asm.assemble("fact_orders", "erp", rows_extracted=4, rows_loaded=4)
    assert k2 == k1 + 1
    row = asm.lookup(k1)
    assert row["rows_rejected"] == 1 and row["quality_indicator"] == "Errors"
    assert row["completeness_indicator"] == "Partial"
    assert AuditDimensionAssembler.attach(orders, k1).select("audit_key").distinct().count() == 1


def test_dedup_fuzzy_and_survivorship(spark):
    df = spark.createDataFrame(
        [(1, "John Smith", "10 Main St", "2024-01-01", "crm"),
         (2, "Jon Smith", "10 Main Street", "2024-03-01", "web"),
         (3, "Mary Jones", "5 Elm Rd", "2024-01-01", "crm"),
         (4, "J. Smith", "10 Main St.", "2024-02-01", "erp")],
        "id int, name string, address string, updated string, src string")
    d = Deduplicator("id", [MatchRule("name", "levenshtein", 1.0), MatchRule("address", "jaccard", 1.0)],
                     blocking_keys=[F.soundex(F.split("name", " ").getItem(1))], threshold=0.6)
    out = {r.id: r._cluster_id for r in d.deduplicate(df).collect()}
    assert out[1] == out[2] == out[4] == 1 and out[3] == 3
    golden = Deduplicator.golden_records(d.cluster(df), {"name": "longest", "address": "most_recent",
                                                         "src": "source_priority"},
                                         recency_column="updated", source_priority={"erp": 1, "crm": 2},
                                         source_column="src")
    g = {r._cluster_id: r for r in golden.collect()}
    assert g[1]["name"] == "John Smith" and g[1]["address"] == "10 Main Street" and g[1]["src"] == "erp"
    assert g[1]["_member_count"] == 3

    ex = Deduplicator.exact(df.withColumn("k", F.lit("x")), ["k"], [F.desc("updated")])
    assert ex.where("_is_survivor").first().id == 2


def test_conformer_and_drill_across(spark):
    src = spark.createDataFrame([("1", " paris ", "M", 10.0), ("2", "Rome", "X", 20.0)],
                                "cust_no string, city string, gender string, weight_lb double")
    schema = T.StructType([T.StructField("customer_id", T.IntegerType()), T.StructField("city", T.StringType()),
                           T.StructField("gender", T.StringType()), T.StructField("weight_kg", T.DoubleType()),
                           T.StructField("segment", T.StringType())])
    out = (Conformer().rename({"cust_no": "customer_id"}).standardize(["city"], case="upper")
           .map_values("gender", {"M": "Male", "F": "Female"}, default="Unknown")
           .convert_units("weight_lb", "weight_kg", 0.5).enforce_schema(schema)).apply(src)
    rows = {r.customer_id: r for r in out.collect()}
    assert rows[1].city == "PARIS" and rows[1].gender == "Male" and rows[2].gender == "Unknown"
    assert rows[1].weight_kg == 5.0 and out.columns == schema.fieldNames()

    master = spark.createDataFrame([(1, "A"), (2, "B")], "k int, attr string")
    assert check_conformance(spark.createDataFrame([(1, "A")], "k int, attr string"), master, ["k"]).conformed
    bad = check_conformance(spark.createDataFrame([(1, "Z"), (3, "C")], "k int, attr string"), master, ["k"])
    assert not bad.conformed and bad.keys_missing_in_master == 1 and bad.attribute_mismatches == {"attr": 1}

    sales = spark.createDataFrame([(1, 10.0), (1, 5.0), (2, 1.0)], "product int, amount double")
    inv = spark.createDataFrame([(1, 100), (3, 7)], "product int, qty int")
    da = {r.product: r for r in drill_across({"s": sales, "i": inv}, ["product"],
                                             {"s": {"revenue": F.sum("amount")}, "i": {"stock": F.sum("qty")}}).collect()}
    assert da[1].revenue == 15.0 and da[1].stock == 100 and da[3].revenue is None
