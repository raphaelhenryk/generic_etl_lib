from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from generic_etl.extract import (
    CDC_OP,
    DataFrameSource,
    DataProfiler,
    ExtractSystem,
    FixedWidthField,
    SnapshotDiffCDC,
    TimestampCDC,
    parse_fixed_width,
    parse_fixed_width_binary,
    unpack_comp3,
)


@pytest.fixture()
def customers(spark):
    return spark.createDataFrame(
        [(1, "Ann", "ann@x.com", 10.0), (2, "Bob", None, 20.0), (3, "Cy", "cy@x.com", 30.0), (4, "Di", "", 1000.0)],
        "id int, name string, email string, amount double",
    )


def test_profile_and_keys(customers):
    p = DataProfiler(customers)
    prof = {r["column_name"]: r for r in p.profile().collect()}
    assert prof["id"]["is_candidate_key"] is True
    assert prof["email"]["null_count"] == 1
    assert prof["email"]["blank_count"] == 1
    assert prof["amount"]["max_value"] == "1000.0"
    assert ["id"] in p.candidate_keys(max_columns=2)
    patterns = {r["pattern"] for r in p.pattern_frequencies("email").collect()}
    assert "AAA@A.AAA" in patterns
    issues = p.anomalies(max_null_pct=0.1, outlier_z=1.0)
    assert {"column": "email", "issue": "high_null_rate", "value": 0.25} in issues


def test_foreign_key_candidate(spark):
    parent = spark.createDataFrame([(1,), (2,)], "id int")
    child = spark.createDataFrame([(1,), (2,), (2,), (3,)], "cust_id int")
    fk = DataProfiler.foreign_key_candidate(child, "cust_id", parent, "id")
    assert fk.orphan_count == 1 and fk.matched_distinct == 2
    assert DataProfiler(spark.createDataFrame([("a", "x"), ("b", "x")], "zip string, city string")) \
        .functional_dependency(["zip"], "city") == 1.0


def test_timestamp_cdc(ctx, spark):
    src = spark.createDataFrame([(1, "2024-01-01 00:00:00"), (2, "2024-01-02 00:00:00")], "id int, ts string") \
        .withColumn("ts", F.to_timestamp("ts"))
    cdc = TimestampCDC(ctx, "src", "ts")
    changes, mark = cdc.extract(src)
    assert changes.count() == 2
    cdc.commit(mark)
    src2 = src.union(spark.createDataFrame([(3, "2024-01-03 00:00:00")], "id int, ts string")
                     .withColumn("ts", F.to_timestamp("ts")))
    changes, _ = TimestampCDC(ctx, "src", "ts").extract(src2)
    assert [r.id for r in changes.collect()] == [3]


def test_snapshot_diff_cdc(ctx, spark):
    cdc = SnapshotDiffCDC(ctx, "snap_customers", keys=["id"])
    v1 = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id int, v string")
    assert cdc.diff(v1).count() == 3
    cdc.commit(v1)
    v2 = spark.createDataFrame([(1, "a"), (2, "B"), (4, "d")], "id int, v string")
    ops = {r.id: r[CDC_OP] for r in cdc.diff(v2).collect()}
    assert ops == {2: "U", 4: "I", 3: "D"}


def test_mainframe_conversions(spark):
    assert unpack_comp3(bytes([0x12, 0x34, 0x5C]), 2) == Decimal("123.45")
    assert unpack_comp3(bytes([0x01, 0x2D])) == Decimal("-12")
    rec = "ABC".encode("cp037") + bytes([0x12, 0x34, 0x5C])
    df = spark.createDataFrame([(bytearray(rec),)], "content binary")
    out = parse_fixed_width_binary(df, [("code", 1, 3, "char"), ("amount", 4, 3, "comp3:2")]).first()
    assert out.code == "ABC" and out.amount == Decimal("123.45")
    txt = spark.createDataFrame([("0001JOHN  0042",)], "value string")
    fw = parse_fixed_width(txt, [FixedWidthField("id", 1, 4, "int"), FixedWidthField("name", 5, 6),
                                 FixedWidthField("age", 11, 4, "int")]).first()
    assert (fw.id, fw.name, fw.age) == (1, "JOHN", 42)


def test_extract_and_land(ctx, customers):
    es = ExtractSystem(ctx)
    src = DataFrameSource("customers", customers, system="crm")
    stats = es.land(src)
    assert stats["rows"] == 4 and stats["staging_table"] == "stg_customers"
    staged = es.read_staged(src)
    assert {"_extract_ts", "_batch_id", "_source_system"} <= set(staged.columns)
