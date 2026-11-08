import os
import sys

import pytest

SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, SRC)
# Python UDF workers must also be able to import the library
os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (SRC, os.environ.get("PYTHONPATH")) if p)

from pyspark.sql import SparkSession  # noqa: E402

from generic_etl.core import EtlContext  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("generic_etl_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture()
def ctx(spark, tmp_path):
    return EtlContext.create(spark, base_path=str(tmp_path / "dw"), job_name="test_job", environment="test")
