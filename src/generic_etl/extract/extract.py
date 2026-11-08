"""Subsystem 3 – Extract System.

Physical connection to heterogeneous sources, protocol / encoding
conversions (EBCDIC -> ASCII, COBOL packed decimals, fixed-width records) and
landing of the raw data into the staging area *without changing its
structure* (only lineage columns are added).
"""
from __future__ import annotations

import codecs
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..core.context import EtlContext

# Lineage columns added to every staged row
EXTRACT_TS = "_extract_ts"
BATCH_ID = "_batch_id"
SOURCE_SYSTEM = "_source_system"
SOURCE_FILE = "_source_file"


# --------------------------------------------------------------------- sources
class Source:
    """A readable source. Subclasses define ``name`` / ``system`` and implement :meth:`read`."""

    def read(self, spark: SparkSession) -> DataFrame:  # pragma: no cover - abstract
        raise NotImplementedError


@dataclass
class FileSource(Source):
    """CSV / JSON / Parquet / ORC / Avro / text / XML file source."""

    name: str
    path: str
    format: str = "csv"
    options: Dict[str, str] = field(default_factory=dict)
    schema: Optional[T.StructType] = None
    system: str = "files"

    def read(self, spark: SparkSession) -> DataFrame:
        opts = dict(self.options)
        if self.format == "csv":
            opts.setdefault("header", "true")
        reader = spark.read.format(self.format).options(**opts)
        if self.schema is not None:
            reader = reader.schema(self.schema)
        return reader.load(self.path).withColumn(SOURCE_FILE, F.col("_metadata.file_path"))


@dataclass
class JdbcSource(Source):
    """Relational source over JDBC with optional parallel partitioned reads.

    Credentials should come from a secret scope, e.g.
    ``password=dbutils.secrets.get("scope", "key")``.
    """

    name: str
    url: str
    table: Optional[str] = None
    query: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    driver: Optional[str] = None
    partition_column: Optional[str] = None
    lower_bound: Optional[int] = None
    upper_bound: Optional[int] = None
    num_partitions: Optional[int] = None
    fetch_size: int = 10000
    options: Dict[str, str] = field(default_factory=dict)
    system: str = "jdbc"

    def read(self, spark: SparkSession) -> DataFrame:
        if not (self.table or self.query):
            raise ValueError("JdbcSource needs `table` or `query`")
        r = spark.read.format("jdbc").option("url", self.url).option("fetchsize", self.fetch_size)
        r = r.option("dbtable", self.table) if self.table else r.option("query", self.query)
        for k, v in (("user", self.user), ("password", self.password), ("driver", self.driver)):
            if v:
                r = r.option(k, v)
        if self.partition_column:
            r = (
                r.option("partitionColumn", self.partition_column)
                .option("lowerBound", self.lower_bound)
                .option("upperBound", self.upper_bound)
                .option("numPartitions", self.num_partitions or 8)
            )
        return r.options(**self.options).load()


@dataclass
class TableSource(Source):
    """An existing metastore / Unity Catalog table or Delta path."""

    name: str
    table: str
    system: str = "lakehouse"
    format: str = "delta"

    def read(self, spark: SparkSession) -> DataFrame:
        if "/" in self.table:
            return spark.read.format(self.format).load(self.table)
        return spark.table(self.table)


@dataclass
class DataFrameSource(Source):
    """Wrap an already-built DataFrame (APIs, pandas, tests...)."""

    name: str
    df: DataFrame
    system: str = "dataframe"

    def read(self, spark: SparkSession) -> DataFrame:
        return self.df


# ------------------------------------------------------------ conversions
def ebcdic_to_string(col, codepage: str = "cp037"):
    """Decode an EBCDIC binary column into a UTF-8 string column.

    ``codepage`` is any Python codec (cp037 US/Canada, cp500 international,
    cp1047 Open Systems, cp273 Germany, ...).
    """
    codecs.lookup(codepage)  # fail fast for unknown codepages

    @F.udf(T.StringType())
    def _decode(b):
        return bytes(b).decode(codepage) if b is not None else None

    return _decode(F.col(col) if isinstance(col, str) else col)


def unpack_comp3(data: Optional[bytes], scale: int = 0) -> Optional[Decimal]:
    """Decode a COBOL COMP-3 (packed decimal) value."""
    if data is None:
        return None
    data = bytes(data)
    if not data:
        return None
    digits = []
    for b in data[:-1]:
        digits.extend([(b >> 4) & 0xF, b & 0xF])
    digits.append((data[-1] >> 4) & 0xF)
    sign_nibble = data[-1] & 0xF
    if any(d > 9 for d in digits):
        raise ValueError(f"invalid packed decimal {data.hex()}")
    value = Decimal(int("".join(map(str, digits)) or "0")).scaleb(-scale)
    return -value if sign_nibble == 0xD else value


def comp3_to_decimal(col, precision: int, scale: int = 0):
    """Spark column converting COMP-3 binary to ``decimal(precision, scale)``."""

    @F.udf(T.DecimalType(precision, scale))
    def _unpack(b):
        return unpack_comp3(b, scale)

    return _unpack(F.col(col) if isinstance(col, str) else col)


@dataclass
class FixedWidthField:
    name: str
    start: int  # 1-based position, like a COBOL copybook
    length: int
    data_type: str = "string"
    trim: bool = True


def parse_fixed_width(df: DataFrame, layout: Sequence[FixedWidthField], value_column: str = "value") -> DataFrame:
    """Split fixed-width records (e.g. ``spark.read.text``) into typed columns."""
    cols = []
    for f in layout:
        c = F.substring(F.col(value_column), f.start, f.length)
        if f.trim:
            c = F.trim(c)
        cols.append(F.nullif(c, F.lit("")).cast(f.data_type).alias(f.name))
    keep = [c for c in df.columns if c != value_column]
    return df.select(*keep, *cols)


def parse_fixed_width_binary(
    df: DataFrame,
    layout: Sequence[Tuple[str, int, int, str]],
    value_column: str = "content",
    codepage: str = "cp037",
) -> DataFrame:
    """Parse binary mainframe records.

    ``layout`` items are ``(name, start, length, kind)`` where kind is
    ``"char"`` (EBCDIC text), ``"comp3:<scale>"`` (packed decimal) or
    ``"binary"`` (big-endian signed integer, COMP).
    """
    out = []
    for name, start, length, kind in layout:
        raw = F.substring(F.col(value_column), start, length)
        if kind == "char":
            c = F.trim(ebcdic_to_string(raw, codepage))
        elif kind.startswith("comp3"):
            scale = int(kind.split(":")[1]) if ":" in kind else 0
            c = comp3_to_decimal(raw, min(38, length * 2), scale)
        elif kind == "binary":
            c = F.conv(F.hex(raw), 16, 10).cast("long")
        else:
            raise ValueError(f"unknown field kind {kind}")
        out.append(c.alias(name))
    return df.select(*out)


def split_binary_records(spark: SparkSession, path: str, record_length: int) -> DataFrame:
    """Read a file of fixed-length binary records (RECFM=FB) into rows."""
    rdd = spark.sparkContext.binaryFiles(path).flatMap(
        lambda kv: [
            (kv[0], bytearray(kv[1][i : i + record_length]))
            for i in range(0, len(kv[1]) - record_length + 1, record_length)
        ]
    )
    schema = T.StructType([T.StructField(SOURCE_FILE, T.StringType()), T.StructField("content", T.BinaryType())])
    return spark.createDataFrame(rdd, schema)


# --------------------------------------------------------------- extractor
class ExtractSystem:
    """Reads sources and lands them, unchanged, in staging tables."""

    def __init__(self, ctx: EtlContext, staging_prefix: str = "stg_") -> None:
        self.ctx = ctx
        self.staging_prefix = staging_prefix

    def staging_name(self, source: Source) -> str:
        return f"{self.staging_prefix}{source.name}"

    def extract(self, source: Source) -> DataFrame:
        df = source.read(self.ctx.spark)
        return (
            df.withColumn(EXTRACT_TS, F.current_timestamp())
            .withColumn(BATCH_ID, F.lit(self.ctx.batch_id))
            .withColumn(SOURCE_SYSTEM, F.lit(source.system))
        )

    def land(self, source: Source, df: Optional[DataFrame] = None, mode: str = "append") -> Dict[str, object]:
        """Extract (if ``df`` is not given) and write to staging; returns stats."""
        df = df if df is not None else self.extract(source)
        name = self.staging_name(source)
        self.ctx.store.write(df, name, mode=mode)
        rows = self.ctx.store.read(name).where(F.col(BATCH_ID) == self.ctx.batch_id).count()
        return {"source": source.name, "staging_table": name, "rows": rows, "batch_id": self.ctx.batch_id}

    def land_all(self, sources: List[Source], mode: str = "append") -> List[Dict[str, object]]:
        return [self.land(s, mode=mode) for s in sources]

    def read_staged(self, source: Source, batch_id: Optional[str] = None) -> DataFrame:
        df = self.ctx.store.read(self.staging_name(source))
        return df.where(F.col(BATCH_ID) == (batch_id or self.ctx.batch_id))
