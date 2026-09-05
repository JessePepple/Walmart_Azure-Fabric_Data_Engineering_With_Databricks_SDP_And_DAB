import sys
import pytest
from pyspark.sql.functions import *
import shutil
import tempfile
from datetime import datetime, timedelta
 
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import Row, SparkSession

sys.path.append(
    "/Workspace/Users/jessepepple36@gmail.com/walmart_project/utils/"
)

from utils import SilverTransformation


# ---------------------------
# Spark Session Fixture (Databricks-safe)
# ---------------------------
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


# ---------------------------
# __init__
# ---------------------------
def test_init(spark):
    data = [("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)

    assert transformer.df is df


# ---------------------------
# read_bronzedata
# ---------------------------
def test_read_bronzedata(spark):
    data = [("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    table_name = "walmart_catalog.bronze.test_dataset"

    df.write.mode("overwrite").saveAsTable(table_name)

    transformer = SilverTransformation()
    result = transformer.read_bronzedata("test_dataset")

    assert result.count() == 2
    assert "name" in result.columns

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------
# add_currtimestamp
# ---------------------------
def test_add_currtimestamp(spark):
    data = [("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.add_currtimestamp("cdc_ts")

    assert "cdc_ts" in result.columns
    assert result.filter(col("cdc_ts").isNull()).count() == 0


# ---------------------------
# fill_nulls
# ---------------------------
def test_fill_nulls(spark):
    data = [
        (1, None),
        (None, 3)
    ]

    df = spark.createDataFrame(data, ["id", "value"])

    transformer = SilverTransformation(df)
    result = transformer.fill_nulls()

    assert result.filter(col("id") == 0).count() == 1
    assert result.filter(col("value") == 0).count() == 1


def test_fill_nulls_no_nulls(spark):
    data = [
        (1, 2),
        (3, 4)
    ]

    df = spark.createDataFrame(data, ["id", "value"])

    transformer = SilverTransformation(df)
    result = transformer.fill_nulls()

    assert result.count() == 2
    assert result.filter(col("id") == 0).count() == 0
    assert result.filter(col("value") == 0).count() == 0


# ---------------------------
# fill_strnulls
# ---------------------------
def test_fill_strnulls(spark):
    data = [
        ("Alice",),
        (None,)
    ]

    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.fill_strnulls()

    assert result.filter(col("name") == "N/A").count() == 1


def test_fill_strnulls_no_nulls(spark):
    data = [
        ("Alice",),
        ("Bob",)
    ]

    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.fill_strnulls()

    assert result.filter(col("name") == "N/A").count() == 0


# ---------------------------
# drop_specificcols
# ---------------------------
def test_drop_specificcols(spark):
    data = [
        ("Alice", 25, "UK")
    ]

    df = spark.createDataFrame(
        data,
        ["name", "age", "country"]
    )

    transformer = SilverTransformation(df)
    result = transformer.drop_specificcols(["age"])

    assert "age" not in result.columns
    assert "name" in result.columns
    assert "country" in result.columns


def test_drop_specificcols_multiple_columns(spark):
    data = [
        ("Alice", 25, "UK")
    ]

    df = spark.createDataFrame(
        data,
        ["name", "age", "country"]
    )

    transformer = SilverTransformation(df)
    result = transformer.drop_specificcols(
        ["age", "country"]
    )

    assert "age" not in result.columns
    assert "country" not in result.columns
    assert "name" in result.columns


# ---------------------------
# split_values
# ---------------------------
def test_split_values(spark):
    data = [
        ("Alice-London-UK",),
        ("Bob-Manchester-UK",)
    ]

    df = spark.createDataFrame(
        data,
        ["location"]
    )

    transformer = SilverTransformation(df)

    result = transformer.split_values(
        "city",
        "location",
        "-",
        1
    )

    assert "city" in result.columns
    assert result.filter(col("city") == "London").count() == 1
    assert result.filter(col("city") == "Manchester").count() == 1


# ---------------------------
# drop_duplicatedata
# ---------------------------
def test_drop_duplicatedata(spark):
    data = [
        (1, "Alice"),
        (1, "Alice"),
        (2, "Bob")
    ]

    df = spark.createDataFrame(
        data,
        ["id", "name"]
    )

    transformer = SilverTransformation(df)
    result = transformer.drop_duplicatedata("id")

    assert result.count() == 2
    assert result.select("id").distinct().count() == 2


def test_drop_duplicatedata_no_duplicates(spark):
    data = [
        (1, "Alice"),
        (2, "Bob")
    ]

    df = spark.createDataFrame(
        data,
        ["id", "name"]
    )

    transformer = SilverTransformation(df)
    result = transformer.drop_duplicatedata("id")

    assert result.count() == 2


# ---------------------------
# cast_intcols
# ---------------------------
def test_cast_intcols(spark):
    data = [
        ("10",),
        ("20",),
        ("30",)
    ]

    df = spark.createDataFrame(
        data,
        ["quantity"]
    )

    transformer = SilverTransformation(df)
    result = transformer.cast_intcols(["quantity"])

    assert result.schema["quantity"].dataType.simpleString() == "int"

    assert result.filter(col("quantity") == 10).count() == 1
    assert result.filter(col("quantity") == 20).count() == 1
    assert result.filter(col("quantity") == 30).count() == 1


def test_cast_intcols_multiple_columns(spark):
    data = [
        ("10", "20"),
        ("30", "40")
    ]

    df = spark.createDataFrame(
        data,
        ["quantity", "age"]
    )

    transformer = SilverTransformation(df)

    result = transformer.cast_intcols(
        ["quantity", "age"]
    )

    assert result.schema["quantity"].dataType.simpleString() == "int"
    assert result.schema["age"].dataType.simpleString() == "int"


# ---------------------------
# cast_timestampcols
# ---------------------------
def test_cast_timestampcols(spark):
    data = [
        ("2026-01-01 10:00:00",),
        ("2026-01-02 11:00:00",)
    ]

    df = spark.createDataFrame(
        data,
        ["created_at"]
    )

    transformer = SilverTransformation(df)

    result = transformer.cast_timestampcols(
        ["created_at"]
    )

    assert (
        result.schema["created_at"]
        .dataType
        .simpleString()
        == "timestamp"
    )

    assert result.filter(
        col("created_at").isNull()
    ).count() == 0


# ---------------------------
# drop_specific_cols
# ---------------------------
def test_drop_specific_cols(spark):
    data = [
        ("Alice", 25, "UK")
    ]

    df = spark.createDataFrame(
        data,
        ["name", "age", "country"]
    )

    transformer = SilverTransformation(df)

    result = transformer.drop_specific_cols(
        ["age"]
    )

    assert "age" not in result.columns
    assert "name" in result.columns
    assert "country" in result.columns


def test_drop_specific_cols_multiple_columns(spark):
    data = [
        ("Alice", 25, "UK")
    ]

    df = spark.createDataFrame(
        data,
        ["name", "age", "country"]
    )

    transformer = SilverTransformation(df)

    result = transformer.drop_specific_cols(
        ["age", "country"]
    )

    assert "age" not in result.columns
    assert "country" not in result.columns
    assert "name" in result.columns


# ---------------------------
# cast_doublecols
# ---------------------------
def test_cast_doublecols(spark):
    data = [
        ("10.50",),
        ("20.75",),
        ("30.25",)
    ]

    df = spark.createDataFrame(
        data,
        ["price"]
    )

    transformer = SilverTransformation(df)

    result = transformer.cast_doublecols(
        ["price"]
    )

    assert (
        result.schema["price"]
        .dataType
        .simpleString()
        == "double"
    )

    assert result.filter(
        col("price") == 10.50
    ).count() == 1

    assert result.filter(
        col("price") == 20.75
    ).count() == 1


def test_cast_doublecols_multiple_columns(spark):
    data = [
        ("10.50", "100.25"),
        ("20.75", "200.50")
    ]

    df = spark.createDataFrame(
        data,
        ["price", "revenue"]
    )

    transformer = SilverTransformation(df)

    result = transformer.cast_doublecols(
        ["price", "revenue"]
    )

    assert (
        result.schema["price"]
        .dataType
        .simpleString()
        == "double"
    )

    assert (
        result.schema["revenue"]
        .dataType
        .simpleString()
        == "double"
    )


# ---------------------------
# rename_columns
# ---------------------------
def test_rename_columns(spark):
    data = [
        (1, "Alice")
    ]

    df = spark.createDataFrame(
        data,
        ["customer_id", "customer_name"]
    )

    transformer = SilverTransformation(df)

    result = transformer.rename_columns(
        {
            "customer_id": "id",
            "customer_name": "name"
        }
    )

    assert "customer_id" not in result.columns
    assert "customer_name" not in result.columns

    assert "id" in result.columns
    assert "name" in result.columns


def test_rename_single_column(spark):
    data = [
        (1, "Alice")
    ]

    df = spark.createDataFrame(
        data,
        ["customer_id", "name"]
    )

    transformer = SilverTransformation(df)

    result = transformer.rename_columns(
        {
            "customer_id": "id"
        }
    )

    assert "customer_id" not in result.columns
    assert "id" in result.columns
    assert "name" in result.columns