import pytest
from spark.utils.data_quality import check_nulls, mask_pii
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark-dq") \
        .getOrCreate()

def test_check_nulls_happy_path(spark):
    data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
    df = spark.createDataFrame(data)
    result_df = check_nulls(df, ["name"])
    assert result_df.filter(F.col("dq_failed")).count() == 0

def test_check_nulls_with_fails(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "Alice"), (2, None)]
    df = spark.createDataFrame(data, schema=schema)
    result_df = check_nulls(df, ["name"])
    assert result_df.filter(F.col("dq_failed")).count() == 1

def test_check_nulls_all_null(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, None), (2, None)]
    df = spark.createDataFrame(data, schema=schema)
    result_df = check_nulls(df, ["name"])
    assert result_df.filter(F.col("dq_failed")).count() == 2

def test_check_nulls_empty_input(spark):
    df = spark.createDataFrame([], "id int, name string")
    result_df = check_nulls(df, ["name"])
    assert result_df.count() == 0

def test_mask_pii_happy_path(spark):
    data = [Row(email="test@example.com")]
    df = spark.createDataFrame(data)
    result_df = mask_pii(df, ["email"])
    val = result_df.collect()[0]["email"]
    assert val != "test@example.com"
    assert len(val) == 64 # SHA-256 length

def test_mask_pii_all_null(spark):
    schema = StructType([StructField("email", StringType(), True)])
    data = [(None,)]
    df = spark.createDataFrame(data, schema=schema)
    result_df = mask_pii(df, ["email"])
    assert result_df.collect()[0]["email"] is None

def test_mask_pii_empty_input(spark):
    df = spark.createDataFrame([], "email string")
    result_df = mask_pii(df, ["email"])
    assert result_df.count() == 0
