import pytest
from spark.utils.data_quality import check_nulls, check_duplicates
from pyspark.sql import SparkSession
from pyspark.sql import Row

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark") \
        .getOrCreate()

def test_check_nulls(spark):
    data = [
        Row(id=1, name="Alice"),
        Row(id=2, name=None),
        Row(id=3, name="Bob")
    ]
    df = spark.createDataFrame(data)

    result = check_nulls(df, ["name"])
    assert "name" in result
    assert result["name"] == 1

def test_check_duplicates(spark):
    data = [
        Row(id=1, email="test@example.com"),
        Row(id=2, email="test@example.com"),
        Row(id=3, email="other@example.com")
    ]
    df = spark.createDataFrame(data)

    result = check_duplicates(df, ["email"])
    assert result == 1
