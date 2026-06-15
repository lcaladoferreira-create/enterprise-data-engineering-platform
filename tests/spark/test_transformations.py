import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark.transformations.silver_layer import process_silver_layer
import os
import shutil

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark-transformations") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_silver_transformation_deduplication(spark):
    # Setup temporary directories
    bronze_path = "tests/temp_bronze"
    silver_path = "tests/temp_silver"
    entity = "customers"

    if os.path.exists(bronze_path): shutil.rmtree(bronze_path)
    if os.path.exists(silver_path): shutil.rmtree(silver_path)

    os.makedirs(f"{bronze_path}/{entity}", exist_ok=True)

    # Create sample data
    data = [
        (1, "John", "2023-01-01 10:00:00"),
        (1, "John Doe", "2023-01-01 11:00:00")
    ]
    columns = ["customer_id", "name", "ingestion_timestamp"]
    df = spark.createDataFrame(data, columns)

    # Cast to correct types
    df = df.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))

    # Save as parquet to simulate bronze layer
    df.write.mode("overwrite").parquet(f"{bronze_path}/{entity}")

    # Run silver transformation
    process_silver_layer(spark, bronze_path, silver_path, entity, "customer_id")

    # Verify result
    result_df = spark.read.parquet(f"{silver_path}/{entity}")

    assert result_df.count() == 1

    # Sort and check name
    row = result_df.collect()[0]
    assert row["name"] == "John Doe"

    # Cleanup
    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)
