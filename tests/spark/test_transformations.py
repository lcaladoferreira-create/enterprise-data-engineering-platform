import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark.transformations.silver_layer import process_silver_layer
import os
import shutil

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark-transformations") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_silver_transformation_deduplication(spark_session):
    # Setup temporary directories
    bronze_path = "tests/temp_bronze_customers"
    silver_path = "tests/temp_silver_customers"
    entity = "customers"

    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)

    os.makedirs(bronze_path, exist_ok=True)

    # Create sample data
    data = [
        (1, "John", "2023-01-01 10:00:00", "local_dev", "source"),
        (1, "John Doe", "2023-01-01 11:00:00", "local_dev", "source")
    ]
    columns = ["customer_id", "name", "processed_at", "batch_id", "source_system"]
    df = spark_session.createDataFrame(data, columns)

    # Cast to correct types
    df = df.withColumn("processed_at", F.to_timestamp("processed_at"))

    # Save as parquet to simulate bronze layer
    df.write.mode("overwrite").parquet(bronze_path)

    # Run silver transformation
    process_silver_layer(
        spark_session,
        entity,
        "customer_id",
        critical_cols=["name"],
        bronze_path=bronze_path,
        silver_path=silver_path
    )

    # Verify result
    result_df = spark_session.read.parquet(silver_path)

    assert result_df.count() == 1

    # Check that the latest record was kept
    row = result_df.collect()[0]
    assert row["name"] == "John Doe"

    # Cleanup
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
