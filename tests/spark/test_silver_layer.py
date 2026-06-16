from pyspark.sql import functions as F
from spark.transformations.silver_layer import process_silver_layer
import os
import shutil

def test_silver_pii_masking(spark, monkeypatch):
    bronze_path = os.path.abspath("tests/test_silver_bronze_pii")
    silver_path = os.path.abspath("tests/test_silver_output_pii")
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    os.makedirs(bronze_path, exist_ok=True)
    data = [(1, "secret@example.com", "123-456", "2023-01-01 10:00:00", "b1", "s1")]
    cols = ["customer_id", "email", "phone", "processed_at", "batch_id", "source_system"]
    df = spark.createDataFrame(data, cols).withColumn("processed_at", F.to_timestamp("processed_at"))
    df.write.mode("overwrite").parquet(bronze_path)
    process_silver_layer(spark, "customers", "customer_id", pii_cols=["email"], bronze_path=bronze_path, silver_path=silver_path)
    result = spark.read.parquet(silver_path).collect()[0]
    assert result["email"] != "secret@example.com"
    assert len(result["email"]) == 64
    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)

def test_silver_quarantine_isolation(spark, monkeypatch):
    bronze_path = os.path.abspath("tests/test_silver_bronze_q")
    silver_path = os.path.abspath("tests/test_silver_output_q")
    quarantine_path = os.path.abspath("tests/test_silver_quarantine_q")

    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    if os.path.exists(quarantine_path):
        shutil.rmtree(quarantine_path)
    os.makedirs(bronze_path, exist_ok=True)

    data = [(1, "Valid", "2023-01-01 10:00:00", "b1", "s1"), (2, None, "2023-01-01 10:00:00", "b1", "s1")]
    cols = ["product_id", "name", "processed_at", "batch_id", "source_system"]
    df = spark.createDataFrame(data, cols).withColumn("processed_at", F.to_timestamp("processed_at"))
    df.write.mode("overwrite").parquet(bronze_path)

    process_silver_layer(
        spark,
        "products",
        "product_id",
        critical_cols=["name"],
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path
    )

    assert spark.read.parquet(silver_path).count() == 1
    assert os.path.exists(quarantine_path)
    assert spark.read.parquet(quarantine_path).count() == 1

    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)
    shutil.rmtree(quarantine_path)

def test_silver_deduplication(spark, monkeypatch):
    """Verifies that only the latest record per PK is kept."""
    bronze_path = os.path.abspath("tests/test_silver_bronze_dedup")
    silver_path = os.path.abspath("tests/test_silver_output_dedup")

    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    os.makedirs(bronze_path, exist_ok=True)

    # Same ID, different processing times
    data = [
        (101, 100.0, "2023-01-01 10:00:00", "b1", "s1"),
        (101, 150.0, "2023-01-01 11:00:00", "b2", "s1")
    ]
    cols = ["order_id", "total_amount", "processed_at", "batch_id", "source_system"]
    df = spark.createDataFrame(data, cols).withColumn("processed_at", F.to_timestamp("processed_at"))
    df.write.mode("overwrite").parquet(bronze_path)

    process_silver_layer(spark, "orders", "order_id", bronze_path=bronze_path, silver_path=silver_path)

    result_df = spark.read.parquet(silver_path)
    assert result_df.count() == 1
    assert result_df.collect()[0]["total_amount"] == 150.0

    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)
