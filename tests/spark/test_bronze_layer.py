import pytest
from pyspark.sql import SparkSession
from spark.transformations.bronze_layer import process_bronze_layer
import os
import shutil
import json
import importlib
import spark.utils.config

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-bronze-layer") \
        .getOrCreate()

def test_bronze_layer_schema_enforcement(spark_session, monkeypatch):
    # Setup
    raw_path = "tests/test_raw"
    bronze_path = "tests/test_bronze"
    entity = "customers"

    # Fully qualified absolute paths for Spark
    abs_raw = os.path.abspath(raw_path)
    abs_bronze = os.path.abspath(bronze_path)

    if os.path.exists(abs_raw):
        shutil.rmtree(abs_raw)
    if os.path.exists(abs_bronze):
        shutil.rmtree(abs_bronze)

    os.makedirs(f"{abs_raw}/{entity}", exist_ok=True)
    sample_data = {"customer_id": 1, "first_name": "John", "last_name": "Doe", "email": "john@example.com"}

    with open(f"{abs_raw}/{entity}/data.json", "w") as f:
        f.write(json.dumps(sample_data))

    monkeypatch.setenv("RAW_PATH", abs_raw)
    monkeypatch.setenv("BRONZE_PATH", abs_bronze)

    # Reload config to pickup monkeypatch
    importlib.reload(spark.utils.config)

    # Run
    process_bronze_layer(spark_session, entity)

    # Verify
    assert os.path.exists(abs_bronze)
    df = spark_session.read.parquet(abs_bronze)

    # Check for metadata columns added by transformation
    assert "batch_id" in df.columns
    assert "record_hash" in df.columns
    assert "ingestion_date" in df.columns

    # Cleanup
    shutil.rmtree(abs_raw)
    shutil.rmtree(abs_bronze)
    if os.path.exists(f"config/state/{entity}_watermark.txt"):
        os.remove(f"config/state/{entity}_watermark.txt")
