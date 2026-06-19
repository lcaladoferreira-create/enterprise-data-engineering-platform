import importlib
import os
import shutil

import spark.utils.config as cfg_module
from spark.transformations.silver_layer import process_silver_layer


def test_silver_pii_masking(spark, monkeypatch):
    bronze_path = os.path.abspath("tests/test_bronze_pii")
    silver_path = os.path.abspath("tests/test_silver_pii")
    entity = "customers"
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)

    os.makedirs(bronze_path, exist_ok=True)
    data = [
        (1, "John", "Doe", "john@example.com", "2023-10-01 10:00:00"),
        (2, "Jane", "Smith", "jane@example.com", "2023-10-01 11:00:00")
    ]
    df = spark.createDataFrame(data, ["customer_id", "first_name", "last_name", "email", "processed_at"])
    df.write.format("delta").save(f"{bronze_path}/{entity}")

    monkeypatch.setenv("BRONZE_PATH", bronze_path)
    monkeypatch.setenv("SILVER_PATH", silver_path)
    importlib.reload(cfg_module)

    process_silver_layer(spark, entity, "customer_id", pii_cols=["email"])

    silver_df = spark.read.format("delta").load(f"{silver_path}/{entity}")
    emails = [row["email"] for row in silver_df.select("email").collect()]

    for email in emails:
        # Check if it looks like a SHA-256 hash
        assert len(email) == 64
        assert email != "john@example.com"
        assert email != "jane@example.com"

    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)

def test_silver_deduplication(spark, monkeypatch):
    bronze_path = os.path.abspath("tests/test_bronze_dedup")
    silver_path = os.path.abspath("tests/test_silver_dedup")
    entity = "orders"
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)

    os.makedirs(bronze_path, exist_ok=True)
    # Duplicate ID with different processed_at
    data = [
        (101, 50.0, "2023-10-01 10:00:00"),
        (101, 55.0, "2023-10-01 12:00:00")
    ]
    df = spark.createDataFrame(data, ["order_id", "amount", "processed_at"])
    df.write.format("delta").save(f"{bronze_path}/{entity}")

    monkeypatch.setenv("BRONZE_PATH", bronze_path)
    monkeypatch.setenv("SILVER_PATH", silver_path)
    importlib.reload(cfg_module)

    process_silver_layer(spark, entity, "order_id")

    silver_df = spark.read.format("delta").load(f"{silver_path}/{entity}")
    assert silver_df.count() == 1
    assert silver_df.collect()[0]["amount"] == 55.0

    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)

def test_silver_quarantine(spark, monkeypatch):
    bronze_path = os.path.abspath("tests/test_bronze_qua")
    silver_path = os.path.abspath("tests/test_silver_qua")
    entity = "products"
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)

    os.makedirs(bronze_path, exist_ok=True)
    # Price is null, should fail DQ
    data = [
        (1, "Laptop", None, "2023-10-01 10:00:00"),
        (2, "Mouse", 25.0, "2023-10-01 10:00:00")
    ]
    df = spark.createDataFrame(data, ["product_id", "name", "price", "processed_at"])
    df.write.format("delta").save(f"{bronze_path}/{entity}")

    monkeypatch.setenv("BRONZE_PATH", bronze_path)
    monkeypatch.setenv("SILVER_PATH", silver_path)
    importlib.reload(cfg_module)

    process_silver_layer(spark, entity, "product_id", critical_cols=["price"])

    silver_df = spark.read.format("delta").load(f"{silver_path}/{entity}")
    assert silver_df.count() == 1
    assert silver_df.collect()[0]["name"] == "Mouse"

    quarantine_df = spark.read.format("delta").load(f"{silver_path}/quarantine/{entity}")
    assert quarantine_df.count() == 1
    assert quarantine_df.collect()[0]["name"] == "Laptop"

    shutil.rmtree(bronze_path)
    shutil.rmtree(silver_path)
