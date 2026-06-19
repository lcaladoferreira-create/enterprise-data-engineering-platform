import importlib
import json
import os
import shutil

import spark.utils.config as cfg_module
from spark.transformations.bronze_layer import process_bronze_layer


def test_bronze_schema_enforcement_valid(spark, monkeypatch):
    raw_path = os.path.abspath("tests/test_raw_v")
    bronze_path = os.path.abspath("tests/test_bronze_v")
    entity = "customers"
    if os.path.exists(raw_path):
        shutil.rmtree(raw_path)
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    # Clean watermark
    w_path = f"config/state/{entity}_high_watermark.txt"
    if os.path.exists(w_path):
        os.remove(w_path)

    os.makedirs(f"{raw_path}/{entity}", exist_ok=True)
    sample = {"customer_id": 1, "first_name": "A", "last_name": "B", "email": "a@b.com"}
    with open(f"{raw_path}/{entity}/data.json", "w") as f:
        f.write(json.dumps(sample))
    monkeypatch.setenv("RAW_PATH", raw_path)
    monkeypatch.setenv("BRONZE_PATH", bronze_path)
    importlib.reload(cfg_module)
    process_bronze_layer(spark, entity)
    assert os.path.exists(bronze_path)
    df = spark.read.format("delta").load(bronze_path)
    assert df.count() == 1
    shutil.rmtree(raw_path)
    shutil.rmtree(bronze_path)


def test_bronze_null_handling(spark, monkeypatch):
    raw_path = os.path.abspath("tests/test_raw_null")
    bronze_path = os.path.abspath("tests/test_bronze_null")
    entity = "orders"
    if os.path.exists(raw_path):
        shutil.rmtree(raw_path)
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    # Clean watermark
    w_path = f"config/state/{entity}_high_watermark.txt"
    if os.path.exists(w_path):
        os.remove(w_path)

    os.makedirs(f"{raw_path}/{entity}", exist_ok=True)
    sample = {"order_id": 1, "customer_id": 10, "order_date": "2023-10-01 10:00:00", "total_amount": 100.0}
    with open(f"{raw_path}/{entity}/data.json", "w") as f:
        f.write(json.dumps(sample))
    monkeypatch.setenv("RAW_PATH", raw_path)
    monkeypatch.setenv("BRONZE_PATH", bronze_path)
    importlib.reload(cfg_module)
    process_bronze_layer(spark, entity)
    df = spark.read.format("delta").load(bronze_path)
    assert df.collect()[0]["status"] is None
    shutil.rmtree(raw_path)
    shutil.rmtree(bronze_path)


def test_bronze_invalid_struct_type(spark, monkeypatch):
    raw_path = os.path.abspath("tests/test_raw_inv")
    bronze_path = os.path.abspath("tests/test_bronze_inv")
    entity = "products"
    if os.path.exists(raw_path):
        shutil.rmtree(raw_path)
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)
    # Clean watermark
    w_path = f"config/state/{entity}_high_watermark.txt"
    if os.path.exists(w_path):
        os.remove(w_path)

    os.makedirs(f"{raw_path}/{entity}", exist_ok=True)
    # product_id is INT in schema, sending STRING
    sample = {"product_id": "INVALID", "name": "Phone", "category": "X", "brand": "Y", "price": 10.0}
    with open(f"{raw_path}/{entity}/data.json", "w") as f:
        f.write(json.dumps(sample))
    monkeypatch.setenv("RAW_PATH", raw_path)
    monkeypatch.setenv("BRONZE_PATH", bronze_path)
    importlib.reload(cfg_module)
    process_bronze_layer(spark, entity)
    df = spark.read.format("delta").load(bronze_path)
    assert df.collect()[0]["product_id"] is None
    shutil.rmtree(raw_path)
    shutil.rmtree(bronze_path)
