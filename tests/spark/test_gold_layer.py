import importlib
import os
import shutil

import spark.utils.config as cfg_module
from spark.transformations.gold_layer import create_gold_star_schema


def test_gold_star_schema_materialization(spark, monkeypatch):
    silver_path = os.path.abspath("tests/test_silver_gold")
    gold_path = os.path.abspath("tests/test_gold_gold")
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    if os.path.exists(gold_path):
        shutil.rmtree(gold_path)

    os.makedirs(silver_path, exist_ok=True)

    # Create required silver tables in Delta format
    entities = ["customers", "orders", "order_items", "products", "payments", "invoices"]
    for entity in entities:
        if entity == "customers":
            data = [(1, "John", "Doe", "NY", "USA", "2023-10-01 10:00:00")]
            cols = ["customer_id", "first_name", "last_name", "city", "country", "processed_at"]
        elif entity == "orders":
            import datetime
            data = [(101, 1, datetime.date(2023, 10, 1), 100.0, "COMPLETED", "2023-10-01 10:00:00")]
            cols = ["order_id", "customer_id", "order_date", "total_amount", "status", "processed_at"]
        elif entity == "products":
            data = [(501, "Laptop", "Tech", "Apple", "2023-10-01 10:00:00")]
            cols = ["product_id", "name", "category", "brand", "processed_at"]
        elif entity == "order_items":
            data = [(1001, 101, 501, 1, 100.0, "2023-10-01 10:00:00")]
            cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "processed_at"]
        elif entity == "payments":
            import datetime
            data = [(2001, 3001, datetime.date(2023, 10, 2), 100.0, "CREDIT_CARD", "TX123", "2023-10-01 10:00:00")]
            cols = ["payment_id", "invoice_id", "payment_date", "amount", "payment_method", "transaction_id", "processed_at"]
        elif entity == "invoices":
            data = [(3001, 101, 100.0, "2023-10-01 10:00:00")]
            cols = ["invoice_id", "order_id", "amount", "processed_at"]

        df = spark.createDataFrame(data, cols)
        df.write.format("delta").save(f"{silver_path}/{entity}")

    monkeypatch.setenv("SILVER_PATH", silver_path)
    monkeypatch.setenv("GOLD_PATH", gold_path)
    importlib.reload(cfg_module)

    create_gold_star_schema(spark)

    # Verify fact_orders exists in Delta format
    assert os.path.exists(f"{gold_path}/fact_orders")
    fact_orders = spark.read.format("delta").load(f"{gold_path}/fact_orders")
    assert fact_orders.count() == 1

    shutil.rmtree(silver_path)
    shutil.rmtree(gold_path)

def test_gold_empty_orders_graceful(spark, monkeypatch):
    silver_path = os.path.abspath("tests/test_silver_empty")
    gold_path = os.path.abspath("tests/test_gold_empty")
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    if os.path.exists(gold_path):
        shutil.rmtree(gold_path)

    os.makedirs(silver_path, exist_ok=True)

    entities = ["customers", "orders", "order_items", "products", "payments", "invoices"]
    for entity in entities:
        # Create schema for empty tables
        if entity == "orders":
            data = []
            from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
            schema = StructType([
                StructField("order_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("order_date", DateType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("processed_at", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
        else:
            # Simplified for others
            df = spark.createDataFrame([ (1, "dummy") ], ["id", "val"]).limit(0)

        df.write.format("delta").save(f"{silver_path}/{entity}")

    monkeypatch.setenv("SILVER_PATH", silver_path)
    monkeypatch.setenv("GOLD_PATH", gold_path)
    importlib.reload(cfg_module)

    # Should not raise exception
    create_gold_star_schema(spark)

    assert not os.path.exists(f"{gold_path}/dim_dates")

    shutil.rmtree(silver_path)
    shutil.rmtree(gold_path)
