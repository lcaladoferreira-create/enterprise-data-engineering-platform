from spark.transformations.gold_layer import create_gold_star_schema
import os
import shutil
import importlib
import spark.utils.config as cfg_module

def test_gold_star_schema_materialization(spark, monkeypatch):
    """Verifies that gold dimensions and facts are correctly materialized."""
    silver_path = os.path.abspath("tests/test_gold_silver")
    gold_path = os.path.abspath("tests/test_gold_output")

    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    if os.path.exists(gold_path):
        shutil.rmtree(gold_path)

    # 8 columns each to match schemas
    tables = {
        "customers": [("c1", "John", "Doe", "NY", "USA", "2023-10-01 10:00:00", "b1", "s1")],
        "orders": [(1, "c1", "2023-10-01 10:00:00", 100.0, "completed", "2023-10-01 10:00:00", "b1", "s1")],
        "order_items": [(10, 1, 501, 2, 50.0, "2023-10-01 10:00:00", "b1", "s1")],
        "products": [(501, "Widget", "Gear", "Acme", 50.0, "2023-10-01 10:00:00", "b1", "s1")],
        "payments": [(20, 1, "2023-10-01 10:05:00", 100.0, "card", "tx1", "2023-10-01 10:00:00", "b1", "s1")],
        "invoices": [(1, 1, "2023-10-01 10:00:00", "2023-10-15", 100.0, "paid", "2023-10-01 10:00:00", "b1", "s1")]
    }

    schemas = {
        "customers": "customer_id string, first_name string, last_name string, city string, country string, processed_at string, batch_id string, source_system string",
        "orders": "order_id int, customer_id string, order_date string, total_amount double, status string, processed_at string, batch_id string, source_system string",
        "order_items": "order_item_id int, order_id int, product_id int, quantity int, unit_price double, processed_at string, batch_id string, source_system string",
        "products": "product_id int, name string, category string, brand string, price double, processed_at string, batch_id string, source_system string",
        "payments": "payment_id int, invoice_id int, payment_date string, amount double, payment_method string, transaction_id string, processed_at string, batch_id string, source_system string",
        "invoices": "invoice_id int, order_id int, invoice_date string, due_date string, amount double, status string, processed_at string, batch_id string, source_system string"
    }

    for name, data in tables.items():
        df = spark.createDataFrame(data, schemas[name])
        if "order_date" in df.columns:
            df = df.withColumn("order_date", df.order_date.cast("timestamp"))
        if "payment_date" in df.columns:
            df = df.withColumn("payment_date", df.payment_date.cast("timestamp"))

        target = f"{silver_path}/{name}"
        os.makedirs(target, exist_ok=True)
        df.write.mode("overwrite").parquet(target)

    monkeypatch.setenv("SILVER_PATH", silver_path)
    monkeypatch.setenv("GOLD_PATH", gold_path)
    importlib.reload(cfg_module)

    create_gold_star_schema(spark)

    assert os.path.exists(f"{gold_path}/dim_customers")
    assert spark.read.parquet(f"{gold_path}/fact_orders").count() == 1
    assert spark.read.parquet(f"{gold_path}/mart_sales_daily").collect()[0]["revenue"] == 100.0

    shutil.rmtree(silver_path)
    shutil.rmtree(gold_path)

def test_gold_star_schema_join_correctness(spark, monkeypatch):
    """Verifies that dimensions and facts correctly join for analysis."""
    silver_path = os.path.abspath("tests/test_gold_silver_j")
    gold_path = os.path.abspath("tests/test_gold_output_j")
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
    if os.path.exists(gold_path):
        shutil.rmtree(gold_path)

    tables = {
        "customers": [("c1", "John", "Doe", "NY", "USA", "2023-10-01 10:00:00", "b1", "s1")],
        "orders": [(1, "c1", "2023-10-01 10:00:00", 100.0, "completed", "2023-10-01 10:00:00", "b1", "s1")],
        "order_items": [(10, 1, 501, 2, 50.0, "2023-10-01 10:00:00", "b1", "s1")],
        "products": [(501, "Widget", "Gear", "Acme", 50.0, "2023-10-01 10:00:00", "b1", "s1")],
        "payments": [(20, 1, "2023-10-01 10:05:00", 100.0, "card", "tx1", "2023-10-01 10:00:00", "b1", "s1")],
        "invoices": [(1, 1, "2023-10-01 10:00:00", "2023-10-15", 100.0, "paid", "2023-10-01 10:00:00", "b1", "s1")]
    }
    schemas = {
        "customers": "customer_id string, first_name string, last_name string, city string, country string, processed_at string, batch_id string, source_system string",
        "orders": "order_id int, customer_id string, order_date string, total_amount double, status string, processed_at string, batch_id string, source_system string",
        "order_items": "order_item_id int, order_id int, product_id int, quantity int, unit_price double, processed_at string, batch_id string, source_system string",
        "products": "product_id int, name string, category string, brand string, price double, processed_at string, batch_id string, source_system string",
        "payments": "payment_id int, invoice_id int, payment_date string, amount double, payment_method string, transaction_id string, processed_at string, batch_id string, source_system string",
        "invoices": "invoice_id int, order_id int, invoice_date string, due_date string, amount double, status string, processed_at string, batch_id string, source_system string"
    }
    for name, data in tables.items():
        df = spark.createDataFrame(data, schemas[name])
        if "order_date" in df.columns:
            df = df.withColumn("order_date", df.order_date.cast("timestamp"))
        if "payment_date" in df.columns:
            df = df.withColumn("payment_date", df.payment_date.cast("timestamp"))
        target = f"{silver_path}/{name}"
        os.makedirs(target, exist_ok=True)
        df.write.mode("overwrite").parquet(target)

    monkeypatch.setenv("SILVER_PATH", silver_path)
    monkeypatch.setenv("GOLD_PATH", gold_path)
    importlib.reload(cfg_module)
    create_gold_star_schema(spark)

    # Verify CLV mart calculation
    clv = spark.read.parquet(f"{gold_path}/mart_customer_lifetime_value")
    # c1 has 1 order of 100
    assert clv.collect()[0]["lifetime_spend"] == 100.0

    shutil.rmtree(silver_path)
    shutil.rmtree(gold_path)
