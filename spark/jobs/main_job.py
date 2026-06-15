import sys
from pyspark.sql import SparkSession
from spark.transformations.bronze_layer import process_bronze_layer
from spark.transformations.silver_layer import process_silver_layer
from spark.transformations.gold_layer import create_gold_customer_orders, create_gold_product_sales

def main():
    if len(sys.argv) < 2:
        print("Usage: main_job.py <layer> [entity]")
        sys.exit(1)

    layer = sys.argv[1]
    spark = SparkSession.builder \
        .appName(f"DataPipeline_{layer}") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    base_data_path = "data"
    raw_path = f"{base_data_path}/raw"
    bronze_path = f"{base_data_path}/bronze"
    silver_path = f"{base_data_path}/silver"
    gold_path = f"{base_data_path}/gold"

    if layer == "bronze":
        entity = sys.argv[2] if len(sys.argv) > 2 else None
        entities = [entity] if entity else ["customers", "orders", "order_items", "products", "payments", "invoices", "user_activity", "logs"]
        for e in entities:
            process_bronze_layer(spark, raw_path, bronze_path, e)

    elif layer == "silver":
        entity = sys.argv[2] if len(sys.argv) > 2 else None
        # Entity to primary key mapping
        entity_pk_map = {
            "customers": "customer_id",
            "orders": "order_id",
            "order_items": "order_item_id",
            "products": "product_id",
            "payments": "payment_id",
            "invoices": "invoice_id",
            "user_activity": "user_id", # Simplified
            "logs": "log_id"
        }

        if entity:
            process_silver_layer(spark, bronze_path, silver_path, entity, entity_pk_map.get(entity, "id"))
        else:
            for e, pk in entity_pk_map.items():
                process_silver_layer(spark, bronze_path, silver_path, e, pk)

    elif layer == "gold":
        create_gold_customer_orders(spark, silver_path, gold_path)
        create_gold_product_sales(spark, silver_path, gold_path)

    spark.stop()

if __name__ == "__main__":
    main()
