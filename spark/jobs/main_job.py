import sys
from pyspark.sql import SparkSession
from spark.transformations.bronze_layer import process_bronze_layer
from spark.transformations.silver_layer import process_silver_layer
from spark.transformations.gold_layer import create_gold_star_schema
from spark.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    if len(sys.argv) < 2:
        logger.error("Missing arguments. Usage: main_job.py <layer> [entity]")
        sys.exit(1)

    layer = sys.argv[1]

    spark = SparkSession.builder \
        .appName(f"Enterprise_Data_Pipeline_{layer}") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    try:
        if layer == "bronze":
            entity = sys.argv[2]
            process_bronze_layer(spark, entity)

        elif layer == "silver":
            entity = sys.argv[2]
            # Production PK and PII config
            configs = {
                "customers": {"pk": "customer_id", "pii": ["email", "phone", "address"], "critical": ["email"]},
                "orders": {"pk": "order_id", "critical": ["customer_id", "total_amount"]},
                "products": {"pk": "product_id", "critical": ["name", "price"]},
                "order_items": {"pk": "order_item_id", "critical": ["order_id", "product_id"]},
                "payments": {"pk": "payment_id", "critical": ["invoice_id", "amount"]},
                "invoices": {"pk": "invoice_id", "critical": ["order_id"]}
            }
            cfg = configs.get(entity, {"pk": "id"})
            process_silver_layer(
                spark,
                entity,
                cfg["pk"],
                cfg.get("pii"),
                cfg.get("critical")
            )

        elif layer == "gold":
            create_gold_star_schema(spark)

        else:
            logger.error(f"Unknown layer: {layer}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Job failed during {layer} processing: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
