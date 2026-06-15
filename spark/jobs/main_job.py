import sys
from pyspark.sql import SparkSession
from spark.transformations.bronze_layer import process_bronze_layer
from spark.transformations.silver_layer import process_silver_layer
from spark.transformations.gold_layer import create_gold_star_schema
from spark.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """
    Main entry point for the Enterprise Data Pipeline Spark application.
    Orchestrates Bronze, Silver, and Gold processing layers.
    """
    if len(sys.argv) < 2:
        logger.error("Missing mandatory arguments. Usage: main_job.py <layer> [entity]")
        sys.exit(1)

    layer = sys.argv[1]

    spark = SparkSession.builder \
        .appName(f"Enterprise_Data_Pipeline_{layer}") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    try:
        if layer == "bronze":
            if len(sys.argv) < 3:
                logger.error("Bronze layer requires an entity name argument.")
                sys.exit(1)
            entity = sys.argv[2]
            process_bronze_layer(spark, entity)

        elif layer == "silver":
            if len(sys.argv) < 3:
                logger.error("Silver layer requires an entity name argument.")
                sys.exit(1)
            entity = sys.argv[2]

            # Configuration for entities
            configs = {
                "customers": {
                    "pk": "customer_id",
                    "pii": ["email", "phone", "address"],
                    "critical": ["email"]
                },
                "orders": {
                    "pk": "order_id",
                    "critical": ["customer_id", "total_amount"]
                },
                "products": {
                    "pk": "product_id",
                    "critical": ["name", "price"]
                },
                "order_items": {
                    "pk": "order_item_id",
                    "critical": ["order_id", "product_id"]
                },
                "payments": {
                    "pk": "payment_id",
                    "critical": ["invoice_id", "amount"]
                },
                "invoices": {
                    "pk": "invoice_id",
                    "critical": ["order_id"]
                }
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
            logger.error(f"Unsupported processing layer: {layer}")
            sys.exit(1)

        logger.info(f"Pipeline execution for layer '{layer}' completed successfully.")

    except Exception as e:
        logger.error(f"Critical failure in pipeline job: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
