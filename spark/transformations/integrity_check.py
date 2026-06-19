from pyspark.sql import SparkSession

from spark.utils.logging import get_logger

logger = get_logger("integrity_check")


def verify_integrity(spark: SparkSession) -> None:
    """
    Data Quality Audit: Ensures record counts are consistent across Silver and Gold.
    """
    try:
        # Check if Silver and Gold orders match within threshold using Delta format
        # Using paths consistent with Spark container mapping
        silver_orders_path = "/opt/bitnami/spark/data/silver/orders"
        gold_fact_orders_path = "/opt/bitnami/spark/data/gold/fact_orders"

        logger.info(f"Loading Silver orders from {silver_orders_path} and Gold fact orders from {gold_fact_orders_path}")

        silver_count = spark.read.format("delta").load(silver_orders_path).count()
        gold_count = spark.read.format("delta").load(gold_fact_orders_path).count()

        logger.info(f"Integrity Check: Silver Orders = {silver_count}, Gold Fact Orders = {gold_count}")

        if gold_count == 0:
            raise ValueError("CRITICAL: Gold layer is empty.")

        if gold_count < (silver_count * 0.95):
            raise ValueError(f"CRITICAL: Data loss detected. Gold count {gold_count} is less than 95% of Silver count {silver_count}.")

        logger.info("Integrity check passed.")

    except Exception as e:
        logger.error(f"Integrity check failed: {str(e)}")
        raise
