from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import check_nulls, mask_pii

logger = get_logger(__name__)

def process_silver_layer(spark: SparkSession, entity_name: str, pk_col: str, pii_cols: list = None, critical_cols: list = None, bronze_path: str = None, silver_path: str = None) -> None:
    """
    Silver Layer: Deduplication, Cleaning, and PII Masking.
    Implements a quarantine pattern for records failing DQ checks.
    """
    try:
        logger.info(f"Starting Silver layer processing for: {entity_name}")

        bronze_source = bronze_path if bronze_path else f"{config.BRONZE_PATH}/{entity_name}"
        silver_dest = silver_path if silver_path else f"{config.SILVER_PATH}/{entity_name}"
        quarantine_dest = f"{config.SILVER_PATH}/quarantine/{entity_name}"

        df = spark.read.parquet(bronze_source)

        # 1. Deduplication (Rank by processing time)
        window_spec = Window.partitionBy(pk_col).orderBy(F.col("processed_at").desc())
        df = df.withColumn("row_num", F.row_number().over(window_spec)) \
               .filter(F.col("row_num") == 1) \
               .drop("row_num")

        # 2. Data Quality Check
        if critical_cols:
            df = check_nulls(df, critical_cols)

            # Quarantine failed records
            quarantine_df = df.filter(F.col("dq_failed"))
            if quarantine_df.count() > 0:
                logger.warning(f"Quarantining {quarantine_df.count()} records for {entity_name}")
                quarantine_df.write.mode("append").parquet(quarantine_dest)

            # Continue with valid records
            df = df.filter(~F.col("dq_failed")).drop("dq_failed", "dq_reason")

        # 3. Standardization
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        # 4. PII Masking
        if pii_cols:
            df = mask_pii(df, pii_cols)

        # Write to silver
        df.write.mode(config.WRITE_MODE).parquet(silver_dest)
        logger.info(f"Silver layer completed for {entity_name}.")

    except Exception as e:
        logger.error(f"Error processing Silver layer for {entity_name}: {str(e)}")
        raise
