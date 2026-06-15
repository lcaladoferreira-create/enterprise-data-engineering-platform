from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import check_nulls, mask_pii

logger = get_logger(__name__)


def process_silver_layer(
    spark: SparkSession,
    entity_name: str,
    pk_col: str,
    pii_cols: list = None,
    critical_cols: list = None,
    bronze_path: str = None,
    silver_path: str = None
) -> None:
    """
    Silver Layer: Implementation of cleaning, deduplication, and data quality standards.

    Operations:
    - Primary Key based deduplication keeping latest metadata.
    - Data Quality enforcement using a Quarantine pattern.
    - String standardization (trimming, case normalization).
    - Hashing of PII fields (GDPR/LGPD compliance).
    """
    try:
        logger.info(f"Initiating Silver layer processing for entity: {entity_name}")

        bronze_source = bronze_path if bronze_path else f"{config.BRONZE_PATH}/{entity_name}"
        silver_dest = silver_path if silver_path else f"{config.SILVER_PATH}/{entity_name}"
        quarantine_dest = f"{config.SILVER_PATH}/quarantine/{entity_name}"

        df = spark.read.parquet(bronze_source)

        # 1. Deduplication: Keep the most recent record version based on audit timestamp
        window_spec = Window.partitionBy(pk_col).orderBy(F.col("processed_at").desc())
        df = df.withColumn("row_num", F.row_number().over(window_spec)) \
               .filter(F.col("row_num") == 1) \
               .drop("row_num")

        # 2. Data Quality: Enforce critical column constraints
        if critical_cols:
            df = check_nulls(df, critical_cols)

            # Action: Quarantine records failing validation
            quarantine_df = df.filter(F.col("dq_failed"))
            if quarantine_df.count() > 0:
                logger.warning(f"Isolating {quarantine_df.count()} failed records to quarantine.")
                quarantine_df.write.mode("append").parquet(quarantine_dest)

            # Proceed only with valid records
            df = df.filter(~F.col("dq_failed")).drop("dq_failed", "dq_reason")

        # 3. Standardization: Standardizing string representations
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        # 4. Security: Anonymize sensitive fields
        if pii_cols:
            df = mask_pii(df, pii_cols)

        # Persistence
        df.write.mode(config.WRITE_MODE).parquet(silver_dest)
        logger.info(f"Silver layer processing successful for {entity_name}.")

    except Exception as e:
        logger.error(f"Silver layer processing failed for {entity_name}: {str(e)}", exc_info=True)
        raise
