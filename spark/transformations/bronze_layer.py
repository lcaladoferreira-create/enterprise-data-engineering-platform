import os
from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import add_audit_metadata, compute_record_hash
from spark.utils.schemas import SCHEMAS

logger = get_logger(__name__)


def get_high_watermark(entity_name: str) -> str:
    """
    Retrieves the last processed timestamp for an entity.
    """
    state_file = f"config/state/{entity_name}_high_watermark.txt"
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return f.read().strip()
    return "1970-01-01 00:00:00"


def set_high_watermark(entity_name: str, timestamp: str) -> None:
    """
    Saves the latest processed timestamp for an entity.
    """
    state_dir = "config/state"
    os.makedirs(state_dir, exist_ok=True)
    state_file = f"{state_dir}/{entity_name}_high_watermark.txt"
    with open(state_file, "w") as f:
        f.write(timestamp)


def process_bronze_layer(spark: SparkSession, entity_name: str) -> None:
    """
    Bronze Layer: Incremental ingestion of raw JSON into Parquet.
    """
    try:
        logger.info(f"Starting Bronze processing for: {entity_name}")

        raw_source = f"{config.RAW_PATH}/{entity_name}"
        bronze_dest = f"{config.BRONZE_PATH}/{entity_name}"

        entity_schema = SCHEMAS.get(entity_name)
        if not entity_schema:
            raise ValueError(f"Schema not found for entity: {entity_name}")

        last_processed_ts = get_high_watermark(entity_name)
        logger.info(f"Incremental Load - High-watermark for {entity_name}: {last_processed_ts}")

        raw_df = spark.read.schema(entity_schema).json(raw_source)

        watermark_col = "order_date" if "order_date" in raw_df.columns else "invoice_date"

        if watermark_col in raw_df.columns:
            raw_df = raw_df.filter(F.col(watermark_col) > F.lit(last_processed_ts))

        if raw_df.count() == 0:
            logger.info(f"No new records found for {entity_name}.")
            return

        df = add_audit_metadata(raw_df, config.BATCH_ID, config.SOURCE_SYSTEM)
        df = compute_record_hash(df)
        df = df.withColumn("ingestion_date", F.to_date(F.col("processed_at")))

        if watermark_col in df.columns:
            new_watermark = df.select(F.max(watermark_col)).collect()[0][0]
        else:
            new_watermark = None

        df.write.mode("append").partitionBy("ingestion_date").parquet(bronze_dest)

        if new_watermark:
            set_high_watermark(entity_name, str(new_watermark))

        logger.info(f"Bronze layer load complete for {entity_name}")

    except Exception as e:
        logger.error(f"Bronze layer failure for {entity_name}: {str(e)}", exc_info=True)
        raise
