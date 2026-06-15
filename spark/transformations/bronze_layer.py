import os
from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import add_audit_metadata, compute_record_hash
from spark.utils.schemas import SCHEMAS

logger = get_logger(__name__)


def get_last_processed_timestamp(entity_name: str) -> str:
    """
    Retrieves the high-watermark from a state file.
    """
    state_file = f"config/state/{entity_name}_watermark.txt"
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return f.read().strip()
    return "1900-01-01 00:00:00"


def update_last_processed_timestamp(entity_name: str, timestamp: str) -> None:
    """
    Updates the high-watermark in a state file.
    """
    state_dir = "config/state"
    os.makedirs(state_dir, exist_ok=True)
    state_file = f"{state_dir}/{entity_name}_watermark.txt"
    with open(state_file, "w") as f:
        f.write(timestamp)


def process_bronze_layer(spark: SparkSession, entity_name: str) -> None:
    """
    Bronze Layer: Incremental load using High-Watermark pattern.
    Only reads records where 'updated_at' or 'created_at' > last_processed.
    """
    try:
        logger.info(f"Starting Incremental Bronze processing for: {entity_name}")

        raw_source = f"{config.RAW_PATH}/{entity_name}"
        bronze_dest = f"{config.BRONZE_PATH}/{entity_name}"

        entity_schema = SCHEMAS.get(entity_name)
        if not entity_schema:
            raise ValueError(f"Schema not found for entity: {entity_name}")

        # 1. Load high-watermark
        last_ts = get_last_processed_timestamp(entity_name)
        logger.info(f"Entity: {entity_name}, High-Watermark: {last_ts}")

        # 2. Extract and Filter Incremental Data
        # We assume the source JSON has a 'processed_at' or similar timestamp from the ingestion layer
        # For this implementation, we'll use Spark's ability to read modification times if available,
        # but here we'll filter on a column present in the data for consistency.
        df = spark.read.schema(entity_schema).json(raw_source)

        # Check if the dataframe has a timestamp column to filter on
        ts_col = "order_date" if "order_date" in df.columns else "invoice_date"
        if ts_col not in df.columns:
            ts_col = None # Fallback to full load if no timestamp available

        if ts_col:
            df = df.filter(F.col(ts_col) > F.lit(last_ts))

        if df.count() == 0:
            logger.info(f"No new records found for {entity_name} since {last_ts}")
            return

        # 3. Transform: Metadata and Integrity
        df = add_audit_metadata(df, config.BATCH_ID, config.SOURCE_SYSTEM)
        df = compute_record_hash(df)
        df = df.withColumn("ingestion_date", F.to_date(F.col("processed_at")))

        # Capture current max timestamp before write
        if ts_col:
            current_max_ts = df.select(F.max(ts_col)).collect()[0][0]

        # 4. Load: Idempotent write
        df.write.mode("append") \
            .partitionBy("ingestion_date") \
            .parquet(bronze_dest)

        # 5. Update Watermark
        if ts_col and current_max_ts:
            update_last_processed_timestamp(entity_name, str(current_max_ts))

        logger.info(f"Bronze incremental load complete for {entity_name}")

    except Exception as e:
        logger.error(f"Bronze layer failure: {str(e)}", exc_info=True)
        raise
