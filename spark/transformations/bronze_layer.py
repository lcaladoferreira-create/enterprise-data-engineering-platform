import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.utils.config import config
from spark.utils.data_quality import add_audit_metadata, compute_record_hash
from spark.utils.logging import get_logger
from spark.utils.schemas import SCHEMAS

logger = get_logger("bronze_layer")


def get_high_watermark(entity_name: str) -> str:
    """
    Retrieves the last processed timestamp for an entity.
    """
    filename = f"{entity_name}_high_watermark.txt"
    default_ts = "1970-01-01 00:00:00"

    try:
        if config.STATE_BUCKET.startswith("gs://") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            from google.cloud import storage

            bucket_name = config.STATE_BUCKET.replace("gs://", "")
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"state/{filename}")
            if blob.exists():
                return blob.download_as_text().strip()
        else:
            state_file = f"config/state/{filename}"
            if os.path.exists(state_file):
                with open(state_file, "r") as f:
                    return f.read().strip()
    except Exception as e:
        logger.warning(f"Failed to fetch watermark from GCS, using default: {str(e)}")

    return default_ts


def set_high_watermark(entity_name: str, timestamp: str) -> None:
    """
    Saves the latest processed timestamp for an entity.
    """
    filename = f"{entity_name}_high_watermark.txt"

    try:
        if config.STATE_BUCKET.startswith("gs://") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            from google.cloud import storage

            bucket_name = config.STATE_BUCKET.replace("gs://", "")
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"state/{filename}")
            blob.upload_from_string(timestamp)
        else:
            state_dir = "config/state"
            os.makedirs(state_dir, exist_ok=True)
            with open(f"{state_dir}/{filename}", "w") as f:
                f.write(timestamp)
    except Exception as e:
        logger.error(f"Failed to save watermark: {str(e)}")


def load_entity_config(entity_name: str):
    """Load config for a specific entity."""
    try:
        with open("config/entities.yml", "r") as f:
            entities = yaml.safe_load(f).get("entities", {})
            return entities.get(entity_name, {})
    except Exception as e:
        logger.error(f"Failed to load entity config for {entity_name}: {str(e)}")
        return {}


def process_bronze_layer(spark: SparkSession, entity_name: str) -> None:
    """
    Bronze Layer: Incremental ingestion of raw JSON into Delta format.
    """
    try:
        logger.info(f"Starting Bronze processing for: {entity_name}")

        raw_source = f"{config.RAW_PATH}/{entity_name}"
        bronze_dest = f"{config.BRONZE_PATH}/{entity_name}"

        entity_schema = SCHEMAS.get(entity_name)
        if not entity_schema:
            raise ValueError(f"Schema not found for entity: {entity_name}")

        entity_cfg = load_entity_config(entity_name)
        watermark_col = entity_cfg.get("watermark_col", "updated_at")

        last_processed_ts = get_high_watermark(entity_name)
        logger.info(f"Incremental Load - High-watermark for {entity_name}: {last_processed_ts}")

        raw_df = spark.read.schema(entity_schema).json(raw_source)

        if watermark_col in raw_df.columns:
            raw_df = raw_df.filter(F.col(watermark_col) > F.lit(last_processed_ts))
        else:
            logger.warning(f"Watermark column '{watermark_col}' not found for {entity_name}. Skipping incremental filter.")

        # Check if new data exists
        if raw_df.limit(1).count() == 0:
            logger.info(f"No new records found for {entity_name}.")
            return

        df = add_audit_metadata(raw_df, config.BATCH_ID, config.SOURCE_SYSTEM)
        df = compute_record_hash(df)
        df = df.withColumn("ingestion_date", F.to_date(F.col("processed_at")))

        if watermark_col in df.columns:
            new_watermark = df.select(F.max(watermark_col)).collect()[0][0]
        else:
            new_watermark = None

        logger.info(f"Writing to Delta at {bronze_dest}")
        df.write.format("delta").mode("append").partitionBy("ingestion_date").option("delta.enableChangeDataFeed", "true").save(
            bronze_dest
        )

        if new_watermark:
            set_high_watermark(entity_name, str(new_watermark))

        logger.info(f"Bronze layer load complete for {entity_name}")

    except Exception as e:
        logger.error(f"Bronze layer failure for {entity_name}: {str(e)}")
        raise
