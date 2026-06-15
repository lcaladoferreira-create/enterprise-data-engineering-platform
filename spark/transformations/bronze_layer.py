from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import add_audit_metadata, compute_record_hash
from spark.utils.schemas import SCHEMAS

logger = get_logger(__name__)

def process_bronze_layer(spark: SparkSession, entity_name: str) -> None:
    """
    Bronze Layer: Converts raw JSON to Parquet with audit metadata and strict schema enforcement.
    Partitioned by ingestion date for scalability.
    """
    try:
        logger.info(f"Starting Bronze layer processing for: {entity_name}")

        raw_source = f"{config.RAW_PATH}/{entity_name}"
        bronze_dest = f"{config.BRONZE_PATH}/{entity_name}"

        # Schema enforcement
        entity_schema = SCHEMAS.get(entity_name)
        if not entity_schema:
            logger.error(f"No schema defined for entity: {entity_name}")
            raise ValueError(f"Missing schema for {entity_name}")

        # Read raw data with explicit schema
        df = spark.read.schema(entity_schema).json(raw_source)

        if df.count() == 0:
            logger.warning(f"No data found in {raw_source}")
            return

        # Add audit metadata
        df = add_audit_metadata(df, config.BATCH_ID, config.SOURCE_SYSTEM)

        # Change tracking hash
        df = compute_record_hash(df)

        # Add partitioning column
        df = df.withColumn("ingestion_date", F.to_date(F.col("processed_at")))

        # Idempotent write with partitioning
        df.write.mode(config.WRITE_MODE) \
            .partitionBy("ingestion_date") \
            .parquet(bronze_dest)

        logger.info(f"Bronze layer completed for {entity_name}. Destination: {bronze_dest}")

    except Exception as e:
        logger.error(f"Error processing Bronze layer for {entity_name}: {str(e)}")
        raise
