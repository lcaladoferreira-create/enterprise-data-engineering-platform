from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import add_audit_metadata, compute_record_hash
from spark.utils.schemas import SCHEMAS

logger = get_logger(__name__)


def process_bronze_layer(spark: SparkSession, entity_name: str) -> None:
    """
    Bronze Layer: Converts raw JSON files to optimized Parquet format.

    Operations:
    - Enforces strict StructType schema validation.
    - Adds audit metadata (batch_id, source_system, processed_at).
    - Computes unique record hashes for change tracking.
    - Implements partitioned, idempotent writes.
    """
    try:
        logger.info(f"Initiating Bronze layer processing for entity: {entity_name}")

        raw_source = f"{config.RAW_PATH}/{entity_name}"
        bronze_dest = f"{config.BRONZE_PATH}/{entity_name}"

        # Validate schema existence
        entity_schema = SCHEMAS.get(entity_name)
        if not entity_schema:
            raise ValueError(f"No schema defined for entity '{entity_name}' in spark.utils.schemas")

        # Load raw data with strict schema enforcement
        df = spark.read.schema(entity_schema).json(raw_source)

        if df.count() == 0:
            logger.warning(f"Aborting Bronze layer: No source records found at {raw_source}")
            return

        # Transform: Add operational metadata
        df = add_audit_metadata(df, config.BATCH_ID, config.SOURCE_SYSTEM)

        # Transform: Payload hashing for data lineage and drift detection
        df = compute_record_hash(df)

        # Transform: Partitioning metadata
        df = df.withColumn("ingestion_date", F.to_date(F.col("processed_at")))

        # Load: Scalable write operation
        df.write.mode(config.WRITE_MODE) \
            .partitionBy("ingestion_date") \
            .parquet(bronze_dest)

        logger.info(f"Bronze layer processing successful. Records persisted to: {bronze_dest}")

    except Exception as e:
        logger.error(f"Bronze layer processing failed for {entity_name}: {str(e)}", exc_info=True)
        raise
