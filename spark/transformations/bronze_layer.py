from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config
from spark.utils.data_quality import add_audit_metadata, compute_record_hash
from spark.utils.schemas import SCHEMAS

logger = get_logger(__name__)


def process_bronze_layer(spark: SparkSession, entity_name: str) -> None:
    """
    Bronze Layer: Converts raw JSON to validated Parquet.
    Implements schema enforcement, audit metadata, and payload hashing.
    """
    try:
        logger.info(f"Starting Bronze processing for: {entity_name}")

        raw_source = f"{config.RAW_PATH}/{entity_name}"
        bronze_dest = f"{config.BRONZE_PATH}/{entity_name}"

        # 1. Schema Enforcement
        entity_schema = SCHEMAS.get(entity_name)
        if not entity_schema:
            raise ValueError(f"Schema not found for entity: {entity_name}")

        # 2. Extract with strict schema
        df = spark.read.schema(entity_schema).json(raw_source)

        if df.count() == 0:
            logger.warning(f"No records found for {entity_name} in {raw_source}")
            return

        # 3. Transform: Add operational metadata
        df = add_audit_metadata(df, config.BATCH_ID, config.SOURCE_SYSTEM)

        # 4. Transform: Integrity hash for change tracking
        df = compute_record_hash(df)

        # 5. Transform: Partitioning key
        df = df.withColumn("ingestion_date", F.to_date(F.col("processed_at")))

        # 6. Load: Idempotent partitioned write
        df.write.mode(config.WRITE_MODE) \
            .partitionBy("ingestion_date") \
            .parquet(bronze_dest)

        logger.info(f"Bronze layer load complete for {entity_name} at {bronze_dest}")

    except Exception as e:
        logger.error(f"Bronze layer failure for {entity_name}: {str(e)}", exc_info=True)
        raise
