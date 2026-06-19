from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.utils.config import config
from spark.utils.data_quality import check_nulls, mask_pii
from spark.utils.logging import get_logger

logger = get_logger("silver_layer")


def process_silver_layer(
    spark: SparkSession,
    entity_name: str,
    pk_col: str,
    pii_cols: list = None,
    critical_cols: list = None,
    bronze_path: str = None,
    silver_path: str = None,
    quarantine_path: str = None,
) -> None:
    """
    Silver Layer: Implementation of deduplication, data cleaning, and security.
    Uses Delta Lake MERGE pattern for idempotency and upserts.
    """
    try:
        logger.info(f"Initiating Silver transformation for entity: {entity_name}")

        bronze_src = bronze_path if bronze_path else f"{config.BRONZE_PATH}/{entity_name}"
        silver_dst = silver_path if silver_path else f"{config.SILVER_PATH}/{entity_name}"
        quarantine_dst = quarantine_path if quarantine_path else f"{config.SILVER_PATH}/quarantine/{entity_name}"

        df = spark.read.format("delta").load(bronze_src)

        # Cache the dataframe to avoid multiple passes during DQ checks and merging
        df.cache()

        # 1. Deduplication
        window_spec = Window.partitionBy(pk_col).orderBy(F.col("processed_at").desc())
        df = (
            df.withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        # 2. Data Quality
        if critical_cols:
            df = check_nulls(df, critical_cols)

            quarantine_df = df.filter(F.col("dq_failed"))

            # Optimized check using limit(1) instead of count() initially
            if quarantine_df.limit(1).count() > 0:
                failure_count = quarantine_df.count()
                logger.warning(f"Moving {failure_count} records to quarantine for {entity_name}")
                quarantine_df.write.format("delta").mode("append").save(quarantine_dst)

            df = df.filter(~F.col("dq_failed")).drop("dq_failed", "dq_reason")

        # 3. Cleaning
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        # 4. Security
        if pii_cols:
            df = mask_pii(df, pii_cols)

        # 5. Upsert to Silver using Delta MERGE
        if not DeltaTable.isDeltaTable(spark, silver_dst):
            logger.info(f"Creating new Delta table at {silver_dst}")
            df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").save(silver_dst)
        else:
            logger.info(f"Merging data into Delta table at {silver_dst}")
            silver_table = DeltaTable.forPath(spark, silver_dst)

            merge_condition = f"target.{pk_col} = source.{pk_col}"

            silver_table.alias("target").merge(df.alias("source"), merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Silver layer materialization successful for {entity_name}.")

        # Unpersist after processing is complete
        df.unpersist()

    except Exception as e:
        logger.error(f"Silver processing failed for {entity_name}: {str(e)}")
        # Ensure we unpersist even on failure if it was cached
        try:
            df.unpersist()
        except NameError:
            pass
        raise
