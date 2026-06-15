from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def process_bronze_layer(spark: SparkSession, source_path: str, destination_path: str, entity_name: str):
    """
    Bronze Layer: Raw to Validated.
    Reads raw data (JSON/CSV), adds metadata, and saves as Parquet.
    """
    print(f"Processing Bronze layer for {entity_name}...")

    # Read raw data
    raw_df = spark.read.json(f"{source_path}/{entity_name}/*.json")

    # Add metadata
    bronze_df = raw_df.withColumn("ingestion_timestamp", F.current_timestamp()) \
                      .withColumn("source_file", F.input_file_name()) \
                      .withColumn("status", F.lit("validated"))

    # Write to bronze layer in Parquet format
    bronze_df.write.mode("overwrite").parquet(f"{destination_path}/{entity_name}")

    print(f"Bronze layer for {entity_name} completed.")
    return bronze_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()
    # Example usage (usually called from a main job or Airflow)
    # process_bronze_layer(spark, "data/raw", "data/bronze", "customers")
