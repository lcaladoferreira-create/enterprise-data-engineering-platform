from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def process_silver_layer(spark: SparkSession, source_path: str, destination_path: str, entity_name: str, id_column: str):
    """
    Silver Layer: Cleaning and Deduplication.
    Reads bronze data, removes duplicates, handles nulls, and standardizes formats.
    """
    print(f"Processing Silver layer for {entity_name}...")

    # Read bronze data
    bronze_df = spark.read.parquet(f"{source_path}/{entity_name}")

    # 1. Deduplication
    window_spec = Window.partitionBy(id_column).orderBy(F.col("ingestion_timestamp").desc())
    deduped_df = bronze_df.withColumn("row_num", F.row_number().over(window_spec)) \
                         .filter(F.col("row_num") == 1) \
                         .drop("row_num")

    # 2. Basic Cleaning (Standardizing strings, handling common nulls)
    cleaned_df = deduped_df
    for col_name, dtype in cleaned_df.dtypes:
        if dtype == "string":
            cleaned_df = cleaned_df.withColumn(col_name, F.trim(F.col(col_name)))

    # 3. Entity-specific cleaning (Example for customers)
    if entity_name == "customers":
        cleaned_df = cleaned_df.withColumn("email", F.lower(F.col("email")))

    # Add processing metadata
    silver_df = cleaned_df.withColumn("processed_timestamp", F.current_timestamp())

    # Write to silver layer
    silver_df.write.mode("overwrite").parquet(f"{destination_path}/{entity_name}")

    print(f"Silver layer for {entity_name} completed.")
    return silver_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverLayer").getOrCreate()
