import os

class Config:
    """
    Centralized configuration management for Spark jobs.
    """
    RAW_PATH = os.getenv("RAW_PATH", "data/raw")
    BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
    SILVER_PATH = os.getenv("SILVER_PATH", "data/silver")
    GOLD_PATH = os.getenv("GOLD_PATH", "data/gold")

    COMPRESSION_CODEC = "snappy"
    WRITE_MODE = "overwrite"

    # Batch metadata
    BATCH_ID = os.getenv("AIRFLOW_RUN_ID", "local_dev")
    SOURCE_SYSTEM = os.getenv("SOURCE_SYSTEM", "multi_source_platform")

config = Config()
