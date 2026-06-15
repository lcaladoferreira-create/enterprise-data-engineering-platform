import os

class Config:
    """
    Centralized configuration management for Spark jobs.
    """
    @property
    def RAW_PATH(self):
        return os.getenv("RAW_PATH", "data/raw")

    @property
    def BRONZE_PATH(self):
        return os.getenv("BRONZE_PATH", "data/bronze")

    @property
    def SILVER_PATH(self):
        return os.getenv("SILVER_PATH", "data/silver")

    @property
    def GOLD_PATH(self):
        return os.getenv("GOLD_PATH", "data/gold")

    COMPRESSION_CODEC = "snappy"
    WRITE_MODE = "overwrite"

    # Batch metadata
    @property
    def BATCH_ID(self):
        return os.getenv("AIRFLOW_RUN_ID", "local_dev")

    @property
    def SOURCE_SYSTEM(self):
        return os.getenv("SOURCE_SYSTEM", "multi_source_platform")

config = Config()
