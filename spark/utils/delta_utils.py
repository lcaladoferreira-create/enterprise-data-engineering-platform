from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from spark.utils.logging import get_logger

logger = get_logger("delta_utils")


def optimize_table(spark: SparkSession, table_path: str) -> None:
    """
    Optimizes a Delta table by compacting small files.
    """
    logger.info(f"Optimizing Delta table at {table_path}")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.optimize().executeCompaction()
    logger.info(f"Optimization complete for {table_path}")


def vacuum_table(spark: SparkSession, table_path: str, retention_hours: int = 168) -> None:
    """
    Removes files that are no longer in the latest state of the transaction log.
    """
    logger.info(f"Vacuuming Delta table at {table_path} with retention {retention_hours}h")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.vacuum(retention_hours)
    logger.info(f"Vacuum complete for {table_path}")


def get_table_history(spark: SparkSession, table_path: str):
    """
    Returns the transaction history of a Delta table.
    """
    logger.info(f"Fetching history for Delta table at {table_path}")
    delta_table = DeltaTable.forPath(spark, table_path)
    return delta_table.history()
