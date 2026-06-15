import logging
from pyspark.sql import DataFrame

def setup_logging(name: str):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    return logging.getLogger(name)

def check_nulls(df: DataFrame, columns: list):
    """
    Checks for nulls in specified columns and returns count of failed records.
    """
    failed_counts = {}
    for col in columns:
        count = df.filter(df[col].isNull()).count()
        if count > 0:
            failed_counts[col] = count
    return failed_counts

def check_duplicates(df: DataFrame, columns: list):
    """
    Checks for duplicates based on a list of columns.
    """
    total_count = df.count()
    unique_count = df.select(columns).distinct().count()
    return total_count - unique_count
