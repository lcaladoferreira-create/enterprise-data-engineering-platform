import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a shared SparkSession for the entire test session.
    Configured for local execution with minimum resource overhead.
    """
    session = SparkSession.builder \
        .master("local[*]") \
        .appName("Enterprise-Integration-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()
