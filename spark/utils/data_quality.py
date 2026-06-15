from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

def check_nulls(df: DataFrame, critical_columns: list) -> DataFrame:
    """
    Identifies records with nulls in critical columns.
    Returns a DataFrame with a 'dq_failed' flag and 'dq_reason'.
    """
    condition = None
    for col in critical_columns:
        if condition is None:
            condition = F.col(col).isNull()
        else:
            condition = condition | F.col(col).isNull()

    return df.withColumn("dq_failed", F.when(condition, True).otherwise(False)) \
             .withColumn("dq_reason", F.when(condition, F.lit("Missing critical values")).otherwise(None))

def check_duplicates(df: DataFrame, columns: list) -> int:
    """
    Checks for duplicates based on a list of columns.
    """
    total_count = df.count()
    unique_count = df.select(columns).distinct().count()
    return total_count - unique_count

def mask_pii(df: DataFrame, pii_columns: list) -> DataFrame:
    """
    Anonymizes PII columns using SHA-256 hashing for GDPR/LGPD compliance.
    """
    masked_df = df
    for col_name in pii_columns:
        if col_name in df.columns:
            masked_df = masked_df.withColumn(
                col_name,
                F.sha2(F.col(col_name).cast(StringType()), 256)
            )
    return masked_df

def add_audit_metadata(df: DataFrame, batch_id: str, source_system: str) -> DataFrame:
    """
    Adds standard audit metadata to every record.
    """
    return df.withColumn("batch_id", F.lit(batch_id)) \
             .withColumn("source_system", F.lit(source_system)) \
             .withColumn("processed_at", F.current_timestamp())

def compute_record_hash(df: DataFrame) -> DataFrame:
    """
    Computes a SHA-256 hash of the entire record payload for change tracking.
    """
    cols = [F.col(c).cast(StringType()) for c in df.columns if c not in ["batch_id", "processed_at", "source_system"]]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("||", *cols), 256))
