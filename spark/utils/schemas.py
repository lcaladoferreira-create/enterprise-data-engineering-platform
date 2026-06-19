from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# Entity schemas for enforcement in the Bronze layer
SCHEMAS = {
    "customers": StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
        ]
    ),
    "orders": StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    ),
    "order_items": StructType(
        [
            StructField("order_item_id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
        ]
    ),
    "payments": StructType(
        [
            StructField("payment_id", IntegerType(), True),
            StructField("invoice_id", IntegerType(), True),
            StructField("payment_date", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("transaction_id", StringType(), True),
        ]
    ),
    "invoices": StructType(
        [
            StructField("invoice_id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("invoice_date", TimestampType(), True),
            StructField("due_date", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
        ]
    ),
}
