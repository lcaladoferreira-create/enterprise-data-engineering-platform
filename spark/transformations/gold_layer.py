from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config

logger = get_logger(__name__)

def create_gold_star_schema(spark: SparkSession) -> None:
    """
    Gold Layer: Implements a star schema for analytical reporting.
    Creates Dimensions and Fact tables.
    """
    try:
        logger.info("Starting Gold layer Star Schema generation...")

        # Load Silver tables
        customers = spark.read.parquet(f"{config.SILVER_PATH}/customers")
        orders = spark.read.parquet(f"{config.SILVER_PATH}/orders")
        spark.read.parquet(f"{config.SILVER_PATH}/order_items")
        products = spark.read.parquet(f"{config.SILVER_PATH}/products")
        payments = spark.read.parquet(f"{config.SILVER_PATH}/payments")
        spark.read.parquet(f"{config.SILVER_PATH}/invoices")

        # 1. dim_customers
        dim_customers = customers.select(
            "customer_id", "first_name", "last_name", "city", "country"
        ).withColumnRenamed("customer_id", "customer_key")
        dim_customers.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/dim_customers")

        # 2. dim_products
        dim_products = products.select(
            "product_id", "name", "category", "brand"
        ).withColumnRenamed("product_id", "product_key")
        dim_products.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/dim_products")

        # 3. fact_orders
        fact_orders = orders.join(payments, orders.order_id == payments.invoice_id, "left") \
            .select(
                orders.order_id,
                orders.customer_id.alias("customer_key"),
                orders.order_date,
                orders.total_amount,
                orders.status.alias("order_status"),
                payments.payment_method,
                payments.amount.alias("paid_amount")
            )
        fact_orders.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/fact_orders")

        # 4. Analytical Mart: Daily Sales
        mart_sales_daily = fact_orders.groupBy(F.to_date("order_date").alias("sale_date")) \
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("revenue"),
                F.avg("total_amount").alias("avg_order_value")
            )
        mart_sales_daily.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/mart_sales_daily")

        logger.info("Gold layer Star Schema generation completed.")

    except Exception as e:
        logger.error(f"Error processing Gold layer: {str(e)}")
        raise
