from pyspark.sql import SparkSession, functions as F
from spark.utils.logger import get_logger
from spark.utils.config import config

logger = get_logger(__name__)


def create_gold_star_schema(spark: SparkSession) -> None:
    """
    Gold Layer: Curated zone implementing a robust Star Schema and domain-specific Data Marts.

    Operations:
    - Creation of Type 1 Dimensions (dim_customers, dim_products, dim_dates).
    - Creation of Fact tables (fact_orders, fact_payments).
    - Materialization of aggregated analytical marts for rapid BI serving.
    """
    try:
        logger.info("Initializing Gold layer star schema and mart generation.")

        # Source valid Silver tables
        customers = spark.read.parquet(f"{config.SILVER_PATH}/customers")
        orders = spark.read.parquet(f"{config.SILVER_PATH}/orders")
        order_items = spark.read.parquet(f"{config.SILVER_PATH}/order_items")
        products = spark.read.parquet(f"{config.SILVER_PATH}/products")
        payments = spark.read.parquet(f"{config.SILVER_PATH}/payments")
        invoices = spark.read.parquet(f"{config.SILVER_PATH}/invoices")

        # 1. Dimension: dim_customers
        dim_customers = customers.select(
            "customer_id", "first_name", "last_name", "city", "country"
        ).withColumnRenamed("customer_id", "customer_key")
        dim_customers.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/dim_customers")

        # 2. Dimension: dim_products
        dim_products = products.select(
            "product_id", "name", "category", "brand"
        ).withColumnRenamed("product_id", "product_key")
        dim_products.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/dim_products")

        # 3. Dimension: dim_dates
        # Efficiently generate date range based on order history
        date_range = orders.select(F.min("order_date"), F.max("order_date")).collect()[0]
        min_date, max_date = date_range[0], date_range[1]

        num_days = (max_date - min_date).days + 1
        dim_dates = spark.range(0, num_days).select(
            F.expr(f"date_add('{min_date.date()}', cast(id as int))").alias("date_key")
        ).select(
            "date_key",
            F.year("date_key").alias("year"),
            F.month("date_key").alias("month"),
            F.dayofmonth("date_key").alias("day"),
            F.quarter("date_key").alias("quarter"),
            F.date_format("date_key", "EEEE").alias("day_name")
        )
        dim_dates.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/dim_dates")

        # 4. Fact Table: fact_orders
        fact_orders = orders.select(
            "order_id",
            F.col("customer_id").alias("customer_key"),
            F.to_date("order_date").alias("order_date_key"),
            "total_amount",
            F.col("status").alias("order_status")
        )
        fact_orders.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/fact_orders")

        # 5. Fact Table: fact_payments
        fact_payments = payments.select(
            "payment_id",
            "invoice_id",
            F.to_date("payment_date").alias("payment_date_key"),
            "amount",
            "payment_method",
            "transaction_id"
        )
        fact_payments.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/fact_payments")

        # 6. Analytical Mart: Daily Sales Analysis
        mart_sales_daily = fact_orders.groupBy("order_date_key") \
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("revenue")
            )
        mart_sales_daily.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/mart_sales_daily")

        # 7. Analytical Mart: Customer Lifetime Value (CLV)
        mart_clv = fact_orders.groupBy("customer_key") \
            .agg(
                F.sum("total_amount").alias("lifetime_spend"),
                F.count("order_id").alias("total_orders"),
                F.min("order_date_key").alias("first_purchase_date"),
                F.max("order_date_key").alias("latest_purchase_date")
            )
        mart_clv.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/mart_customer_lifetime_value")

        # 8. Analytical Mart: Product Category Performance
        mart_prod_perf = order_items.join(products, "product_id") \
            .groupBy("product_id", "name", "category") \
            .agg(
                F.sum("quantity").alias("total_units_sold"),
                F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_revenue")
            )
        mart_prod_perf.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/mart_product_performance")

        # 9. Analytical Mart: Payment/Invoice Reconciliation
        mart_pay_recon = invoices.join(payments, "invoice_id", "left") \
            .select(
                invoices.invoice_id,
                invoices.order_id,
                invoices.amount.alias("invoiced_amount"),
                F.coalesce(payments.amount, F.lit(0)).alias("received_amount"),
                (invoices.amount - F.coalesce(payments.amount, F.lit(0))).alias("payment_variance")
            )
        mart_pay_recon.write.mode(config.WRITE_MODE).parquet(f"{config.GOLD_PATH}/mart_payment_reconciliation")

        logger.info("Gold layer artifacts successfully materialized.")

    except Exception as e:
        logger.error(f"Gold layer processing failed: {str(e)}", exc_info=True)
        raise
