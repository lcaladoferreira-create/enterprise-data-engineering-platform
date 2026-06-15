from pyspark.sql import SparkSession, functions as F

def create_gold_customer_orders(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Gold Layer: Curated Analytical Datasets.
    Joins silver tables to create business-ready views.
    """
    print("Creating Gold Customer Orders dataset...")

    customers = spark.read.parquet(f"{silver_path}/customers")
    orders = spark.read.parquet(f"{silver_path}/orders")

    # Join and aggregate
    gold_df = orders.join(customers, "customer_id", "left") \
                    .select(
                        orders.order_id,
                        orders.customer_id,
                        customers.first_name,
                        customers.last_name,
                        customers.city,
                        customers.country,
                        orders.order_date,
                        orders.total_amount,
                        orders.status
                    )

    # Write to gold layer
    gold_df.write.mode("overwrite").parquet(f"{gold_path}/customer_orders")

    print("Gold Customer Orders dataset created.")
    return gold_df

def create_gold_product_sales(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Aggregates sales by product and category.
    """
    print("Creating Gold Product Sales dataset...")

    order_items = spark.read.parquet(f"{silver_path}/order_items")
    products = spark.read.parquet(f"{silver_path}/products")

    gold_df = order_items.join(products, "product_id", "inner") \
                         .groupBy("product_id", "name", "category", "brand") \
                         .agg(
                             F.sum("quantity").alias("total_units_sold"),
                             F.sum(F.col("quantity") * F.col("unit_price")).alias("total_revenue")
                         )

    gold_df.write.mode("overwrite").parquet(f"{gold_path}/product_sales")

    print("Gold Product Sales dataset created.")
    return gold_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldLayer").getOrCreate()
