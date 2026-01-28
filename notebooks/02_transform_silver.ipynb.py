# Databricks notebook source
# DBTITLE 1,Cell 1
# Databricks Notebook: 02_transform_silver_south_asia
# Purpose: Clean raw transactional data and create Silver business tables
# Input: bronze.raw_transactions (single CSV source)

from pyspark.sql.functions import (
    try_to_timestamp,
    col, to_date, sum as _sum, count as _count
)

BRONZE_DB = "bronze"
SILVER_DB = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DB}")

# =========================
# LOAD BRONZE DATA
# =========================
raw_df = spark.table(f"{BRONZE_DB}.raw_transactions")

# =========================
# =========================
# TYPE CASTING & CLEANING
# =========================
# order_date format example: "03.11.2015 06:44" (dd.MM.yyyy HH:mm)
# Use to_timestamp with explicit format, then cast to DATE

from pyspark.sql.functions import expr

clean_df = (
    raw_df
    .withColumn(
        "order_ts",
        expr("try_to_timestamp(order_date, 'dd.MM.yyyy HH:mm')")
    )
    .withColumn("order_date", col("order_ts").cast("date"))
    .withColumn(
        "shipped_ts",
        expr("try_to_timestamp(shipped_at, 'dd.MM.yyyy HH:mm')")
    )
    .withColumn("shipped_at", col("shipped_ts").cast("date"))
    .withColumn("prod_qty", expr("try_cast(prod_qty as int)"))
    .drop("order_ts", "shipped_ts")
)


# =========================
# ORDERS TABLE
# =========================
orders_df = (
    clean_df
    .select("order_id", "order_date", "shipped_at")
    .dropDuplicates(["order_id"])
)

# =========================
# PRODUCTS TABLE
# =========================
products_df = (
    clean_df
    .select(col("prod_sku").alias("product_id"))
    .dropDuplicates(["product_id"])
)

# =========================
# ORDER ITEMS TABLE
# =========================
order_items_df = (
    clean_df
    .select(
        "order_id",
        col("prod_sku").alias("product_id"),
        col("prod_qty").alias("quantity")
    )
)

# =========================
# DAILY PRODUCT SALES (ML GRAIN)
# =========================
daily_product_sales = (
    order_items_df
    .join(orders_df, "order_id")
    .groupBy("product_id", "order_date")
    .agg(
        _sum("quantity").alias("units_sold"),
        _count("order_id").alias("orders_count")
    )
)

# =========================
# WRITE SILVER TABLES
# =========================
orders_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.orders")
products_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.products")
daily_product_sales.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.daily_product_sales")

# =========================
# OPTIMIZATION
# =========================
spark.sql(f"OPTIMIZE {SILVER_DB}.daily_product_sales ZORDER BY (product_id, order_date)")

# =========================
# VALIDATION
# =========================
spark.sql("SELECT * FROM silver.daily_product_sales LIMIT 10").show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.raw_transactions
# MAGIC WHERE prod_qty LIKE '%E+%';