# Databricks notebook source
# Databricks Notebook: 03_gold_aggregations_south_asia
# Purpose: Create Gold analytical & ML-ready tables
# Input: silver.orders, silver.products, silver.daily_product_sales

from pyspark.sql.functions import (
    col, sum as _sum, avg as _avg, countDistinct,
    lag, expr
)
from pyspark.sql.window import Window

SILVER_DB = "silver"
GOLD_DB = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DB}")

# =========================
# LOAD SILVER TABLES
# =========================
orders = spark.table(f"{SILVER_DB}.orders")
daily_sales = spark.table(f"{SILVER_DB}.daily_product_sales")

# =========================
# KPI 1: DAILY SALES SUMMARY
# =========================
daily_sales_summary = (
    daily_sales
    .groupBy("order_date")
    .agg(
        _sum("units_sold").alias("total_units_sold"),
        _sum("orders_count").alias("total_orders")
    )
)

# =========================
# KPI 2: PRODUCT DEMAND VELOCITY
# Rolling 7 / 14 / 30 day demand per SKU
# =========================
window_7d = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-6, 0)
window_14d = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-13, 0)
window_30d = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-29, 0)

product_demand_features = (
    daily_sales
    .withColumn("units_7d", _sum("units_sold").over(window_7d))
    .withColumn("units_14d", _sum("units_sold").over(window_14d))
    .withColumn("units_30d", _sum("units_sold").over(window_30d))
)

# =========================
# KPI 3: DEMAND ACCELERATION (TREND SIGNAL)
# =========================
trend_window = Window.partitionBy("product_id").orderBy("order_date")

product_trends = (
    product_demand_features
    .withColumn("prev_units", lag("units_sold", 1).over(trend_window))
    .withColumn(
        "demand_change",
        col("units_sold") - col("prev_units")
    )
)

# =========================
# KPI 4: SKU PERFORMANCE SUMMARY
# =========================
sku_performance = (
    daily_sales
    .groupBy("product_id")
    .agg(
        _sum("units_sold").alias("lifetime_units_sold"),
        _avg("units_sold").alias("avg_daily_units"),
        countDistinct("order_date").alias("active_days")
    )
)

# =========================
# WRITE GOLD TABLES
# =========================
daily_sales_summary.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.daily_sales_summary")
product_demand_features.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.product_demand_features")
product_trends.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.product_trends")
sku_performance.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.sku_performance")

# =========================
# OPTIMIZATION
# =========================
spark.sql(f"OPTIMIZE {GOLD_DB}.product_demand_features ZORDER BY (product_id, order_date)")

# =========================
# VALIDATION
# =========================
spark.sql("SELECT * FROM gold.product_demand_features LIMIT 10").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.product_demand_features;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.product_trends
# MAGIC WHERE demand_change IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.sku_performance
# MAGIC ORDER BY lifetime_units_sold DESC
# MAGIC LIMIT 10;
# MAGIC