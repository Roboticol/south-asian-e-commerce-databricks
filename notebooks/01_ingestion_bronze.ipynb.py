# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS main;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.raw;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.raw.south_asia_ecommerce;
# MAGIC SHOW CATALOGS

# COMMAND ----------

# DBTITLE 1,Cell 1
# Databricks Notebook: 01_ingestion_bronze_south_asia
# Purpose: Ingest South Asia E-Commerce (100k orders) dataset into Bronze Delta tables
# Dataset source: Mendeley – E-Commerce Dataset (South Asia)
# Storage: Unity Catalog Volume (NO DBFS ROOT)

from pyspark.sql.functions import current_timestamp

# =========================
# CONFIG (Unity Catalog compliant)
# =========================
CATALOG = "main"
BRONZE_DB = "bronze"
RAW_VOLUME_PATH = "/Volumes/main/raw/south_asia_ecommerce"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_DB}")

# =========================
# Helper Function
# =========================
def ingest_csv(table_name, file_name):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_VOLUME_PATH}/{file_name}")
        .withColumn("ingestion_ts", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{BRONZE_DB}.{table_name}")
    )

    print(f"Ingested {BRONZE_DB}.{table_name}")

# =========================
# INGEST RAW DATA (SINGLE FILE DATASET)
# =========================
# Dataset: orders.csv (South Asia, 100k orders)
# This file contains transactional order-level data

ingest_csv("raw_transactions", "orders.csv")

# =========================
# VALIDATION
# =========================
spark.sql("SELECT COUNT(*) AS row_count FROM bronze.raw_transactions").show()
spark.sql("DESCRIBE TABLE bronze.raw_transactions").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE bronze.raw_transactions;