# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS main;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.mlflow;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.mlflow.tmp;
# MAGIC SHOW CATALOGS

# COMMAND ----------

# Databricks Notebook: 04_ml_demand_forecasting_south_asia
# Purpose: Train SKU-level demand forecasting model with MLflow
# Input: gold.product_demand_features

import mlflow
import mlflow.spark
import os

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# =========================
# UNITY CATALOG MLflow TEMP PATH
# =========================
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/main/mlflow/tmp"

GOLD_DB = "gold"

# =========================
# LOAD FEATURE TABLE
# =========================
features_df = (
    spark.table(f"{GOLD_DB}.product_demand_features")
    # Labels MUST be non-null for Spark ML
    .filter(col("units_sold").isNotNull())
    .filter(col("units_7d").isNotNull())
    .filter(col("units_14d").isNotNull())
    .filter(col("units_30d").isNotNull())
)

# =========================
# TRAIN / TEST SPLIT (TIME-AWARE)
# =========================
train_df = features_df.filter(col("order_date") < "2018-01-01")
test_df = features_df.filter(col("order_date") >= "2018-01-01")

from pyspark.ml.feature import VectorAssembler

feature_cols = ["units_7d", "units_14d", "units_30d"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

train_vec = assembler.transform(train_df)

# =========================
# MODEL SIGNATURE & INPUT EXAMPLE (DenseVector-safe)
# =========================
input_example = (
    train_vec
    .select("features")
    .limit(5)
    .toPandas()
)

# Convert DenseVector -> list for JSON serialization
input_example["features"] = input_example["features"].apply(list)

# =========================
# FEATURE ASSEMBLY
# =========================
feature_cols = ["units_7d", "units_14d", "units_30d"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

train_vec = assembler.transform(train_df)
test_vec = assembler.transform(test_df)

# =========================
# MODEL DEFINITION
# =========================
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="units_sold",
    numTrees=50,
    maxDepth=8,
    seed=42
)

# =========================
# MLflow EXPERIMENT
# =========================
mlflow.set_experiment("/Shared/south_asia_ecommerce_demand_forecasting")

with mlflow.start_run(run_name="rf_demand_forecast"):
    model = rf.fit(train_vec)

    predictions = model.transform(test_vec)

    evaluator = RegressionEvaluator(
        labelCol="units_sold",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)

    # =========================
    # LOGGING
    # =========================
    mlflow.log_param("num_trees", 50)
    mlflow.log_param("max_depth", 8)
    mlflow.log_metric("rmse", rmse)

    mlflow.spark.log_model(
        model,
        artifact_path="demand_model",
        input_example=input_example
    )

    print(f"RMSE: {rmse}")

# =========================
# INFERENCE SAMPLE
# =========================
predictions.select(
    "product_id",
    "order_date",
    "units_sold",
    "prediction"
).show(10)


# COMMAND ----------

predictions.select(
    "product_id",
    "order_date",
    "units_sold",
    "prediction"
).show(10)