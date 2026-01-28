# Databricks notebook source
# Databricks Notebook: 05_jobs_orchestration_south_asia
# Purpose: Orchestrate end-to-end Lakehouse pipeline using Databricks Jobs
# Pipeline: Bronze -> Silver -> Gold -> ML

# NOTE:
# This notebook is intended to be used as the ENTRY POINT of a Databricks Job.
# Each stage is executed as a task via dbutils.notebook.run().

# =========================
# CONFIGURATION
# =========================
BRONZE_NOTEBOOK = "/Workspace/Users/fiverrticol@gmail.com/notebooks/01_ingestion_bronze.ipynb"
SILVER_NOTEBOOK = "/Workspace/Users/fiverrticol@gmail.com/notebooks/02_transform_silver.ipynb"
GOLD_NOTEBOOK   = "/Workspace/Users/fiverrticol@gmail.com/notebooks/03_gold_aggregations.ipynb"
ML_NOTEBOOK     = "/Workspace/Users/fiverrticol@gmail.com/notebooks/04_ml_demand_forecasting.ipynb"

TIMEOUT = 0  # 0 = no timeout

# =========================
# ORCHESTRATION
# =========================

def run_stage(name, notebook_path):
    print(f"\n▶ Running stage: {name}")
    result = dbutils.notebook.run(notebook_path, TIMEOUT)
    print(f"✔ Completed stage: {name}")
    return result

# Execute pipeline sequentially
run_stage("Bronze Ingestion", BRONZE_NOTEBOOK)
run_stage("Silver Transformation", SILVER_NOTEBOOK)
run_stage("Gold Aggregations", GOLD_NOTEBOOK)
run_stage("ML Training", ML_NOTEBOOK)

print("\n🚀 END-TO-END PIPELINE EXECUTED SUCCESSFULLY")
