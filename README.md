# South Asia E‑Commerce Demand Forecasting on Databricks

## 1. Project Overview
This project implements an **end‑to‑end, production‑grade data and ML pipeline** on the Databricks Lakehouse for **SKU‑level demand forecasting** using a [large South Asia e‑commerce dataset](https://data.mendeley.com/datasets/ggbkd8ck3x/1) (100,000+ orders).

The solution demonstrates how raw CSV data can be transformed into **business insights and ML predictions** using Databricks best practices:
- Medallion Architecture (Bronze → Silver → Gold)
- Delta Lake with ACID guarantees
- Unity Catalog governance
- MLflow‑tracked machine learning
- Automated Jobs orchestration

The project is designed to be **portfolio‑ready** and aligned with real‑world retail analytics and forecasting use cases.



## 2. Problem Statement & AI Framing

**Problem:**
E‑commerce platforms need accurate short‑term demand forecasts to optimize inventory, reduce stock‑outs, and improve fulfillment efficiency.

**AI Framing:**
- **Task:** Supervised regression
- **Target:** Daily units sold per SKU
- **Granularity:** (product_id, order_date)
- **Approach:** Train a machine learning model using rolling historical demand features



## 3. Dataset

- **Source:** [South Asia E‑Commerce Dataset (CSV)](https://data.mendeley.com/datasets/ggbkd8ck3x/1)
- **Size:** ~100,000 orders
- **Key Challenges:**
  - Non‑ISO timestamps (e.g. `03.11.2015 06:44`)
  - Missing values (`NA`)
  - Malformed numeric fields (scientific notation in quantities)

These issues are intentionally handled in the pipeline to reflect **real‑world data quality problems**.



## 4. Architecture (Medallion Lakehouse)

### Architecture Diagram

```
        ┌──────────────┐
        │  Raw CSV     │
        │ (E‑Commerce) │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │ Bronze Layer │
        │ Delta Tables │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │ Silver Layer │
        │ Cleaned Data │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │ Gold Layer   │
        │ Features &   │
        │ KPIs         │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │ ML Training  │
        │ + MLflow     │
        └──────────────┘
```

### 🟤 Bronze Layer – Raw Ingestion
**Notebook:** `01_ingestion_bronze_south_asia`
- Ingests raw CSV into Delta Lake
- Minimal transformation
- Adds ingestion timestamp
- Stored as `bronze.raw_transactions`

### ⚪ Silver Layer – Data Cleaning & Business Logic
**Notebook:** `02_transform_silver_south_asia`
- Explicit timestamp parsing with tolerant functions
- Null‑safe casting using `try_cast`
- Business‑level tables:
  - `silver.orders`
  - `silver.products`
  - `silver.daily_product_sales`

### 🟡 Gold Layer – Analytics & Features
**Notebook:** `03_gold_aggregations_south_asia`
- Rolling demand windows (7 / 14 / 30 days)
- Demand acceleration signals
- SKU performance KPIs
- ML‑ready feature tables

### 🔵 ML Layer – Demand Forecasting
**Notebook:** `04_ml_demand_forecasting_south_asia`
- RandomForest regression model
- Time‑aware train/test split
- MLflow experiment tracking
- Unity Catalog–compliant model logging



## 5. Machine Learning Details

- **Model:** RandomForestRegressor
- **Features:**
  - units_7d
  - units_14d
  - units_30d
- **Label:** units_sold
- **Metric:** RMSE

**Why Random Forest?**
- Handles non‑linear demand patterns
- Robust to noise
- Minimal feature assumptions

MLflow logs:
- Parameters
- Metrics
- Model artifact
- Input example & inferred signature



## 6. Orchestration & Automation

**Notebook:** `05_jobs_orchestration_south_asia`

- Uses `dbutils.notebook.run()`
- Single entry point for the full pipeline
- Executed via **Databricks Jobs**
- Supports scheduled or on‑demand runs

Pipeline order:
1. Bronze ingestion
2. Silver transformations
3. Gold aggregations
4. ML training



## 7. Governance & Compliance

- **Unity Catalog schemas** for Bronze, Silver, Gold
- **UC Volumes** for MLflow artifacts
- No DBFS root dependency
- Suitable for shared or serverless clusters



## 8. Business Impact

This solution enables:
- SKU‑level demand visibility
- Early trend detection
- Data‑driven inventory planning
- Scalable ML deployment foundation