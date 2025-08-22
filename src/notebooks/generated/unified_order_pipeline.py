# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Order Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for order_pipeline with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader


# COMMAND ----------

@dlt.table(
    name="vbdemos.adls_bronze.orders_new",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_orders",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def orders_new():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.schemaLocation", "abfss://aaboode@oneenvadls.dfs.core.windows.net/astellas/checkpoint/orders/schema")
            .option("cloudFiles.maxFilesPerTrigger", "50")
            .option("cloudFiles.allowOverwrites", "false")
            .option("cloudFiles.format", "json")
            .option("multiline", "true")
            .load("abfss://aaboode@oneenvadls.dfs.core.windows.net/astellas/json/orders")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("vbdemos.adls_silver.orders_scd2")

# COMMAND ----------

@dlt.view
def bronze_orders_scd2_source():
    return spark.readStream.table("vbdemos.adls_bronze.orders_new")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="vbdemos.adls_silver.orders_scd2",
    source="bronze_orders_scd2_source",
    keys=["order_id"],
    sequence_by="created_ts",
    except_column_list=["customer_id", "order_date", "total_amount", "status", "shipping_address", "payment_method", "created_ts"],
    stored_as_scd_type="2"
)


# COMMAND ----------

# Pipeline execution completed
