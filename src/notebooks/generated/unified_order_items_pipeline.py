# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Order Items Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for order_items_pipeline with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader


# COMMAND ----------

@dlt.table(
    name="vbdemos.adls_bronze.order_items_new",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_order_items",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def order_items_new():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.schemaLocation", "abfss://aaboode@oneenvadls.dfs.core.windows.net/astellas/checkpoint/order_items/schema")
            .option("cloudFiles.maxFilesPerTrigger", "50")
            .option("cloudFiles.allowOverwrites", "false")
            .option("cloudFiles.format", "json")
            .option("multiline", "true")
            .load("abfss://aaboode@oneenvadls.dfs.core.windows.net/astellas/json/order_items")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("vbdemos.adls_silver.order_items_scd2")

# COMMAND ----------

@dlt.view
def bronze_order_items_scd2_source():
    return spark.readStream.table("vbdemos.adls_bronze.order_items_new")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="vbdemos.adls_silver.order_items_scd2",
    source="bronze_order_items_scd2_source",
    keys=["item_id"],
    sequence_by="created_ts",
    except_column_list=["order_id", "product_id", "product_name", "quantity", "unit_price", "total_price", "category"],
    stored_as_scd_type="2"
)


# COMMAND ----------

# Pipeline execution completed
