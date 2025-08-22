# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Product Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for product_pipeline with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader


# COMMAND ----------

@dlt.table(
    name="vbdemos.adls_bronze.products_new",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_products",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def products_new():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.schemaLocation", "abfss://aaboode@oneenvadls.dfs.core.windows.net/astellas/checkpoint/products/schema")
            .option("cloudFiles.maxFilesPerTrigger", "200")
            .option("cloudFiles.allowOverwrites", "false")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("abfss://aaboode@oneenvadls.dfs.core.windows.net/astellas/csv/products")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("vbdemos.adls_silver.products_scd2")

# COMMAND ----------

@dlt.view
def bronze_products_scd2_source():
    return spark.readStream.table("vbdemos.adls_bronze.products_new")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="vbdemos.adls_silver.products_scd2",
    source="bronze_products_scd2_source",
    keys=["product_id"],
    sequence_by="created_date",
    except_column_list=["product_name", "category", "price", "brand", "description", "stock_quantity", "created_date", "last_updated"],
    stored_as_scd_type="2"
)


# COMMAND ----------

# Pipeline execution completed
