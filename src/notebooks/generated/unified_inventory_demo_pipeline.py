# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Inventory Demo Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for inventory_demo_pipeline with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC
# MAGIC 
# MAGIC Note: Gold operations are implemented as separate notebook tasks in the job.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader


# COMMAND ----------

@dlt.table(
    name="vbdemos.adls_bronze.inventory_demo",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_inventory_demo",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def inventory_demo():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", "/Volumes/vbdemos/dbdemos_autoloader/raw_data/schema/inventory")
            .option("cloudFiles.checkpointLocation", "/Volumes/vbdemos/dbdemos_autoloader/raw_data/checkpoint/inventory")
            .option("cloudFiles.maxFilesPerTrigger", "200")
            .option("cloudFiles.allowOverwrites", "false")
            .option("cloudFiles.archiveLocation", "/Volumes/vbdemos/dbdemos_autoloader/raw_data/archive/inventory")
            .option("cloudFiles.rescuedDataColumn", "corrupt_data")
            .option("cloudFiles.validateOptions", "false")
            .load("/Volumes/vbdemos/dbdemos_autoloader/raw_data/inventory")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("vbdemos.adls_silver.inventory_demo_scd1")

# COMMAND ----------

@dlt.view
def bronze_inventory_demo_scd1_source():
    return spark.readStream.table("vbdemos.adls_bronze.inventory_demo")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="vbdemos.adls_silver.inventory_demo_scd1",
    source="bronze_inventory_demo_scd1_source",
    keys=["product_id"],
    sequence_by="created_ts",
    except_column_list=["product_name", "category", "brand", "sku", "quantity_on_hand", "quantity_reserved", "unit_cost", "unit_price", "warehouse_location", "last_restocked", "reorder_level", "supplier_id"],
    stored_as_scd_type="1"
)


# COMMAND ----------

# Pipeline execution completed
