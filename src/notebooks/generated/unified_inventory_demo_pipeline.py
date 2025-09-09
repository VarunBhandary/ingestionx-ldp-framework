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
            .options(**{"cloudFiles.schemaLocation": "/Volumes/vbdemos/dbdemos_autoloader/raw_data/schema/inventory", "cloudFiles.checkpointLocation": "/Volumes/vbdemos/dbdemos_autoloader/raw_data/checkpoint/inventory", "cloudFiles.maxFilesPerTrigger": "200", "cloudFiles.allowOverwrites": "false", "header": "true", "inferSchema": "false", "cloudFiles.archiveLocation": "/Volumes/vbdemos/dbdemos_autoloader/raw_data/archive/inventory", "cloudFiles.rescuedDataColumn": "corrupt_data", "cloudFiles.validateOptions": "false"})
            .option("cloudFiles.format", "csv")
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
    **{"keys": ["product_id"], "stored_as_scd_type": "1", "sequence_by": "created_ts"}
)


# COMMAND ----------

# Pipeline execution completed
