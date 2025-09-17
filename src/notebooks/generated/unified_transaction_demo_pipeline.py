# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Transaction Demo Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for transaction_demo_pipeline with:
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
    name="my_test_catalog.adls_bronze.transactions_demo",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_transactions_demo",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def transactions_demo():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.schemaLocation", "/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/schema/transactions")
            .option("cloudFiles.checkpointLocation", "/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/checkpoint/transactions")
            .option("cloudFiles.maxFilesPerTrigger", "50")
            .option("cloudFiles.allowOverwrites", "false")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.validateOptions", "false")
            .option("cloudFiles.format", "csv")
            .load("/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/transactions")
            .selectExpr("*", "_metadata as source_metadata", "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("my_test_catalog.adls_silver.transactions_demo_scd1")

# COMMAND ----------

@dlt.view
def bronze_transactions_demo_scd1_source():
    return spark.readStream.table("my_test_catalog.adls_bronze.transactions_demo")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="my_test_catalog.adls_silver.transactions_demo_scd1",
    source="bronze_transactions_demo_scd1_source",
    **{"keys": ["transaction_id"], "stored_as_scd_type": "1", "sequence_by": "updated_ts"}
)


# COMMAND ----------

# Pipeline execution completed
