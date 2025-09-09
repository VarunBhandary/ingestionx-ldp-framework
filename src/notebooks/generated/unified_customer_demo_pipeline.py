# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Customer Demo Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for customer_demo_pipeline with:
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
    name="vbdemos.adls_bronze.customers_demo",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_customers_demo",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def customers_demo():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .options(**{"cloudFiles.schemaLocation": "/Volumes/vbdemos/dbdemos_autoloader/raw_data/schema/customers", "cloudFiles.checkpointLocation": "/Volumes/vbdemos/dbdemos_autoloader/raw_data/checkpoint/customers", "cloudFiles.maxFilesPerTrigger": "100", "cloudFiles.allowOverwrites": "false", "header": "true", "inferSchema": "true", "cloudFiles.validateOptions": "false"})
            .option("cloudFiles.format", "csv")
            .load("/Volumes/vbdemos/dbdemos_autoloader/raw_data/customers")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("vbdemos.adls_silver.customers_demo_scd2")

# COMMAND ----------

@dlt.view
def bronze_customers_demo_scd2_source():
    return spark.readStream.table("vbdemos.adls_bronze.customers_demo")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="vbdemos.adls_silver.customers_demo_scd2",
    source="bronze_customers_demo_scd2_source",
    **{"keys": ["customer_id"], "track_history_except_column_list": ["first_name", "last_name", "email", "phone_number", "address", "city", "state", "zip_code", "country", "customer_tier"], "stored_as_scd_type": "2", "sequence_by": "update_ts"}
)


# COMMAND ----------

# Pipeline execution completed
