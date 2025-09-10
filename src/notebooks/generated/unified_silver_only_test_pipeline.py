# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Silver Only Test Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for silver_only_test_pipeline with:
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

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("vbdemos.adls_silver.customers_demo_silver_only_scd2")

# COMMAND ----------

@dlt.view
def bronze_customers_demo_silver_only_scd2_source():
    return spark.readStream.table("vbdemos.adls_bronze.customers_demo")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="vbdemos.adls_silver.customers_demo_silver_only_scd2",
    source="bronze_customers_demo_silver_only_scd2_source",
    **{"keys": ["customer_id"], "track_history_except_column_list": ["created_at", "is_active"], "stored_as_scd_type": "2", "sequence_by": "updated_at"}
)


# COMMAND ----------

# Pipeline execution completed
