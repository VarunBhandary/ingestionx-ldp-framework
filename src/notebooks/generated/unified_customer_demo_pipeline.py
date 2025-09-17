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
from pyspark.sql.types import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader


# COMMAND ----------
# Define fixed schema for data validation
schema = StructType([StructField('customer_id', StringType(), False), StructField('created_at', TimestampType(), True), StructField('is_active', BooleanType(), True), StructField('updated_at', TimestampType(), True)])

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
            .option("cloudFiles.checkpointLocation", "/Volumes/vbdemos/dbdemos_autoloader/raw_data/checkpoint/customers")
            .option("cloudFiles.maxFilesPerTrigger", "100")
            .option("cloudFiles.allowOverwrites", "false")
            .option("header", "true")
            .option("cloudFiles.rescuedDataColumn", "corrupt_data")
            .option("cloudFiles.validateOptions", "false")
            .option("cloudFiles.format", "csv")
            .schema(schema)  # Apply fixed schema for data validation
            .load("/Volumes/vbdemos/dbdemos_autoloader/raw_data/customers")
            .selectExpr("*", "_metadata as source_metadata", "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------
# Define fixed schema for data validation
schema = StructType([StructField('customer_id', StringType(), False), StructField('preference_category', StringType(), True), StructField('preference_value', StringType(), True), StructField('preference_score', DoubleType(), True), StructField('is_active', BooleanType(), True), StructField('created_at', TimestampType(), True), StructField('updated_at', TimestampType(), True)])

@dlt.table(
    name="vbdemos.adls_bronze.customer_preferences_demo",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_customer_preferences_demo",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def customer_preferences_demo():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.checkpointLocation", "/Volumes/vbdemos/dbdemos_autoloader/raw_data/checkpoint/customer_preferences")
            .option("cloudFiles.maxFilesPerTrigger", "50")
            .option("cloudFiles.allowOverwrites", "false")
            .option("cloudFiles.useManagedFileEvents", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.validateOptions", "false")
            .option("multiline", "true")
            .option("cloudFiles.format", "json")
            .schema(schema)  # Apply fixed schema for data validation
            .load("/Volumes/vbdemos/dbdemos_autoloader/raw_data/customer_preferences")
            .selectExpr("*", "_metadata as source_metadata", "current_timestamp() as _ingestion_timestamp"))


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
    **{"keys": ["customer_id"], "track_history_except_column_list": ["created_at", "is_active"], "stored_as_scd_type": "2", "sequence_by": "updated_at"}
)


# COMMAND ----------

# Pipeline execution completed
