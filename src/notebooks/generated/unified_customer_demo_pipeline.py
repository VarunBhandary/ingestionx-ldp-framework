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
schema = StructType([StructField('customer_id', StringType(), False), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('email', StringType(), True), StructField('phone_number', StringType(), True), StructField('address', StringType(), True), StructField('city', StringType(), True), StructField('state', StringType(), True), StructField('zip_code', StringType(), True), StructField('country', StringType(), True), StructField('customer_tier', StringType(), True), StructField('registration_date', DateType(), True), StructField('update_ts', TimestampType(), True)])

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
