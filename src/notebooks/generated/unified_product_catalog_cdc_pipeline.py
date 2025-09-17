# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Product Catalog Cdc Pipeline
# MAGIC 
# MAGIC This notebook implements a unified pipeline for product_catalog_cdc_pipeline with:
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
schema = StructType([StructField('product_id', StringType(), False), StructField('product_name', StringType(), True), StructField('category', StringType(), True), StructField('price', DoubleType(), True), StructField('description', StringType(), True), StructField('status', StringType(), True), StructField('created_at', TimestampType(), True), StructField('updated_at', TimestampType(), True), StructField('deleted_at', TimestampType(), True)])

@dlt.table(
    name="my_test_catalog.adls_bronze.product_catalog_cdc",
    table_properties={
        "quality": "bronze",
        "operation": "bronze_product_catalog_cdc",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def product_catalog_cdc():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.checkpointLocation", "/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/checkpoint/product_catalog_cdc")
            .option("cloudFiles.maxFilesPerTrigger", "50")
            .option("cloudFiles.allowOverwrites", "false")
            .option("header", "true")
            .option("cloudFiles.rescuedDataColumn", "corrupt_data")
            .option("cloudFiles.validateOptions", "false")
            .option("cloudFiles.format", "csv")
            .schema(schema)  # Apply fixed schema for data validation
            .load("/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/product_catalog_cdc")
            .selectExpr("*", "_metadata as source_metadata", "CASE WHEN deleted_at IS NOT NULL THEN 'DELETE' WHEN created_at = updated_at THEN 'INSERT' ELSE 'UPDATE' END as cdc_operation", "COALESCE(updated_at, created_at) as sequence_ts", "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("my_test_catalog.adls_silver.product_catalog_cdc_scd2")

# COMMAND ----------

@dlt.view
def bronze_product_catalog_cdc_scd2_source():
    return spark.readStream.table("my_test_catalog.adls_bronze.product_catalog_cdc").selectExpr("product_id", "product_name as product_title", "category as product_category", "price as unit_price", "description as product_description", "status as product_status", "created_at as effective_start_date", "updated_at as last_modified_date", "deleted_at as soft_delete_date", "cdc_operation", "sequence_ts")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="my_test_catalog.adls_silver.product_catalog_cdc_scd2",
    source="bronze_product_catalog_cdc_scd2_source",
    **{"keys": ["product_id"], "track_history_except_column_list": ["product_title", "product_category", "unit_price", "product_description", "product_status"], "stored_as_scd_type": "2", "sequence_by": "sequence_ts"}
)


# COMMAND ----------

# Pipeline execution completed
