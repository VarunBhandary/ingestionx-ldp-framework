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
from pyspark.sql.types import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader


# COMMAND ----------
# Define fixed schema for data validation
schema = StructType([StructField('product_id', StringType(), False), StructField('product_name', StringType(), True), StructField('category', StringType(), True), StructField('brand', StringType(), True), StructField('sku', StringType(), True), StructField('quantity_on_hand', IntegerType(), True), StructField('quantity_reserved', IntegerType(), True), StructField('unit_cost', DoubleType(), True), StructField('unit_price', DoubleType(), True), StructField('warehouse_location', StringType(), True), StructField('last_restocked', DateType(), True), StructField('reorder_level', IntegerType(), True), StructField('supplier_id', StringType(), True), StructField('batch_date', StringType(), True), StructField('created_ts', TimestampType(), True)])

@dlt.table(
    name="my_test_catalog.adls_bronze.inventory_demo",
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
            .option("cloudFiles.checkpointLocation", "/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/checkpoint/inventory")
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .option("cloudFiles.allowOverwrites", "false")
            .option("header", "true")
            .option("cloudFiles.cleanSource", "MOVE")
            .option("cloudFiles.cleanSource.moveDestination", "/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/archive/inventory")
            .option("cloudFiles.rescuedDataColumn", "corrupt_data")
            .option("cloudFiles.validateOptions", "false")
            .option("cloudFiles.format", "csv")
            .schema(schema)  # Apply fixed schema for data validation
            .load("/Volumes/my_test_catalog/dbdemos_autoloader/raw_data/inventory")
            .selectExpr("*", "_metadata as source_metadata", "current_timestamp() as _ingestion_timestamp"))


# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC


# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("my_test_catalog.adls_silver.inventory_demo_scd1")

# COMMAND ----------

@dlt.view
def bronze_inventory_demo_scd1_source():
    return spark.readStream.table("my_test_catalog.adls_bronze.inventory_demo")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="my_test_catalog.adls_silver.inventory_demo_scd1",
    source="bronze_inventory_demo_scd1_source",
    **{"keys": ["product_id"], "stored_as_scd_type": "1", "sequence_by": "created_ts"}
)


# COMMAND ----------

# Pipeline execution completed
