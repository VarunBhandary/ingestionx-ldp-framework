# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Bronze & Silver Layer
# MAGIC 
# MAGIC This notebook implements a **single declarative pipeline** that processes both:
# MAGIC - **Bronze Layer**: File ingestion using Autoloader
# MAGIC - **Silver Layer**: SCD Type 2 transformations using Auto CDC
# MAGIC 
# MAGIC The pipeline processes all operations in sequence within a single DLT pipeline.

# COMMAND ----------

import json
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get pipeline configuration
operations_config = json.loads(spark.conf.get("operations_config", "{}"))
pipeline_group = spark.conf.get("pipeline_group", "default")

# COMMAND ----------

# Extract bronze and silver operations
bronze_operations = []
silver_operations = []

for operation_name, operation_config in operations_config.items():
    if operation_name.startswith("bronze_"):
        bronze_operations.append((operation_name, operation_config))
    elif operation_name.startswith("silver_"):
        silver_operations.append((operation_name, operation_config))

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader

# Create streaming tables for all bronze operations
for op_name, op_config in bronze_operations:
    table_name = op_config.get("target_table", "").split('.')[-1] if '.' in op_config.get("target_table", "") else op_config.get("target_table", "")
    dlt.create_streaming_table(table_name)

# COMMAND ----------

# BRONZE TABLE: Products
if any(op[0] == "bronze_product" for op in bronze_operations):
    @dlt.table(
        name="products_new",
        table_properties={
            "quality": "bronze",
            "operation": "bronze_product",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true"
        }
    )
    def bronze_products():
        # Get config for bronze_product
        op_config = next(op[1] for op in bronze_operations if op[0] == "bronze_product")
        
        # Build autoloader options
        autoloader_options = {
            "cloudFiles.schemaLocation": op_config.get("schema_location", ""),
            "cloudFiles.checkpointLocation": op_config.get("checkpoint_location", ""),
            "cloudFiles.maxFilesPerTrigger": op_config.get("cloudFiles.maxFilesPerTrigger", "100"),
            "cloudFiles.allowOverwrites": op_config.get("cloudFiles.allowOverwrites", "false"),
            "header": op_config.get("header", "true"),
            "inferSchema": "true"
        }
        
        # Read from source using autoloader
        return (spark.readStream
                .format("cloudFiles")
                .options(**autoloader_options)
                .option("cloudFiles.format", "csv")
                .load(op_config.get("source_path", "")))

# COMMAND ----------

# BRONZE TABLE: Orders
if any(op[0] == "bronze_order" for op in bronze_operations):
    @dlt.table(
        name="orders_new",
        table_properties={
            "quality": "bronze",
            "operation": "bronze_order",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true"
        }
    )
    def bronze_orders():
        # Get config for bronze_order
        op_config = next(op[1] for op in bronze_operations if op[0] == "bronze_order")
        
        # Build autoloader options
        autoloader_options = {
            "cloudFiles.schemaLocation": op_config.get("schema_location", ""),
            "cloudFiles.checkpointLocation": op_config.get("checkpoint_location", ""),
            "cloudFiles.maxFilesPerTrigger": op_config.get("cloudFiles.maxFilesPerTrigger", "50"),
            "cloudFiles.allowOverwrites": op_config.get("cloudFiles.allowOverwrites", "false"),
            "multiline": "true"
        }
        
        # Read from source using autoloader
        return (spark.readStream
                .format("cloudFiles")
                .options(**autoloader_options)
                .option("cloudFiles.format", "json")
                .load(op_config.get("source_path", "")))

# COMMAND ----------

# BRONZE TABLE: Order Items
if any(op[0] == "bronze_order_items" for op in bronze_operations):
    @dlt.table(
        name="order_items_new",
        table_properties={
            "quality": "bronze",
            "operation": "bronze_order_items",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true"
        }
    )
    def bronze_order_items():
        # Get config for bronze_order_items
        op_config = next(op[1] for op in bronze_operations if op[0] == "bronze_order_items")
        
        # Build autoloader options
        autoloader_options = {
            "cloudFiles.schemaLocation": op_config.get("schema_location", ""),
            "cloudFiles.checkpointLocation": op_config.get("checkpoint_location", ""),
            "cloudFiles.maxFilesPerTrigger": op_config.get("cloudFiles.maxFilesPerTrigger", "50"),
            "cloudFiles.allowOverwrites": op_config.get("cloudFiles.allowOverwrites", "false"),
            "multiline": "true"
        }
        
        # Read from source using autoloader
        return (spark.readStream
                .option("cloudFiles.format", "json")
                .options(**autoloader_options)
                .load(op_config.get("source_path", "")))

# COMMAND ----------

# BRONZE TABLE: Customers
if any(op[0] == "bronze_customer" for op in bronze_operations):
    @dlt.table(
        name="customers_new",
        table_properties={
            "quality": "bronze",
            "operation": "bronze_customer",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true"
        }
    )
    def bronze_customers():
        # Get config for bronze_customer
        op_config = next(op[1] for op in bronze_operations if op[0] == "bronze_customer")
        
        # Build autoloader options
        autoloader_options = {
            "cloudFiles.schemaLocation": op_config.get("schema_location", ""),
            "cloudFiles.checkpointLocation": op_config.get("checkpoint_location", ""),
            "cloudFiles.maxFilesPerTrigger": op_config.get("cloudFiles.maxFilesPerTrigger", "100"),
            "cloudFiles.allowOverwrites": op_config.get("cloudFiles.allowOverwrites", "false"),
            "header": op_config.get("header", "true"),
            "inferSchema": "true"
        }
        
        # Read from source using autoloader
        return (spark.readStream
                .format("cloudFiles")
                .options(**autoloader_options)
                .option("cloudFiles.format", "csv")
                .load(op_config.get("source_path", "")))

# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC

# Create streaming tables for all silver operations
for op_name, op_config in silver_operations:
    table_name = op_config.get("target_table", "").split('.')[-1] if '.' in op_config.get("target_table", "") else op_config.get("target_table", "")
    dlt.create_streaming_table(table_name)

# COMMAND ----------

# SILVER TABLE: Products SCD Type 2
if any(op[0] == "silver_product" for op in silver_operations):
    @dlt.view
    def bronze_products_source():
        return spark.readStream.table("products_new")
    
    dlt.create_auto_cdc_flow(
        target="products_scd2",
        target_table_name="products_scd2",
        source="bronze_products_source",
        keys=["product_id"],
        sequence_by="_ingestion_timestamp",
        apply_as_deletes=expr("operation = 'DELETE'"),
        except_column_list=["product_name", "category", "price", "brand", "description", "stock_quantity"],
        stored_as_scd_type="2"
    )

# COMMAND ----------

# SILVER TABLE: Orders SCD Type 2
if any(op[0] == "silver_order" for op in silver_operations):
    @dlt.view
    def bronze_orders_source():
        return spark.readStream.table("orders_new")
    
    dlt.create_auto_cdc_flow(
        target="orders_scd2",
        target_table_name="orders_scd2",
        source="bronze_orders_source",
        keys=["order_id"],
        sequence_by="_ingestion_timestamp",
        apply_as_deletes=expr("operation = 'DELETE'"),
        except_column_list=["order_status", "total_amount", "shipping_address"],
        stored_as_scd_type="2"
    )

# COMMAND ----------

# SILVER TABLE: Order Items SCD Type 2
if any(op[0] == "silver_order_items" for op in silver_operations):
    @dlt.view
    def bronze_order_items_source():
        return spark.readStream.table("order_items_new")
    
    dlt.create_auto_cdc_flow(
        target="order_items_scd2",
        target_table_name="order_items_scd2",
        source="bronze_order_items_source",
        keys=["order_item_id"],
        sequence_by="_ingestion_timestamp",
        apply_as_deletes=expr("operation = 'DELETE'"),
        except_column_list=["quantity", "unit_price", "discount"],
        stored_as_scd_type="2"
    )

# COMMAND ----------

# SILVER TABLE: Customers SCD Type 2
if any(op[0] == "silver_customer" for op in silver_operations):
    @dlt.view
    def bronze_customers_source():
        return spark.readStream.table("customers_new")
    
    dlt.create_auto_cdc_flow(
        target="customers_scd2",
        target_table_name="customers_scd2",
        source="bronze_customers_source",
        keys=["customer_id"],
        sequence_by="_ingestion_timestamp",
        apply_as_deletes=expr("operation = 'DELETE'"),
        except_column_list=["name", "email", "phone", "address", "status"],
        stored_as_scd_type="2"
    )

# COMMAND ----------

# Pipeline execution completed
