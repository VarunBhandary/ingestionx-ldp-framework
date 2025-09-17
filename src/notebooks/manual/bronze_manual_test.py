# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Manual Test Notebook
# MAGIC 
# MAGIC This notebook demonstrates a simple bronze layer processing step in a manual pipeline.
# MAGIC It shows how to access parameters passed by the framework.
# MAGIC 
# MAGIC **Pipeline Group**: `${pipeline_group}`
# MAGIC **Operation Type**: `${operation_type}`
# MAGIC **Operation Index**: `${operation_index}`
# MAGIC **Total Operations**: `${total_operations}`
# MAGIC 
# MAGIC **Note**: The parameters above are passed automatically by the framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Processing
# MAGIC 
# MAGIC This section simulates data ingestion and initial processing.

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER PROCESSING")
print("=" * 60)

# Access framework parameters
pipeline_group = dbutils.widgets.get("pipeline_group")
operation_type = dbutils.widgets.get("operation_type")
operation_index = dbutils.widgets.get("operation_index")
total_operations = dbutils.widgets.get("total_operations")

# Access user-defined parameters (passed from job configuration)
batch_size = dbutils.widgets.get("batch_size")
debug_mode = dbutils.widgets.get("debug_mode").lower() == "true"
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Pipeline Group: {pipeline_group}")
print(f"Operation Type: {operation_type}")
print(f"Operation Index: {operation_index}")
print(f"Total Operations: {total_operations}")
print(f"Batch Size: {batch_size}")
print(f"Debug Mode: {debug_mode}")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Data Ingestion

# COMMAND ----------

print("\n[INFO] Simulating data ingestion...")
print("  - Reading from source systems")
print("  - Validating data format")
print("  - Applying basic data quality checks")
print("  - Storing raw data")

# Use parameters in processing logic
if debug_mode:
    print(f"  - DEBUG: Processing batch size: {batch_size}")
    print(f"  - DEBUG: Target catalog: {catalog}")
    print(f"  - DEBUG: Target schema: {schema}")

# Simulate some processing time based on batch size
import time
processing_time = min(3, int(batch_size) / 1000)  # Scale processing time with batch size
time.sleep(processing_time)

print("[SUCCESS] Bronze layer processing completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Metrics

# COMMAND ----------

print("\n[INFO] Data Quality Metrics:")
print(f"  - Records processed: {batch_size}")
print("  - Data quality score: 95%")
print(f"  - Processing time: {processing_time:.1f} seconds")
print("  - Memory used: 128 MB")
if debug_mode:
    print(f"  - DEBUG: Catalog used: {catalog}")
    print(f"  - DEBUG: Schema used: {schema}")

# COMMAND ----------

print("\n[INFO] Bronze layer task completed!")
print(f"Next: Silver layer processing (operation {int(operation_index) + 1})")