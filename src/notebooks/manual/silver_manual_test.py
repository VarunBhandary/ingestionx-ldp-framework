# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Manual Test Notebook
# MAGIC 
# MAGIC This notebook demonstrates a simple silver layer processing step in a manual pipeline.
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
# MAGIC ## Silver Layer Processing
# MAGIC 
# MAGIC This section simulates data transformation and cleansing.

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER PROCESSING")
print("=" * 60)

# Access framework parameters
pipeline_group = dbutils.widgets.get("pipeline_group")
operation_type = dbutils.widgets.get("operation_type")
operation_index = dbutils.widgets.get("operation_index")
total_operations = dbutils.widgets.get("total_operations")

# Access user-defined parameters (with defaults)
environment = dbutils.widgets.get("environment")
retry_count = int(dbutils.widgets.get("retry_count"))
volume = dbutils.widgets.get("volume")

print(f"Pipeline Group: {pipeline_group}")
print(f"Operation Type: {operation_type}")
print(f"Operation Index: {operation_index}")
print(f"Total Operations: {total_operations}")
print(f"Environment: {environment}")
print(f"Retry Count: {retry_count}")
print(f"Volume: {volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Data Transformation

# COMMAND ----------

print("\n🔄 Simulating data transformation...")
print("  - Reading from bronze layer")
print("  - Applying business rules")
print("  - Data cleansing and validation")
print("  - Enriching with additional data")
print("  - Storing transformed data")

# Use parameters in processing logic
print(f"  - Environment: {environment}")
print(f"  - Volume: {volume}")
print(f"  - Retry attempts: {retry_count}")

# Simulate some processing time based on environment
import time
if environment == "production":
    processing_time = 4  # Longer processing for production
else:
    processing_time = 2  # Faster for dev/test
time.sleep(processing_time)

print("✅ Silver layer processing completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Metrics

# COMMAND ----------

print("\n📊 Transformation Metrics:")
print("  - Records transformed: 1,000")
print("  - Data quality score: 98%")
print(f"  - Processing time: {processing_time:.1f} seconds")
print("  - Memory used: 256 MB")
print("  - Business rules applied: 15")
print(f"  - Environment: {environment}")
print(f"  - Volume: {volume}")
print(f"  - Retry count: {retry_count}")

# COMMAND ----------

print("\n🎯 Silver layer task completed!")
print(f"Next: Gold layer processing (operation {int(operation_index) + 1})")