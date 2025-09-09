# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Manual Test Notebook
# MAGIC 
# MAGIC This notebook demonstrates a parallel processing step in a manual pipeline.
# MAGIC It shows how multiple tasks can run concurrently after a common dependency.
# MAGIC 
# MAGIC **Pipeline Group**: `${pipeline_group}`
# MAGIC **Operation Type**: `${operation_type}`
# MAGIC **Operation Index**: `${operation_index}`
# MAGIC **Total Operations**: `${total_operations}`
# MAGIC 
# MAGIC **Note**: The parameters above are passed automatically by the framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel Processing Logic
# MAGIC 
# MAGIC This section simulates a task that runs in parallel with other tasks in the same order,
# MAGIC after a specified dependency is met.

# COMMAND ----------

print("=" * 60)
print("PARALLEL PROCESSING TASK")
print("=" * 60)

# Access framework parameters
pipeline_group = dbutils.widgets.get("pipeline_group")
operation_type = dbutils.widgets.get("operation_type")
operation_index = dbutils.widgets.get("operation_index")
total_operations = dbutils.widgets.get("total_operations")

print(f"Pipeline Group: {pipeline_group}")
print(f"Operation Type: {operation_type}")
print(f"Operation Index: {operation_index}")
print(f"Total Operations: {total_operations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Parallel Task Processing

# COMMAND ----------

print("\n⚡ Simulating parallel task processing...")
print("  - This task runs in parallel with other order 2 tasks")
print("  - Depends on operation 1 (bronze layer) completion")
print("  - Processing independent data stream")
print("  - Applying specialized transformations")

# Simulate some processing time
import time
time.sleep(2)

print("✅ Parallel task processing completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel Task Metrics

# COMMAND ----------

print("\n📊 Parallel Task Metrics:")
print("  - Records processed: 500")
print("  - Processing time: 2.1 seconds")
print("  - Memory used: 192 MB")
print("  - CPU utilization: 75%")
print("  - Parallel execution: True")

# COMMAND ----------

print("\n🎯 Parallel task completed!")
print("This task ran concurrently with other order 2 tasks")