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

# Access user-defined parameters (with defaults)
parallel_mode = dbutils.widgets.get("parallel_mode").lower() == "true"
max_workers = int(dbutils.widgets.get("max_workers"))
catalog = dbutils.widgets.get("catalog")

print(f"Pipeline Group: {pipeline_group}")
print(f"Operation Type: {operation_type}")
print(f"Operation Index: {operation_index}")
print(f"Total Operations: {total_operations}")
print(f"Parallel Mode: {parallel_mode}")
print(f"Max Workers: {max_workers}")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Parallel Task Processing

# COMMAND ----------

print("\n⚡ Simulating parallel task processing...")
print("  - This task runs in parallel with other order 2 tasks")
print("  - Depends on operation 1 (bronze layer) completion")
print("  - Processing independent data stream")
print("  - Applying specialized transformations")

# Use parameters in processing logic
if parallel_mode:
    print(f"  - PARALLEL MODE: Using {max_workers} workers")
    print(f"  - PARALLEL MODE: Processing catalog {catalog}")
else:
    print("  - SEQUENTIAL MODE: Processing one task at a time")

# Simulate some processing time based on parallel mode
import time
if parallel_mode:
    processing_time = 1.5  # Faster with parallel processing
else:
    processing_time = 2.5  # Slower with sequential processing
time.sleep(processing_time)

print("✅ Parallel task processing completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel Task Metrics

# COMMAND ----------

print("\n📊 Parallel Task Metrics:")
print("  - Records processed: 500")
print(f"  - Processing time: {processing_time:.1f} seconds")
print(f"  - Parallel mode: {parallel_mode}")
print(f"  - Max workers: {max_workers}")
print(f"  - Catalog: {catalog}")
print("  - Memory used: 192 MB")
print("  - CPU utilization: 75%")
print("  - Parallel execution: True")

# COMMAND ----------

print("\n🎯 Parallel task completed!")
print("This task ran concurrently with other order 2 tasks")