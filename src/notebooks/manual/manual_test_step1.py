# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Test Step 1
# MAGIC 
# MAGIC Simple test notebook for manual operation type - Step 1

# COMMAND ----------

print("🚀 Starting Manual Test Step 1")
print("=" * 50)

# COMMAND ----------

# Get parameters passed by the framework
try:
    pipeline_group = dbutils.widgets.get("pipeline_group")
    operation_type = dbutils.widgets.get("operation_type")
    operation_index = dbutils.widgets.get("operation_index")
    total_operations = dbutils.widgets.get("total_operations")
    
    print(f"📋 Pipeline Group: {pipeline_group}")
    print(f"🔧 Operation Type: {operation_type}")
    print(f"📊 Step: {operation_index} of {total_operations}")
except:
    print("⚠️  Parameters not available (running in interactive mode)")

# COMMAND ----------

print("💾 Simulating data processing in Step 1...")
print("   - Reading source data")
print("   - Applying transformations")
print("   - Writing to intermediate table")

# COMMAND ----------

print("✅ Manual Test Step 1 completed successfully!")
print("🎯 Ready for Step 2")
print("=" * 50)
