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
pipeline_config = json.loads(spark.conf.get("pipeline_config", "{}"))
pipeline_group = spark.conf.get("pipeline_group", "default")

print(f"🚀 Starting Unified Pipeline")
print(f"📋 Pipeline Group: {pipeline_group}")
print(f"⚙️  Configuration: {pipeline_config}")

# COMMAND ----------

# Create all streaming tables upfront
print("📊 Creating streaming tables for all operations...")

# Bronze layer tables
bronze_tables = {}
silver_tables = {}

# Parse the unified configuration to identify all operations
if isinstance(pipeline_config, str):
    pipeline_config = json.loads(pipeline_config)

# Extract bronze and silver operations
bronze_operations = []
silver_operations = []

for key, value in pipeline_config.items():
    if key.startswith("bronze_"):
        bronze_operations.append((key, value))
    elif key.startswith("silver_"):
        silver_operations.append((key, value))

print(f"  📥 Found {len(bronze_operations)} bronze operations")
print(f"  🔄 Found {len(silver_operations)} silver operations")

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader
print("📥 Processing Bronze Layer Operations...")

for op_name, op_config in bronze_operations:
    try:
        # Extract configuration
        source_path = op_config.get("source_path", "")
        target_table = op_config.get("target_table", "")
        file_format = op_config.get("file_format", "csv")
        
        # Extract autoloader options
        schema_location = op_config.get("schema_location", "")
        checkpoint_location = op_config.get("checkpoint_location", "")
        
        # Get table name for the target
        table_name = target_table.split('.')[-1] if '.' in target_table else target_table
        
        print(f"  📁 Processing {op_name}: {source_path} -> {target_table}")
        
        # Create the streaming table
        dlt.create_streaming_table(table_name)
        
        # Create the streaming live table with autoloader
        @dlt.table(
            name=table_name,
            table_properties={
                "quality": "bronze",
                "operation": op_name,
                "pipelines.autoOptimize.optimizeWrite": "true",
                "pipelines.autoOptimize.autoCompact": "true"
            }
        )
        def bronze_table():
            # Build autoloader options
            autoloader_options = {
                "cloudFiles.schemaLocation": schema_location,
                "cloudFiles.checkpointLocation": checkpoint_location
            }
            
            # Add format-specific options
            if file_format.lower() == "csv":
                autoloader_options.update({
                    "header": op_config.get("header", "true"),
                    "inferSchema": "true"
                })
            elif file_format.lower() == "json":
                autoloader_options.update({
                    "multiline": "true"
                })
            
            # Add other autoloader options from config
            for key, value in op_config.items():
                if key.startswith("cloudFiles.") or key in ["maxFilesPerTrigger", "allowOverwrites"]:
                    autoloader_options[key] = value
            
            print(f"    🔧 Autoloader Options for {op_name}: {autoloader_options}")
            
            # Read from source using autoloader
            return (spark.readStream
                    .format("cloudFiles")
                    .options(**autoloader_options)
                    .option("cloudFiles.format", file_format)
                    .load(source_path))
        
        bronze_tables[op_name] = table_name
        print(f"    ✅ Created bronze table: {table_name}")
        
    except Exception as e:
        print(f"    ❌ Error processing bronze operation {op_name}: {e}")
        continue

# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC
print("🔄 Processing Silver Layer Operations...")

for op_name, op_config in silver_operations:
    try:
        # Extract configuration
        bronze_table = op_config.get("bronze_table", "")
        target_table = op_config.get("target_table", "")
        
        # Extract Auto CDC configuration
        keys = op_config.get("keys", [])
        except_column_list = op_config.get("track_history_except_column_list", [])
        stored_as_scd_type = op_config.get("stored_as_scd_type", "2")
        sequence_by = op_config.get("sequence_by", "_ingestion_timestamp")
        
        # Get table name for the target
        table_name = target_table.split('.')[-1] if '.' in target_table else target_table
        
        print(f"  🔄 Processing {op_name}: {bronze_table} -> {target_table}")
        
        # Create the target streaming table
        dlt.create_streaming_table(table_name)
        
        # Create source view from bronze table
        @dlt.view
        def bronze_source_view():
            return spark.readStream.table(bronze_table)
        
        # Create Auto CDC flow for SCD Type 2
        dlt.create_auto_cdc_flow(
            target=table_name,
            source="bronze_source_view",
            keys=keys,
            sequence_by=sequence_by,
            apply_as_deletes=expr("operation = 'DELETE'"),
            except_column_list=except_column_list,
            stored_as_scd_type=stored_as_scd_type
        )
        
        silver_tables[op_name] = table_name
        print(f"    ✅ Created silver table: {table_name}")
        
    except Exception as e:
        print(f"    ❌ Error processing silver operation {op_name}: {e}")
        continue

# COMMAND ----------

print(f"✅ Unified Pipeline completed successfully!")
print(f"📋 Pipeline Group: {pipeline_group}")
print(f"📊 Bronze Tables Created: {list(bronze_tables.values())}")
print(f"🔄 Silver Tables Created: {list(silver_tables.values())}")
print(f"🎯 Total Operations: {len(bronze_operations) + len(silver_operations)}")
