# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - Bronze & Silver Layer
# MAGIC 
# MAGIC This notebook implements a unified DLT pipeline that can handle both:
# MAGIC - **Bronze Layer**: File ingestion using Autoloader
# MAGIC - **Silver Layer**: SCD Type 2 transformations using Auto CDC
# MAGIC 
# MAGIC The pipeline type and configuration are determined by the `pipeline_type` and `pipeline_config` parameters.

# COMMAND ----------

import json
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get pipeline configuration
pipeline_config = json.loads(spark.conf.get("pipeline_config", "{}"))
pipeline_type = spark.conf.get("pipeline_type", "bronze")

print(f"🚀 Starting Unified Pipeline")
print(f"📋 Pipeline Type: {pipeline_type}")
print(f"⚙️  Configuration: {pipeline_config}")

# COMMAND ----------

if pipeline_type == "bronze":
    # BRONZE LAYER - File Ingestion with Autoloader
    print("📥 Processing Bronze Layer - File Ingestion")
    
    # Extract configuration
    source_type = pipeline_config.get("source_type", "adls")
    source_path = pipeline_config.get("source_path", "")
    target_table = pipeline_config.get("target_table", "")
    file_format = pipeline_config.get("file_format", "csv")
    
    # Extract autoloader options
    schema_location = pipeline_config.get("schema_location", "")
    checkpoint_location = pipeline_config.get("checkpoint_location", "")
    
    # Get table name for the target
    table_name = target_table.split('.')[-1] if '.' in target_table else target_table
    
    print(f"  📁 Source: {source_path}")
    print(f"  🎯 Target: {target_table}")
    print(f"  📄 Format: {file_format}")
    print(f"  📊 Table Name: {table_name}")
    
    # Create the streaming table
    dlt.create_streaming_table(table_name)
    
    # Create the streaming live table with autoloader
    @dlt.table(
        name=table_name,
        table_properties={
            "quality": "bronze",
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
                "header": pipeline_config.get("header", "true"),
                "inferSchema": "true"
            })
        elif file_format.lower() == "json":
            autoloader_options.update({
                "multiline": "true"
            })
        
        # Add other autoloader options from config
        for key, value in pipeline_config.items():
            if key.startswith("cloudFiles.") or key in ["maxFilesPerTrigger", "allowOverwrites"]:
                autoloader_options[key] = value
        
        print(f"  🔧 Autoloader Options: {autoloader_options}")
        
        # Read from source using autoloader
        return (spark.readStream
                .format("cloudFiles")
                .options(**autoloader_options)
                .option("cloudFiles.format", file_format)
                .load(source_path))

elif pipeline_type == "silver":
    # SILVER LAYER - SCD Type 2 with Auto CDC
    print("🔄 Processing Silver Layer - SCD Type 2 Transformation")
    
    # Extract configuration
    bronze_table = pipeline_config.get("bronze_table", "")
    target_table = pipeline_config.get("target_table", "")
    
    # Extract Auto CDC configuration
    keys = pipeline_config.get("keys", [])
    except_column_list = pipeline_config.get("track_history_except_column_list", [])
    stored_as_scd_type = pipeline_config.get("stored_as_scd_type", "2")
    sequence_by = pipeline_config.get("sequence_by", "_ingestion_timestamp")
    
    # Get table name for the target
    table_name = target_table.split('.')[-1] if '.' in target_table else target_table
    
    print(f"  📥 Source Table: {bronze_table}")
    print(f"  🎯 Target Table: {target_table}")
    print(f"  🔑 Keys: {keys}")
    print(f"  📊 Table Name: {table_name}")
    
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

else:
    raise ValueError(f"Unsupported pipeline_type: {pipeline_type}. Must be 'bronze' or 'silver'")

# COMMAND ----------

print(f"✅ Unified Pipeline completed successfully!")
print(f"🎯 Pipeline Type: {pipeline_type}")
if pipeline_type == "bronze":
    print(f"📁 Ingesting from: {source_path}")
    print(f"📊 Target table: {table_name}")
elif pipeline_type == "silver":
    print(f"🔄 Transforming from: {bronze_table}")
    print(f"📊 Target table: {table_name}")
