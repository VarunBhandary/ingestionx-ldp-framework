#!/usr/bin/env python3
"""
Notebook Generator for Unified Pipeline Framework

This script generates static, simple DLT notebooks for each pipeline group
based on the unified configuration. Each generated notebook is completely static
with no loops or dynamic logic.
"""

import pandas as pd
import json
import os
from pathlib import Path

def generate_bronze_table_notebook(op_name, op_config, notebook_path):
    """Generate a simple bronze table notebook for a single operation"""
    
    # Extract configuration
    source_path = op_config.get("source_path", "")
    target_table = op_config.get("target_table", "")
    file_format = op_config.get("file_format", "csv")
    
    # Extract autoloader options
    schema_location = op_config.get("schema_location", "")
    checkpoint_location = op_config.get("checkpoint_location", "")
    
    # Get table name for the target
    table_name = target_table.split('.')[-1] if '.' in target_table else target_table
    
    # Build autoloader options
    autoloader_options = {
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.checkpointLocation": checkpoint_location,
        "cloudFiles.maxFilesPerTrigger": op_config.get("cloudFiles.maxFilesPerTrigger", "100"),
        "cloudFiles.allowOverwrites": op_config.get("cloudFiles.allowOverwrites", "false")
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
    
    # Generate the notebook content
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {op_name.replace('_', ' ').title()} - Bronze Table
# MAGIC 
# MAGIC This notebook ingests data from {source_path} into {target_table}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    name="{table_name}",
    table_properties={{
        "quality": "bronze",
        "operation": "{op_name}",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }}
)
        def {table_name}():
            # Read from source using autoloader and add audit columns using selectExpr
            return (spark.readStream
                    .format("cloudFiles")
                    .options(**{json.dumps(autoloader_options)})
                    .option("cloudFiles.format", "{file_format}")
                    .load("{source_path}")
                    .selectExpr("*", 
                                "current_timestamp() as _ingestion_timestamp"))

# COMMAND ----------

# Pipeline execution completed
'''
    
    # Write the notebook
    with open(notebook_path, 'w') as f:
        f.write(notebook_content)
    
    print(f"Generated bronze notebook: {notebook_path}")

def generate_silver_table_notebook(op_name, op_config, notebook_path):
    """Generate a simple silver table notebook for a single operation"""
    
    # Extract configuration
    bronze_table = op_config.get("source_path", "")  # This should be the bronze table path
    target_table = op_config.get("target_table", "")
    
    # Extract Auto CDC configuration
    keys = op_config.get("keys", [])
    except_column_list = op_config.get("track_history_except_column_list", [])
    stored_as_scd_type = op_config.get("stored_as_scd_type", "2")
    sequence_by = op_config.get("sequence_by", "last_updated")
    
    # Get table names
    bronze_table_name = bronze_table.split('.')[-1] if '.' in bronze_table else bronze_table
    target_table_name = target_table.split('.')[-1] if '.' in target_table else target_table
    
    # Generate the notebook content
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {op_name.replace('_', ' ').title()} - Silver Table SCD Type 2
# MAGIC 
# MAGIC This notebook creates SCD Type 2 table from {bronze_table} into {target_table}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("{target_table_name}")

# COMMAND ----------

@dlt.view
def bronze_source_view():
    return spark.readStream.table("{bronze_table_name}")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="{target_table_name}",
    source="bronze_source_view",
    keys={json.dumps(keys)},
    sequence_by="{sequence_by}",
    except_column_list={json.dumps(except_column_list)},
    stored_as_scd_type="{stored_as_scd_type}"
)

# COMMAND ----------

# Pipeline execution completed
'''
    
    # Write the notebook
    with open(notebook_path, 'w') as f:
        f.write(notebook_content)
    
    print(f"Generated silver notebook: {notebook_path}")

def generate_pipeline_group_notebook(pipeline_group, group_operations, output_dir):
    """Generate a complete notebook for a pipeline group"""
    
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Separate bronze and silver operations
    bronze_operations = []
    silver_operations = []
    
    for op_name, op_config in group_operations.items():
        if op_name.startswith("bronze_"):
            bronze_operations.append((op_name, op_config))
        elif op_name.startswith("silver_"):
            silver_operations.append((op_name, op_config))
    
    # Generate only the combined notebook for the entire pipeline group
    combined_notebook_path = output_dir / f"unified_{pipeline_group}.py"
    generate_combined_notebook(pipeline_group, bronze_operations, silver_operations, combined_notebook_path)

def generate_combined_notebook(pipeline_group, bronze_operations, silver_operations, notebook_path):
    """Generate a combined notebook for all operations in a pipeline group"""
    
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - {pipeline_group.replace('_', ' ').title()}
# MAGIC 
# MAGIC This notebook implements a unified pipeline for {pipeline_group} with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader
'''
    
    # Add bronze tables
    for op_name, op_config in bronze_operations:
        source_path = op_config.get("source_path", "")
        target_table = op_config.get("target_table", "")
        file_format = op_config.get("file_format", "csv")
        table_name = target_table.split('.')[-1] if '.' in target_table else target_table
        
        # Build autoloader options
        autoloader_options = {
            "cloudFiles.schemaLocation": op_config.get("cloudFiles.schemaLocation", ""),
            "cloudFiles.checkpointLocation": op_config.get("cloudFiles.checkpointLocation", ""),
            "cloudFiles.maxFilesPerTrigger": op_config.get("cloudFiles.maxFilesPerTrigger", "100"),
            "cloudFiles.allowOverwrites": op_config.get("cloudFiles.allowOverwrites", "false")
        }
        
        if file_format.lower() == "csv":
            autoloader_options.update({
                "header": op_config.get("header", "true"),
                "inferSchema": "true"
            })
        elif file_format.lower() == "json":
            autoloader_options.update({
                "multiline": "true"
            })
        
        notebook_content += f'''

# COMMAND ----------

@dlt.table(
    name="{target_table}",
    table_properties={{
        "quality": "bronze",
        "operation": "{op_name}",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }}
)
def {table_name}():
    # Read from source using autoloader and add audit columns using selectExpr
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.schemaLocation", "{op_config.get("cloudFiles.schemaLocation", "")}")
            .option("cloudFiles.maxFilesPerTrigger", "{op_config.get("cloudFiles.maxFilesPerTrigger", "100")}")
            .option("cloudFiles.allowOverwrites", "{op_config.get("cloudFiles.allowOverwrites", "false")}")
            .option("cloudFiles.format", "{file_format}")'''
        
        # Add format-specific options
        if file_format.lower() == "csv":
            notebook_content += f'''
            .option("header", "{op_config.get("header", "true")}")
            .option("inferSchema", "true")'''
        elif file_format.lower() == "json":
            notebook_content += f'''
            .option("multiline", "true")'''
        
        notebook_content += f'''
            .load("{source_path}")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp"))
'''
    
    # Add silver layer
    notebook_content += '''

# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC
'''
    
    # Add silver tables
    for op_name, op_config in silver_operations:
        bronze_table = op_config.get("source_path", "")  # This should be the bronze table path
        target_table = op_config.get("target_table", "")
        keys = op_config.get("keys", [])
        except_column_list = op_config.get("track_history_except_column_list", [])
        stored_as_scd_type = op_config.get("stored_as_scd_type", "2")
        sequence_by = op_config.get("sequence_by", "_ingestion_timestamp")
        
        bronze_table_name = bronze_table.split('.')[-1] if '.' in bronze_table else bronze_table
        target_table_name = target_table.split('.')[-1] if '.' in target_table else target_table
        
        notebook_content += f'''

# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("{target_table}")

# COMMAND ----------

@dlt.view
def bronze_{target_table_name}_source():
    return spark.readStream.table("{bronze_table}")

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="{target_table}",
    source="bronze_{target_table_name}_source",
    keys={json.dumps(keys)},
    sequence_by="{sequence_by}",
    except_column_list={json.dumps(except_column_list)},
    stored_as_scd_type="{stored_as_scd_type}"
)
'''
    
    notebook_content += '''

# COMMAND ----------

# Pipeline execution completed
'''
    
    # Write the notebook
    with open(notebook_path, 'w') as f:
        f.write(notebook_content)
    
    print(f"Generated combined notebook: {notebook_path}")

def main():
    """Main function to generate notebooks for all pipeline groups"""
    
    # Read the unified configuration
    config_file = "config/unified_pipeline_config.tsv"
    if not os.path.exists(config_file):
        print(f"Configuration file not found: {config_file}")
        return
    
    # Load configuration
    df = pd.read_csv(config_file, sep='\t')
    
    # Group by pipeline_group
    pipeline_groups = df.groupby('pipeline_group')
    
    # Create output directory
    output_dir = "src/notebooks/generated"
    
    # Generate notebooks for each pipeline group
    for group_name, group_df in pipeline_groups:
        print(f"\nGenerating notebooks for pipeline group: {group_name}")
        
        # Convert group_df to operations_config format
        operations_config = {}
        for _, row in group_df.iterrows():
            operation_type = row['operation_type']
            if operation_type == 'bronze':
                op_name = f"bronze_{row['target_table'].split('.')[-1].replace('_new', '')}"
                
                # Parse the pipeline_config JSON for bronze operations
                try:
                    pipeline_config = json.loads(row['pipeline_config'])
                    schema_location = pipeline_config.get('cloudFiles.schemaLocation', '')
                    checkpoint_location = pipeline_config.get('cloudFiles.checkpointLocation', '')
                    max_files_per_trigger = pipeline_config.get('cloudFiles.maxFilesPerTrigger', '100')
                    allow_overwrites = pipeline_config.get('cloudFiles.allowOverwrites', 'false')
                    header = pipeline_config.get('header', 'true')
                except (json.JSONDecodeError, TypeError):
                    # Fallback if JSON parsing fails
                    schema_location = ''
                    checkpoint_location = ''
                    max_files_per_trigger = '100'
                    allow_overwrites = 'false'
                    header = 'true'
                
                operations_config[op_name] = {
                    'source_path': row['source_path'],
                    'target_table': row['target_table'],
                    'file_format': row['file_format'],
                    'cloudFiles.schemaLocation': schema_location,
                    'cloudFiles.maxFilesPerTrigger': max_files_per_trigger,
                    'cloudFiles.allowOverwrites': allow_overwrites,
                    'header': header
                }
            elif operation_type == 'silver':
                op_name = f"silver_{row['target_table'].split('.')[-1].replace('_scd2', '')}"
                
                # Parse the pipeline_config JSON for silver operations
                try:
                    pipeline_config = json.loads(row['pipeline_config'])
                    keys = pipeline_config.get('keys', [])
                    track_history_except_column_list = pipeline_config.get('track_history_except_column_list', [])
                    stored_as_scd_type = pipeline_config.get('stored_as_scd_type', '2')
                    sequence_by = pipeline_config.get('sequence_by', '_ingestion_timestamp')
                except (json.JSONDecodeError, TypeError):
                    # Fallback if JSON parsing fails
                    keys = []
                    track_history_except_column_list = []
                    stored_as_scd_type = '2'
                    sequence_by = 'last_updated'
                
                operations_config[op_name] = {
                    'source_path': row['source_path'],  # This should be the bronze table
                    'target_table': row['target_table'],
                    'keys': keys,
                    'track_history_except_column_list': track_history_except_column_list,
                    'stored_as_scd_type': stored_as_scd_type,
                    'sequence_by': sequence_by
                }
        
        # Generate notebooks for this group
        generate_pipeline_group_notebook(group_name, operations_config, output_dir)
    
    print(f"\nAll notebooks generated in: {output_dir}")

if __name__ == "__main__":
    main()
