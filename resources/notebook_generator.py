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
import sys
from pathlib import Path

# Add the src directory to the path so we can import our utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from utils.schema_converter import convert_json_schema_to_spark, convert_json_schema_to_spark_string, validate_schema_definition

def generate_bronze_table_notebook(op_name, op_config, notebook_path):
    """Generate a simple bronze table notebook for a single operation"""
    
    # Extract configuration
    source_path = op_config.get("source_path", "")
    target_table = op_config.get("target_table", "")
    file_format = op_config.get("file_format", "csv")
    
    # Get table name for the target
    table_name = target_table.split('.')[-1] if '.' in target_table else target_table
    
    # Check if inline schema is provided
    schema_definition = op_config.get("schema")
    schema_code = ""
    
    if schema_definition:
        # Validate schema definition
        errors = validate_schema_definition(schema_definition)
        if errors:
            print(f"Warning: Schema validation errors for {op_name}: {errors}")
            schema_code = ""
        else:
            # Convert JSON schema to PySpark schema code
            try:
                # Try to use PySpark objects first, fallback to string representation
                try:
                    spark_schema = convert_json_schema_to_spark(schema_definition)
                    schema_code = f'''
# Define fixed schema for data validation
from pyspark.sql.types import *

schema = {spark_schema}'''
                except ImportError:
                    # PySpark not available, use string representation
                    spark_schema_str = convert_json_schema_to_spark_string(schema_definition)
                    schema_code = f'''
# Define fixed schema for data validation
from pyspark.sql.types import *

schema = {spark_schema_str}'''
            except Exception as e:
                print(f"Warning: Error converting schema for {op_name}: {e}")
                schema_code = ""
    
    # Build autoloader options from config - only include options that are specified
    autoloader_options = {}
    
    # Add all options from the config that start with cloudFiles. or are format-specific
    # Exclude schema and schema_location since we're handling schema differently now
    for key, value in op_config.items():
        if key.startswith("cloudFiles.") or key in ["header", "inferSchema", "multiline"]:
            # Skip schema-related options if we have inline schema
            if schema_definition and key in ["cloudFiles.schemaLocation", "inferSchema"]:
                continue
            autoloader_options[key] = value
    
    # Generate individual .option() calls for each autoloader option
    option_lines = []
    for key, value in autoloader_options.items():
        option_lines.append(f'            .option("{key}", "{value}")')
    
    options_code = '\n'.join(option_lines) if option_lines else ''
    
    # Generate schema application code
    schema_application = ""
    if schema_definition:
        schema_application = '''
            .schema(schema)  # Apply fixed schema for data validation'''
    
    # Check if file metadata should be included
    include_file_metadata = op_config.get("include_file_metadata", False)
    # Convert string values to boolean
    if isinstance(include_file_metadata, str):
        include_file_metadata = include_file_metadata.lower() in ['true', '1', 'yes']
    
    # Handle custom expressions
    custom_expr = op_config.get("custom_expr", "")
    if custom_expr:
        # Check if custom expression starts with * - if so, use selectExpr with * and remaining expression
        if custom_expr.strip().startswith('*'):
            # Use selectExpr() with * as separate argument and the remaining expression
            select_method = "selectExpr"
            # Remove * from the expression
            remaining_expr = custom_expr.strip()[1:].strip()
            if remaining_expr.startswith(','):
                remaining_expr = remaining_expr[1:].strip()
            
            # Add file metadata if enabled
            if include_file_metadata:
                # Check if _metadata is already in the expression to avoid conflicts
                if '_metadata' not in remaining_expr:
                    remaining_expr = f'_metadata as source_metadata, {remaining_expr}'
                else:
                    # If _metadata exists, rename it to source_metadata to avoid conflicts
                    remaining_expr = remaining_expr.replace('_metadata', 'source_metadata')
            
            # Pass * and the remaining expression as separate arguments
            select_expr = f'"*", "{remaining_expr}"'
        else:
            # Use selectExpr() for expressions without *
            select_method = "selectExpr"
            
            # Add file metadata if enabled and not already present
            if include_file_metadata and '_metadata' not in custom_expr:
                select_expr = f'"{custom_expr}", "_metadata as source_metadata"'
            elif include_file_metadata and '_metadata' in custom_expr:
                # Replace _metadata with source_metadata to avoid conflicts
                select_expr = f'"{custom_expr.replace("_metadata", "source_metadata")}"'
            else:
                select_expr = f'"{custom_expr}"'
    else:
        select_method = "selectExpr"
        if include_file_metadata:
            select_expr = '"*", "_metadata as source_metadata", "current_timestamp() as _ingestion_timestamp"'
        else:
            select_expr = '"*", "current_timestamp() as _ingestion_timestamp"'

    # Generate the notebook content
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {op_name.replace('_', ' ').title()} - Bronze Table
# MAGIC 
# MAGIC This notebook ingests data from {source_path} into {target_table}
# MAGIC {"with fixed schema validation" if schema_definition else "with schema inference"}{" and custom expressions" if custom_expr else ""}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *{schema_code}

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
            .format("cloudFiles"){f'''
{options_code}''' if options_code else ''}
            .option("cloudFiles.format", "{file_format}"){schema_application}
            .load("{source_path}")
            .{select_method}({select_expr}))

# COMMAND ----------

# Pipeline execution completed
'''
    
    # Write the notebook
    with open(notebook_path, 'w') as f:
        f.write(notebook_content)
    
    print(f"Generated bronze notebook: {notebook_path}")
    if schema_definition:
        print(f"  - Using fixed schema with {len(schema_definition.get('fields', []))} fields")
    else:
        print(f"  - Using schema inference")

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
    
    # Handle custom expressions for silver layer
    custom_expr = op_config.get("custom_expr", "")
    if custom_expr:
        source_view_code = f'''@dlt.view
def bronze_source_view():
    return spark.readStream.table("{bronze_table_name}").selectExpr("{custom_expr}")'''
    else:
        source_view_code = f'''@dlt.view
def bronze_source_view():
    return spark.readStream.table("{bronze_table_name}")'''
    
    # Generate the notebook content
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {op_name.replace('_', ' ').title()} - Silver Table SCD Type 2
# MAGIC 
# MAGIC This notebook creates SCD Type 2 table from {bronze_table} into {target_table}{" with custom column mapping" if custom_expr else ""}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("{target_table_name}")

# COMMAND ----------

{source_view_code}

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

def generate_gold_table_notebook(op_name, op_config, notebook_path):
    """Generate a gold table notebook for analytics and business intelligence"""
    
    # Extract configuration
    target_table = op_config.get("target_table", "")
    
    # Generate the notebook content with proper Databricks notebook format
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Table: {op_name.replace('_', ' ').title()}
# MAGIC Creates a simple analytics table for business intelligence

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Read from the silver SCD2 table (replace with your silver table name)
# Note: Update this table reference to match your actual silver table
silver_df = spark.read.table("${var.catalog_name}.${var.schema_name}.products_scd2")

# COMMAND ----------

# Debug: Show available columns in the silver table
print("[DEBUG] Available columns in silver table:")
silver_df.printSchema()
print(f"[INFO] Total records in silver table: {silver_df.count()}")

# COMMAND ----------

# Create analytics table with business logic
analytics_df = silver_df.select(
    # Primary key
    col("product_id").cast("string").alias("product_id"),
    
    # Business analytics columns
    lit("Analytics Data").alias("data_type"),
    lit("2024").alias("year"),
    lit("Q1").alias("quarter"),
    
    # Dummy calculated field (using literal values instead of potentially missing columns)
    lit(1000).alias("inventory_value"),
    
    # Processing timestamp
    current_timestamp().alias("processed_at")
)

# COMMAND ----------

# Write to target table
analytics_df.write.mode("overwrite").saveAsTable("{target_table}")

# COMMAND ----------

print("[SUCCESS] Gold table created successfully!")
print(f"[INFO] Records processed: {{analytics_df.count()}}")
print(f"[INFO] Target table: {target_table}")
'''
    
    # Write the notebook
    with open(notebook_path, 'w') as f:
        f.write(notebook_content)
    
    print(f"Generated gold notebook: {notebook_path}")

def generate_pipeline_group_notebook(pipeline_group, group_operations, output_dir, bundle_variables=None):
    """Generate a complete notebook for a pipeline group"""
    
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Separate bronze, silver, and gold operations
    bronze_operations = []
    silver_operations = []
    gold_operations = []
    
    for op_name, op_config in group_operations.items():
        if op_name.startswith("bronze_"):
            bronze_operations.append((op_name, op_config))
        elif op_name.startswith("silver_"):
            silver_operations.append((op_name, op_config))
        elif op_name.startswith("gold_"):
            gold_operations.append((op_name, op_config))
    
    # Generate only the combined notebook for the entire pipeline group
    combined_notebook_path = output_dir / f"unified_{pipeline_group}.py"
    generate_combined_notebook(pipeline_group, bronze_operations, silver_operations, gold_operations, str(combined_notebook_path), bundle_variables)

def generate_combined_notebook(pipeline_group, bronze_operations, silver_operations, gold_operations, notebook_path, bundle_variables=None):
    """Generate a combined notebook for all operations in a pipeline group"""
    
    # Debug output
    print(f"      [INFO] Generating notebook for {pipeline_group}")
    print(f"         Bronze operations: {len(bronze_operations)}")
    print(f"         Silver operations: {len(silver_operations)}")
    print(f"         Gold operations: {len(gold_operations)}")
    print(f"         Notebook path: {notebook_path} (type: {type(notebook_path)})")
    
    # Check if any bronze operation has a schema
    has_schema = any(op_config.get("schema") for _, op_config in bronze_operations)
    
    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - {pipeline_group.replace('_', ' ').title()}
# MAGIC 
# MAGIC This notebook implements a unified pipeline for {pipeline_group} with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC
# MAGIC 
# MAGIC Note: Gold operations are implemented as separate notebook tasks in the job.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *{'''
from pyspark.sql.types import *''' if has_schema else ''}

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader
'''
    
    # Add bronze tables
    for op_name, op_config in bronze_operations:
        source_path = op_config.get("source_path", "")
        target_table = op_config.get("target_table", "")
        file_format = op_config.get("file_format", "csv")
        table_name = target_table.split('.')[-1] if '.' in target_table else target_table
        
        # Check if inline schema is provided
        schema_definition = op_config.get("schema")
        schema_code = ""
        
        if schema_definition:
            # Validate schema definition
            errors = validate_schema_definition(schema_definition)
            if errors:
                print(f"Warning: Schema validation errors for {op_name}: {errors}")
                schema_code = ""
            else:
                # Convert JSON schema to PySpark schema code
                try:
                    # Try to use PySpark objects first, fallback to string representation
                    try:
                        spark_schema = convert_json_schema_to_spark(schema_definition)
                        schema_code = f'''
# Define fixed schema for data validation
schema = {spark_schema}'''
                    except ImportError:
                        # PySpark not available, use string representation
                        spark_schema_str = convert_json_schema_to_spark_string(schema_definition)
                        schema_code = f'''
# Define fixed schema for data validation
schema = {spark_schema_str}'''
                except Exception as e:
                    print(f"Warning: Error converting schema for {op_name}: {e}")
                    schema_code = ""
        
        # Build autoloader options from config - only include options that are specified
        autoloader_options = {}
        
        # Add all options from the config that start with cloudFiles. or are format-specific
        # Exclude schema and schema_location since we're handling schema differently now
        for key, value in op_config.items():
            if key.startswith("cloudFiles.") or key in ["header", "inferSchema", "multiline"]:
                # Skip schema-related options if we have inline schema
                if schema_definition and key in ["cloudFiles.schemaLocation", "inferSchema"]:
                    continue
                autoloader_options[key] = value
        
        # Generate individual .option() calls for each autoloader option
        option_lines = []
        for key, value in autoloader_options.items():
            option_lines.append(f'            .option("{key}", "{value}")')
        
        options_code = '\n'.join(option_lines) if option_lines else ''
        
        # Generate schema application code
        schema_application = ""
        if schema_definition:
            schema_application = '''
            .schema(schema)  # Apply fixed schema for data validation'''
        
        # Check if file metadata should be included
        include_file_metadata = op_config.get("include_file_metadata", False)
        # Convert string values to boolean
        if isinstance(include_file_metadata, str):
            include_file_metadata = include_file_metadata.lower() in ['true', '1', 'yes']
        
        # Handle custom expressions
        custom_expr = op_config.get("custom_expr", "")
        # Handle NaN values from pandas
        if pd.isna(custom_expr) or custom_expr == "nan" or custom_expr == "":
            custom_expr = ""
        
        if custom_expr:
            # Check if custom expression starts with * - if so, use selectExpr with * and remaining expression
            if custom_expr.strip().startswith('*'):
                # Use selectExpr() with * as separate argument and the remaining expression
                select_method = "selectExpr"
                # Remove * from the expression
                remaining_expr = custom_expr.strip()[1:].strip()
                if remaining_expr.startswith(','):
                    remaining_expr = remaining_expr[1:].strip()
                
                # Add file metadata if enabled
                if include_file_metadata:
                    # Check if _metadata is already in the expression to avoid conflicts
                    if '_metadata' not in remaining_expr:
                        remaining_expr = f'_metadata as source_metadata, {remaining_expr}'
                    else:
                        # If _metadata exists, rename it to source_metadata to avoid conflicts
                        remaining_expr = remaining_expr.replace('_metadata', 'source_metadata')
                
                # Split the remaining expression by commas, but be careful with function calls and string literals
                expressions = _parse_expressions_smart(remaining_expr)
                
                select_expr = '"*", ' + ', '.join([f'"{expr}"' for expr in expressions])
            else:
                # Use selectExpr() for expressions without *
                select_method = "selectExpr"
                
                # Add file metadata if enabled and not already present
                if include_file_metadata and '_metadata' not in custom_expr:
                    custom_expr = f'{custom_expr}, _metadata as source_metadata'
                elif include_file_metadata and '_metadata' in custom_expr:
                    # Replace _metadata with source_metadata to avoid conflicts
                    custom_expr = custom_expr.replace('_metadata', 'source_metadata')
                
                # Split the expression by commas and format as separate arguments
                expressions = _parse_expressions_smart(custom_expr)
                select_expr = ', '.join([f'"{expr}"' for expr in expressions])
        else:
            select_method = "selectExpr"
            if include_file_metadata:
                select_expr = '"*", "_metadata as source_metadata", "current_timestamp() as _ingestion_timestamp"'
            else:
                select_expr = '"*", "current_timestamp() as _ingestion_timestamp"'
        
        notebook_content += f'''

# COMMAND ----------{schema_code}

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
            .format("cloudFiles"){f'''
{options_code}''' if options_code else ''}
            .option("cloudFiles.format", "{file_format}"){schema_application}
            .load("{source_path}")
            .{select_method}({select_expr}))
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
        
        bronze_table_name = bronze_table.split('.')[-1] if '.' in bronze_table else bronze_table
        target_table_name = target_table.split('.')[-1] if '.' in target_table else target_table
        
        # Handle custom expressions for silver layer
        custom_expr = op_config.get("custom_expr", "")
        # Handle NaN values from pandas
        if pd.isna(custom_expr) or custom_expr == "nan" or custom_expr == "":
            custom_expr = ""
        
        if custom_expr:
            # Split the custom expression by commas, but be careful with function calls and string literals
            expressions = _parse_expressions_smart(custom_expr)
            
            select_expr_args = ', '.join([f'"{expr}"' for expr in expressions])
            source_view_code = f'''@dlt.view
def bronze_{target_table_name}_source():
    return spark.readStream.table("{bronze_table}").selectExpr({select_expr_args})'''
        else:
            source_view_code = f'''@dlt.view
def bronze_{target_table_name}_source():
    return spark.readStream.table("{bronze_table}")'''
        
        # Build DLT options from config - only include options that are specified
        dlt_options = {}
        
        # Add all DLT options from the config (excluding source_path, target_table, and custom_expr)
        for key, value in op_config.items():
            if key not in ["source_path", "target_table", "custom_expr"]:
                dlt_options[key] = value
        
        # If we have custom expressions, we need to update the DLT options to use mapped column names
        if custom_expr:
            # Parse the custom expression to extract column mappings using smart parsing
            column_mappings = {}
            expressions = _parse_expressions_smart(custom_expr)
            
            for expr in expressions:
                if ' as ' in expr:
                    # Extract original and mapped column names
                    original, mapped = expr.split(' as ', 1)
                    original = original.strip()
                    mapped = mapped.strip()
                    column_mappings[original] = mapped
            
            # Update track_history_except_column_list if it exists
            if 'track_history_except_column_list' in dlt_options:
                original_columns = dlt_options['track_history_except_column_list']
                if isinstance(original_columns, list):
                    # Map original column names to new names
                    mapped_columns = []
                    for col in original_columns:
                        if col in column_mappings:
                            mapped_columns.append(column_mappings[col])
                        else:
                            mapped_columns.append(col)
                    dlt_options['track_history_except_column_list'] = mapped_columns
        
        notebook_content += f'''

# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table("{target_table}")

# COMMAND ----------

{source_view_code}

# COMMAND ----------

# Create Auto CDC flow for SCD Type 2
dlt.create_auto_cdc_flow(
    target="{target_table}",
    source="bronze_{target_table_name}_source",
    **{json.dumps(dlt_options)}
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

def resolve_variables_in_config(df: pd.DataFrame, bundle_variables: dict = None) -> pd.DataFrame:
    """Resolve bundle variables in the configuration DataFrame."""
    print("Resolving bundle variables in configuration...")
    
    # Use provided bundle variables or fallback to defaults
    if bundle_variables:
        catalog_name = bundle_variables.get("catalog_name", "vbdemos")
        schema_name = bundle_variables.get("schema_name", "dbdemos_autoloader")
        volume_name = bundle_variables.get("volume_name", "raw_data")
    else:
        # Fallback to hardcoded values for standalone execution
        catalog_name = "vbdemos"
        schema_name = "dbdemos_autoloader"
        volume_name = "raw_data"
    
    print(f"  Using catalog: {catalog_name}")
    print(f"  Using schema: {schema_name}")
    print(f"  Using volume: {volume_name}")
    
    # Create a copy of the dataframe to avoid modifying the original
    df_resolved = df.copy()
    
    # Define variable mapping
    variable_map = {
        '${var.catalog_name}': catalog_name,
        '${var.schema_name}': schema_name,
        '${var.volume_name}': volume_name
    }
    
    # Replace variables in all string columns using simple string replacement
    for column in df_resolved.columns:
        if df_resolved[column].dtype == 'object':  # String columns
            for old, new in variable_map.items():
                df_resolved[column] = df_resolved[column].astype(str).str.replace(old, new)
    
    print("  Variable resolution completed")
    return df_resolved

def _parse_expressions_smart(custom_expr):
    """
    Smart expression parser that handles:
    - Function calls with parentheses
    - String literals with commas
    - Nested quotes and escapes
    - Complex SQL expressions
    """
    expressions = []
    current_expr = ""
    paren_count = 0
    in_string = False
    string_char = None
    i = 0
    
    while i < len(custom_expr):
        char = custom_expr[i]
        
        if not in_string:
            # Not inside a string literal
            if char in ['"', "'"]:
                # Start of string literal
                in_string = True
                string_char = char
                current_expr += char
            elif char == ',' and paren_count == 0:
                # Comma at top level - split expression
                expressions.append(current_expr.strip())
                current_expr = ""
            elif char == '(':
                paren_count += 1
                current_expr += char
            elif char == ')':
                paren_count -= 1
                current_expr += char
            else:
                current_expr += char
        else:
            # Inside a string literal
            if char == string_char:
                # Check for escaped quote
                if i + 1 < len(custom_expr) and custom_expr[i + 1] == string_char:
                    # Escaped quote - add both characters and skip next
                    current_expr += char + char
                    i += 1
                else:
                    # End of string literal
                    in_string = False
                    string_char = None
                    current_expr += char
            elif char == '\\' and i + 1 < len(custom_expr):
                # Escaped character - add both characters and skip next
                current_expr += char + custom_expr[i + 1]
                i += 1
            else:
                current_expr += char
        
        i += 1
    
    # Add the last expression if any
    if current_expr.strip():
        expressions.append(current_expr.strip())
    
    return expressions

def main():
    """Main function to generate notebooks for all pipeline groups"""
    
    # Read the unified configuration
    config_file = "config/unified_pipeline_config.tsv"
    if not os.path.exists(config_file):
        print(f"Configuration file not found: {config_file}")
        return
    
    # Load configuration
    df = pd.read_csv(config_file, sep='\t')
    
    # Get bundle variables from environment or use defaults
    bundle_variables = {
        "catalog_name": os.environ.get("CATALOG_NAME", "vbdemos"),
        "schema_name": os.environ.get("SCHEMA_NAME", "dbdemos_autoloader"),
        "volume_name": os.environ.get("VOLUME_NAME", "raw_data")
    }
    
    # Resolve variables in the configuration
    df = resolve_variables_in_config(df, bundle_variables)
    
    # Group by pipeline_group
    pipeline_groups = df.groupby('pipeline_group')
    
    # Create output directory
    output_dir = "src/notebooks/generated"
    
    # Generate notebooks for each pipeline group
    for group_name, group_df in pipeline_groups:
        print(f"\nGenerating notebooks for pipeline group: {group_name}")
        
        # Check if this is a manual-only group - skip DLT notebook generation
        operation_types = group_df['operation_type'].unique()
        if all(op_type == 'manual' for op_type in operation_types):
            print(f"  Skipping DLT notebook generation for manual-only group: {group_name}")
            continue
        
        # Convert group_df to operations_config format
        operations_config = {}
        for _, row in group_df.iterrows():
            operation_type = row['operation_type']
            if operation_type == 'bronze':
                op_name = f"bronze_{row['target_table'].split('.')[-1].replace('_new', '')}"
                
                # Parse the pipeline_config JSON for bronze operations
                try:
                    pipeline_config = json.loads(row['pipeline_config'])
                except (json.JSONDecodeError, TypeError):
                    # Fallback if JSON parsing fails
                    pipeline_config = {}
                
                # Start with basic config and add all autoloader options from pipeline_config
                operations_config[op_name] = {
                    'source_path': row['source_path'],
                    'target_table': row['target_table'],
                    'file_format': row['file_format']
                }
                
                # Add custom_expr if present
                if pd.notna(row.get('custom_expr', '')) and row['custom_expr']:
                    operations_config[op_name]['custom_expr'] = row['custom_expr']
                
                # Add include_file_metadata if present
                if pd.notna(row.get('include_file_metadata', '')) and row['include_file_metadata']:
                    operations_config[op_name]['include_file_metadata'] = row['include_file_metadata']
                
                # Add all autoloader options from the pipeline_config
                for key, value in pipeline_config.items():
                    if key.startswith("cloudFiles.") or key in ["header", "inferSchema", "multiline"] or key == "schema":
                        operations_config[op_name][key] = value
            elif operation_type == 'silver':
                op_name = f"silver_{row['target_table'].split('.')[-1].replace('_scd2', '')}"
                
                # Parse the pipeline_config JSON for silver operations
                try:
                    pipeline_config = json.loads(row['pipeline_config'])
                except (json.JSONDecodeError, TypeError):
                    # Fallback if JSON parsing fails
                    pipeline_config = {}
                
                # Start with basic config and add all DLT options from pipeline_config
                operations_config[op_name] = {
                    'source_path': row['source_path'],  # This should be the bronze table
                    'target_table': row['target_table']
                }
                
                # Add custom_expr if present
                if pd.notna(row.get('custom_expr', '')) and row['custom_expr']:
                    operations_config[op_name]['custom_expr'] = row['custom_expr']
                
                # Add all DLT options from the pipeline_config
                for key, value in pipeline_config.items():
                    operations_config[op_name][key] = value
            elif operation_type == 'gold':
                op_name = f"gold_{row['target_table'].split('.')[-1] if row['target_table'] else 'analytics'}"
                
                # For gold operations, source_path contains the notebook path
                notebook_path = row['source_path']
                
                # Parse the pipeline_config JSON for additional gold operation settings
                try:
                    pipeline_config = json.loads(row['pipeline_config'])
                    # Additional gold-specific configuration can be added here
                except (json.JSONDecodeError, TypeError):
                    # Fallback if JSON parsing fails
                    pass
                
                operations_config[op_name] = {
                    'source_path': notebook_path,  # This contains the notebook path
                    'target_table': row['target_table'],
                    'pipeline_config': row['pipeline_config']  # Keep the full config for gold operations
                }
        
        # Generate notebooks for this group
        generate_pipeline_group_notebook(group_name, operations_config, output_dir)
    
    print(f"\nAll notebooks generated in: {output_dir}")

if __name__ == "__main__":
    main()
