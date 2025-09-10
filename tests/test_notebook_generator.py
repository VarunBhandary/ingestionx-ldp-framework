#!/usr/bin/env python3
"""
Test suite for notebook generation functionality in resources/notebook_generator.py

This test suite validates the notebook generation functionality including:
- TSV configuration loading
- Variable resolution
- Notebook content generation
- Bronze layer generation
- Silver layer generation
- Gold layer generation
- Error handling
"""

import pytest
import pandas as pd
import tempfile
import os
import sys
from unittest.mock import patch, Mock

# Add the resources directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'resources'))

from notebook_generator import resolve_variables_in_config, generate_combined_notebook


class TestNotebookGenerator:
    """Test cases for notebook generation functionality"""
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_config_file = os.path.join(self.temp_dir, "test_config.tsv")
        
    def teardown_method(self):
        """Clean up after each test method"""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def create_test_config(self, data):
        """Helper method to create test TSV files"""
        df = pd.DataFrame(data)
        df.to_csv(self.test_config_file, sep='\t', index=False)
        return self.test_config_file
    
    def test_resolve_variables_in_config(self):
        """Test variable resolution in configuration"""
        test_data = [
            {
                'source_path': '/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/test',
                'target_table': '${var.catalog_name}.adls_bronze.test_table',
                'pipeline_config': '{"checkpointLocation": "/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/checkpoint"}'
            }
        ]
        
        df = pd.DataFrame(test_data)
        resolved_df = resolve_variables_in_config(df)
        
        # Check that variables were resolved
        assert '${var.catalog_name}' not in resolved_df.iloc[0]['source_path']
        assert 'vbdemos' in resolved_df.iloc[0]['source_path']
        assert 'dbdemos_autoloader' in resolved_df.iloc[0]['source_path']
        assert 'raw_data' in resolved_df.iloc[0]['source_path']
        
        assert '${var.catalog_name}' not in resolved_df.iloc[0]['target_table']
        assert 'vbdemos' in resolved_df.iloc[0]['target_table']
        
        assert '${var.catalog_name}' not in resolved_df.iloc[0]['pipeline_config']
        assert 'vbdemos' in resolved_df.iloc[0]['pipeline_config']
    
    def test_resolve_variables_in_all_columns(self):
        """Test that variables are resolved in all string columns"""
        test_data = [
            {
                'string_col1': '${var.catalog_name}_test',
                'string_col2': '${var.schema_name}_data',
                'string_col3': '${var.volume_name}_files',
                'numeric_col': 123,  # Should not be processed
                'boolean_col': True  # Should not be processed
            }
        ]
        
        df = pd.DataFrame(test_data)
        resolved_df = resolve_variables_in_config(df)
        
        # Check string columns were resolved
        assert resolved_df.iloc[0]['string_col1'] == 'vbdemos_test'
        assert resolved_df.iloc[0]['string_col2'] == 'dbdemos_autoloader_data'
        assert resolved_df.iloc[0]['string_col3'] == 'raw_data_files'
        
        # Check non-string columns were not processed
        assert resolved_df.iloc[0]['numeric_col'] == 123
        assert resolved_df.iloc[0]['boolean_col'] == True
    
    def test_generate_combined_notebook_bronze_only(self):
        """Test generation of notebook with only bronze operations"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': ''
            })
        ]
        
        silver_operations = []
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that bronze layer code is present
        assert 'def test_table():' in content
        assert 'vbdemos.adls_bronze.test_table' in content
        assert 'cloudFiles' in content
        assert 'csv' in content
    
    def test_generate_combined_notebook_silver_only(self):
        """Test generation of notebook with only silver operations"""
        bronze_operations = []
        silver_operations = [
            ('silver_test', {
                'target_table': 'vbdemos.adls_silver.test_table',
                'pipeline_config': '{"keys": ["id"], "track_history_except_column_list": ["name"], "stored_as_scd_type": "2"}',
                'custom_expr': 'id, name as full_name, email'
            })
        ]
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that silver layer code is present
        assert 'dlt.create_streaming_table("vbdemos.adls_silver.test_table")' in content
        assert 'dlt.create_auto_cdc_flow' in content
        assert 'bronze_test_table_source' in content
    
    def test_generate_combined_notebook_gold_only(self):
        """Test generation of notebook with only gold operations"""
        bronze_operations = []
        silver_operations = []
        gold_operations = [
            ('gold_test', {
                'target_table': 'vbdemos.adls_gold.test_table',
                'pipeline_config': '{"query": "SELECT * FROM vbdemos.adls_silver.test_table"}',
                'custom_expr': ''
            })
        ]
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that gold layer code is present (gold operations are implemented as separate notebook tasks)
        # For now, just check that the notebook was created successfully
        assert 'Pipeline execution completed' in content
    
    def test_generate_combined_notebook_all_layers(self):
        """Test generation of notebook with all layers"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': ''
            })
        ]
        
        silver_operations = [
            ('silver_test', {
                'target_table': 'vbdemos.adls_silver.test_table',
                'pipeline_config': '{"keys": ["id"], "track_history_except_column_list": ["name"], "stored_as_scd_type": "2"}',
                'custom_expr': 'id, name as full_name, email'
            })
        ]
        
        gold_operations = [
            ('gold_test', {
                'target_table': 'vbdemos.adls_gold.test_table',
                'pipeline_config': '{"query": "SELECT * FROM vbdemos.adls_silver.test_table"}',
                'custom_expr': ''
            })
        ]
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that all layers are present
        assert 'def test_table():' in content
        assert 'dlt.create_streaming_table("vbdemos.adls_silver.test_table")' in content
        # Gold operations are implemented as separate notebook tasks
        assert 'Pipeline execution completed' in content
    
    def test_generate_combined_notebook_with_custom_expressions(self):
        """Test generation of notebook with custom expressions"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': '*, CASE WHEN deleted_at IS NOT NULL THEN \'DELETE\' WHEN created_at = updated_at THEN \'INSERT\' ELSE \'UPDATE\' END as cdc_operation, COALESCE(updated_at, created_at) as sequence_ts, current_timestamp() as _ingestion_timestamp'
            })
        ]
        
        silver_operations = [
            ('silver_test', {
                'target_table': 'vbdemos.adls_silver.test_table',
                'pipeline_config': '{"keys": ["id"], "track_history_except_column_list": ["name"], "stored_as_scd_type": "2"}',
                'custom_expr': 'id, name as full_name, email, cdc_operation, sequence_ts'
            })
        ]
        
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that custom expressions are properly formatted
        assert 'selectExpr(' in content
        assert 'CASE WHEN deleted_at IS NOT NULL' in content
        assert 'COALESCE(updated_at, created_at)' in content
        assert 'current_timestamp() as _ingestion_timestamp' in content
        assert 'name as full_name' in content
    
    def test_generate_combined_notebook_empty_custom_expr(self):
        """Test generation of notebook with empty custom expressions"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': ''  # Empty custom expression
            })
        ]
        
        silver_operations = []
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that default selectExpr is used
        assert 'selectExpr("*", "current_timestamp() as _ingestion_timestamp")' in content
    
    def test_generate_combined_notebook_nan_custom_expr(self):
        """Test generation of notebook with NaN custom expressions"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': float('nan')  # NaN custom expression
            })
        ]
        
        silver_operations = []
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that default selectExpr is used (no "nan" in content)
        assert 'selectExpr("*", "current_timestamp() as _ingestion_timestamp")' in content
        assert 'selectExpr("nan")' not in content
    
    def test_generate_combined_notebook_string_nan_custom_expr(self):
        """Test generation of notebook with string 'nan' custom expressions"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': 'nan'  # String 'nan' custom expression
            })
        ]
        
        silver_operations = []
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that default selectExpr is used (no "nan" in content)
        assert 'selectExpr("*", "current_timestamp() as _ingestion_timestamp")' in content
        assert 'selectExpr("nan")' not in content
    
    def test_generate_combined_notebook_complex_custom_expressions(self):
        """Test generation of notebook with complex custom expressions"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': '*, CASE WHEN col1 = "," THEN col2 ELSE col3 END as result, CONCAT(col1, ", ", col2) as full_name'
            })
        ]
        
        silver_operations = []
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that complex expressions are properly formatted
        assert 'selectExpr(' in content
        assert 'CASE WHEN col1 = "," THEN col2 ELSE col3 END as result' in content
        assert 'CONCAT(col1, ", ", col2) as full_name' in content
    
    def test_generate_combined_notebook_silver_with_column_mapping(self):
        """Test generation of notebook with silver layer column mapping"""
        bronze_operations = []
        silver_operations = [
            ('silver_test', {
                'target_table': 'vbdemos.adls_silver.test_table',
                'pipeline_config': '{"keys": ["id"], "track_history_except_column_list": ["name", "email"], "stored_as_scd_type": "2"}',
                'custom_expr': 'id, name as full_name, email as email_address'
            })
        ]
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check that column mapping is applied to DLT flow
        # The actual implementation uses the original column names in the DLT flow
        assert 'track_history_except_column_list\\": [\\"name\\", \\"email\\"]' in content
        assert 'name as full_name' in content
        assert 'email as email_address' in content
    
    def test_generate_combined_notebook_file_creation(self):
        """Test that notebook file is created with proper structure"""
        bronze_operations = [
            ('bronze_test', {
                'source_path': '/Volumes/vbdemos/dbdemos_autoloader/raw_data/test',
                'target_table': 'vbdemos.adls_bronze.test_table',
                'file_format': 'csv',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
                'custom_expr': ''
            })
        ]
        
        silver_operations = []
        gold_operations = []
        
        notebook_path = os.path.join(self.temp_dir, "test_notebook.py")
        
        generate_combined_notebook(
            'test_pipeline',
            bronze_operations,
            silver_operations,
            gold_operations,
            notebook_path
        )
        
        # Verify notebook was created
        assert os.path.exists(notebook_path)
        
        # Read and verify content structure
        with open(notebook_path, 'r') as f:
            content = f.read()
        
        # Check for required imports and structure
        assert 'import dlt' in content
        assert 'from pyspark.sql.functions import *' in content
        assert '# COMMAND ----------' in content
        assert 'def test_table():' in content
        assert 'return (' in content
        assert 'spark.readStream' in content
        assert '.format("cloudFiles")' in content


if __name__ == "__main__":
    # Run the tests if this file is executed directly
    pytest.main([__file__, "-v"])
