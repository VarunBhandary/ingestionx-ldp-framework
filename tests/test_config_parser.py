#!/usr/bin/env python3
"""
Test suite for ConfigParser utility in src/utils/config_parser.py

This test suite validates the configuration parsing functionality including:
- TSV file loading
- Configuration validation
- Error handling
- Data type conversions
- Required field validation
"""

import pytest
import pandas as pd
import json
import tempfile
import os
from pathlib import Path
import sys

# Add the src directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.config_parser import ConfigParser


class TestConfigParser:
    """Test cases for ConfigParser functionality"""
    
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
    
    def create_valid_test_data(self):
        """Helper method to create valid test data"""
        return [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
    
    def test_load_config_success(self):
        """Test successful configuration loading"""
        test_data = self.create_valid_test_data()
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        
        assert len(df) == 1
        assert df.iloc[0]['operation_type'] == 'bronze'
        assert df.iloc[0]['pipeline_group'] == 'test_pipeline'
    
    def test_load_config_file_not_found(self):
        """Test error handling when config file doesn't exist"""
        parser = ConfigParser("nonexistent_file.tsv")
        
        with pytest.raises(FileNotFoundError):
            parser.load_config()
    
    def test_validate_config_success(self):
        """Test successful configuration validation"""
        test_data = self.create_valid_test_data()
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) == 0
    
    def test_validate_config_missing_columns(self):
        """Test validation with missing required columns"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                # Missing required columns
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Missing required columns" in error for error in errors)
    
    def test_validate_config_invalid_operation_types(self):
        """Test validation with invalid operation types"""
        test_data = [
            {
                'operation_type': 'invalid_operation',  # Invalid - should be 'bronze', 'silver', 'gold', or 'manual'
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid operation types" in error for error in errors)
    
    def test_validate_config_invalid_source_types(self):
        """Test validation with invalid source types"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'invalid_source',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid source types" in error for error in errors)
    
    def test_validate_config_invalid_file_formats(self):
        """Test validation with invalid file formats"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'invalid_format',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid file formats" in error for error in errors)
    
    def test_validate_config_invalid_trigger_types(self):
        """Test validation with invalid trigger types"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'invalid_trigger',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid trigger types" in error for error in errors)
    
    def test_validate_config_invalid_cluster_sizes(self):
        """Test validation with invalid cluster sizes"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'invalid_size',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid cluster sizes:" in error for error in errors)
    
    def test_validate_config_invalid_json(self):
        """Test validation with invalid JSON in pipeline_config"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true",}',  # Invalid JSON
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid JSON" in error for error in errors)
    
    def test_validate_config_missing_notification_recipients(self):
        """Test validation with missing notification recipients"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true}',  # Missing recipients
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("notifications must contain 'recipients' field" in error for error in errors)
    
    def test_validate_config_invalid_email_format(self):
        """Test validation with invalid email format"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["invalid-email"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("Invalid email format in recipients" in error for error in errors)
    
    def test_validate_config_invalid_source_paths(self):
        """Test validation with invalid source paths"""
        test_cases = [
            ('file', 'invalid/path', 'Invalid file path format'),
            ('table', 'invalid_table', 'Invalid table reference format'),
            ('notebook', 'invalid/notebook.py', 'Invalid notebook path format')
        ]
        
        for source_type, invalid_path, expected_error in test_cases:
            test_data = [
                {
                    'operation_type': 'bronze',
                    'pipeline_group': 'test_pipeline',
                    'source_type': source_type,
                    'source_path': invalid_path,
                    'target_table': 'test.bronze.table',
                    'file_format': 'csv',
                    'trigger_type': 'time',
                    'schedule': '0 0 6 * * ?',
                    'pipeline_config': '{"header": "true"}',
                    'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                    'custom_expr': ''
                }
            ]
            
            self.create_test_config(test_data)
            parser = ConfigParser(self.test_config_file)
            df = parser.load_config()
            errors = parser.validate_config(df)
            
            assert len(errors) > 0
            assert any(expected_error in error for error in errors)
    
    def test_validate_config_serverless_cluster_validation(self):
        """Test validation for serverless cluster configuration"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'serverless',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 0
        assert any("serverless cluster requires warehouse_id" in error for error in errors)
    
    def test_validate_config_valid_serverless_cluster(self):
        """Test validation for valid serverless cluster configuration"""
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'csv',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'serverless',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        # This should still have errors because we need cluster_config with warehouse_id
        assert len(errors) > 0
    
    def test_validate_config_valid_operation_types(self):
        """Test validation with valid operation types"""
        valid_types = ['bronze', 'silver', 'gold', 'manual']
        
        for operation_type in valid_types:
            test_data = [
                {
                    'operation_type': operation_type,
                    'pipeline_group': f'test_pipeline_{operation_type}',
                    'source_type': 'file',
                    'source_path': '/Volumes/test/raw_data',
                    'target_table': 'test.bronze.table',
                    'file_format': 'csv',
                    'trigger_type': 'time',
                    'schedule': '0 0 6 * * ?',
                    'pipeline_config': '{"header": "true"}',
                    'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                    'custom_expr': ''
                }
            ]
            
            self.create_test_config(test_data)
            parser = ConfigParser(self.test_config_file)
            df = parser.load_config()
            errors = parser.validate_config(df)
            
            assert len(errors) == 0, f"Operation type '{operation_type}' should be valid but got errors: {errors}"
    
    def test_validate_config_valid_source_types(self):
        """Test validation with valid source types"""
        valid_types = ['file', 'table', 'notebook']
        
        for source_type in valid_types:
            # Set appropriate path format for each source type
            if source_type == 'file':
                source_path = '/Volumes/test/raw_data'
            elif source_type == 'table':
                source_path = 'test.catalog.table'
            else:  # notebook
                source_path = 'src/notebooks/test.py'
            
            test_data = [
                {
                    'operation_type': 'bronze',
                    'pipeline_group': f'test_pipeline_{source_type}',
                    'source_type': source_type,
                    'source_path': source_path,
                    'target_table': 'test.bronze.table',
                    'file_format': 'csv',
                    'trigger_type': 'time',
                    'schedule': '0 0 6 * * ?',
                    'pipeline_config': '{"header": "true"}',
                    'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                    'custom_expr': ''
                }
            ]
            
            self.create_test_config(test_data)
            parser = ConfigParser(self.test_config_file)
            df = parser.load_config()
            errors = parser.validate_config(df)
            
            assert len(errors) == 0, f"Source type '{source_type}' should be valid but got errors: {errors}"
    
    def test_validate_config_valid_file_formats(self):
        """Test validation with valid file formats"""
        valid_formats = ['json', 'parquet', 'csv', 'avro', 'orc']
        
        for file_format in valid_formats:
            test_data = [
                {
                    'operation_type': 'bronze',
                    'pipeline_group': f'test_pipeline_{file_format}',
                    'source_type': 'file',
                    'source_path': '/Volumes/test/raw_data',
                    'target_table': 'test.bronze.table',
                    'file_format': file_format,
                    'trigger_type': 'time',
                    'schedule': '0 0 6 * * ?',
                    'pipeline_config': '{"header": "true"}',
                    'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                    'custom_expr': ''
                }
            ]
            
            self.create_test_config(test_data)
            parser = ConfigParser(self.test_config_file)
            df = parser.load_config()
            errors = parser.validate_config(df)
            
            assert len(errors) == 0, f"File format '{file_format}' should be valid but got errors: {errors}"
    
    def test_validate_config_valid_trigger_types(self):
        """Test validation with valid trigger types"""
        valid_triggers = ['file', 'time']
        
        for trigger_type in valid_triggers:
            test_data = [
                {
                    'operation_type': 'bronze',
                    'pipeline_group': f'test_pipeline_{trigger_type}',
                    'source_type': 'file',
                    'source_path': '/Volumes/test/raw_data',
                    'target_table': 'test.bronze.table',
                    'file_format': 'csv',
                    'trigger_type': trigger_type,
                    'schedule': '0 0 6 * * ?',
                    'pipeline_config': '{"header": "true"}',
                    'cluster_size': 'medium',
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                    'custom_expr': ''
                }
            ]
            
            self.create_test_config(test_data)
            parser = ConfigParser(self.test_config_file)
            df = parser.load_config()
            errors = parser.validate_config(df)
            
            assert len(errors) == 0, f"Trigger type '{trigger_type}' should be valid but got errors: {errors}"
    
    def test_validate_config_valid_cluster_sizes(self):
        """Test validation with valid cluster sizes"""
        valid_sizes = ['small', 'medium', 'large', 'serverless']
        
        for size in valid_sizes:
            # Provide cluster_config for serverless clusters
            cluster_config = '{"warehouse_id": "valid_warehouse_id"}' if size == 'serverless' else '{}'
            
            test_data = [
                {
                    'operation_type': 'bronze',
                    'pipeline_group': f'test_pipeline_{size}',
                    'source_type': 'file',
                    'source_path': '/Volumes/test/raw_data',
                    'target_table': 'test.bronze.table',
                    'file_format': 'csv',
                    'trigger_type': 'time',
                    'schedule': '0 0 6 * * ?',
                    'pipeline_config': '{"header": "true"}',
                    'cluster_size': size,
                    'cluster_config': cluster_config,
                    'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                    'custom_expr': ''
                }
            ]
            
            self.create_test_config(test_data)
            parser = ConfigParser(self.test_config_file)
            df = parser.load_config()
            errors = parser.validate_config(df)
            
            assert len(errors) == 0, f"Cluster size '{size}' should be valid but got errors: {errors}"
    
    def test_validate_config_multiple_errors(self):
        """Test validation with multiple errors"""
        test_data = [
            {
                'operation_type': 'invalid_operation',  # Invalid operation type
                'pipeline_group': 'test_pipeline',
                'source_type': 'invalid_source',  # Invalid source type
                'source_path': '/Volumes/test/raw_data',
                'target_table': 'test.bronze.table',
                'file_format': 'invalid_format',  # Invalid file format
                'trigger_type': 'invalid_trigger',  # Invalid trigger type
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true",}',  # Invalid JSON
                'cluster_size': 'invalid_size',  # Invalid cluster size
                'cluster_config': '{}',
                'notifications': '{"on_success": true, "on_failure": true}',  # Missing recipients
                'custom_expr': ''
            }
        ]
        
        self.create_test_config(test_data)
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        assert len(errors) > 1
        assert any("Invalid operation types" in error for error in errors)
        assert any("Invalid source types" in error for error in errors)
        assert any("Invalid file formats" in error for error in errors)
        assert any("Invalid trigger types" in error for error in errors)
        assert any("Invalid cluster sizes:" in error for error in errors)
        assert any("Invalid JSON" in error for error in errors)
        assert any("notifications must contain 'recipients' field" in error for error in errors)
    
    def test_validate_config_empty_dataframe(self):
        """Test validation with empty dataframe"""
        # Create an empty TSV file
        with open(self.test_config_file, 'w') as f:
            f.write('')
        
        parser = ConfigParser(self.test_config_file)
        
        # This should raise an error when trying to load an empty file
        with pytest.raises(pd.errors.EmptyDataError):
            parser.load_config()


if __name__ == "__main__":
    # Run the tests if this file is executed directly
    pytest.main([__file__, "-v"])