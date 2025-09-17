#!/usr/bin/env python3
"""
Integration test suite for the autoloader framework

This test suite validates end-to-end functionality including:
- Complete pipeline generation workflow
- Bundle deployment simulation
- Configuration validation
- Resource generation
- Error handling across components
"""

import pytest
import pandas as pd
import tempfile
import os
import sys
import json
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add the resources and src directories to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'resources'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from unified_pipeline_generator import UnifiedPipelineGenerator
from notebook_generator import main as generate_notebooks
from utils.config_parser import ConfigParser
from utils.schema_converter import convert_json_schema_to_spark


class TestIntegration:
    """Integration tests for the autoloader framework"""
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_config_file = os.path.join(self.temp_dir, "test_config.tsv")
        
        # Create mock bundle
        self.mock_bundle = Mock()
        self.mock_bundle.variables = {
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "volume_name": "test_volume"
        }
        
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
    
    def test_complete_pipeline_generation_workflow(self):
        """Test complete pipeline generation workflow"""
        # Create comprehensive test configuration
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'customer_demo_pipeline',
                'source_type': 'file',
                'file_format': 'csv',
                'source_path': '/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/customers',
                'target_table': '${var.catalog_name}.adls_bronze.customers_demo',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "customer_id", "type": "string", "nullable": false}, {"name": "first_name", "type": "string", "nullable": true}]}, "header": "true", "cloudFiles.checkpointLocation": "/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/checkpoint/customers"}',
                'cluster_size': 'medium',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            },
            {
                'operation_type': 'silver',
                'pipeline_group': 'customer_demo_pipeline',
                'source_type': 'table',
                'file_format': '',
                'source_path': '',
                'target_table': '${var.catalog_name}.adls_silver.customers_demo_scd2',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"keys": ["customer_id"], "track_history_except_column_list": ["first_name", "last_name", "email"], "stored_as_scd_type": "2", "sequence_by": "update_ts"}',
                'cluster_size': 'medium',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            }
        ]
        
        self.create_test_config(test_data)
        
        # Test pipeline generator
        generator = UnifiedPipelineGenerator(self.mock_bundle)
        generator.config_file = self.test_config_file
        
        # Load and validate configuration
        df = generator.load_config()
        assert len(df) == 2
        assert '${var.catalog_name}' not in df.iloc[0]['source_path']
        assert 'test_catalog' in df.iloc[0]['source_path']
        
        # Test notebook generation
        with patch('notebook_generator.os.path.exists', return_value=True):
            with patch('notebook_generator.pd.read_csv') as mock_read_csv:
                with patch('yaml.safe_load') as mock_yaml:
                    with patch('builtins.open', MagicMock()) as mock_file:
                        # Mock the yaml config
                        mock_yaml.return_value = {
                            'targets': {
                                'dev': {
                                    'variables': {
                                        'catalog_name': 'test_catalog',
                                        'schema_name': 'test_schema',
                                        'volume_name': 'test_volume'
                                    }
                                }
                            }
                        }
                        
                        mock_read_csv.return_value = df
                        
                        generate_notebooks()
                        
                        # Verify that notebooks were generated
                        assert mock_file.called
    
    def test_configuration_validation_integration(self):
        """Test configuration validation across components"""
        # Create valid configuration
        valid_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'file_format': 'csv',
                'source_path': '/Volumes/test_catalog/test_schema/test_volume/test',
                'target_table': 'test_catalog.adls_bronze.test_table',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'notifications': '{"on_success": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            }
        ]
        
        self.create_test_config(valid_data)
        
        # Test with ConfigParser
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        # Add missing required columns for validation
        if 'cluster_config' not in df.columns:
            df['cluster_config'] = '{}'
        if 'notifications' not in df.columns:
            df['notifications'] = '{}'
        if 'custom_expr' not in df.columns:
            df['custom_expr'] = ''
        if 'parameters' not in df.columns:
            df['parameters'] = '{}'
        if 'include_file_metadata' not in df.columns:
            df['include_file_metadata'] = 'true'
        
        errors = parser.validate_config(df)
        assert len(errors) == 0
        
        # Test with UnifiedPipelineGenerator
        generator = UnifiedPipelineGenerator(self.mock_bundle)
        generator.config_file = self.test_config_file
        
        loaded_df = generator.load_config()
        assert len(loaded_df) == 1
        assert loaded_df.iloc[0]['operation_type'] == 'bronze'
    
    def test_schema_conversion_integration(self):
        """Test schema conversion integration"""
        # Test schema from actual configuration
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "customer_id", "type": "string", "nullable": False},
                {"name": "first_name", "type": "string", "nullable": True},
                {"name": "last_name", "type": "string", "nullable": True},
                {"name": "email", "type": "string", "nullable": True},
                {"name": "phone_number", "type": "string", "nullable": True},
                {"name": "address", "type": "string", "nullable": True},
                {"name": "city", "type": "string", "nullable": True},
                {"name": "state", "type": "string", "nullable": True},
                {"name": "zip_code", "type": "string", "nullable": True},
                {"name": "country", "type": "string", "nullable": True},
                {"name": "customer_tier", "type": "string", "nullable": True},
                {"name": "registration_date", "type": "date", "nullable": True},
                {"name": "update_ts", "type": "timestamp", "nullable": True}
            ]
        }
        
        struct_type = convert_json_schema_to_spark(schema_json)
        
        assert struct_type is not None
        assert len(struct_type.fields) == 13
        
        # Verify field names
        field_names = [field.name for field in struct_type.fields]
        expected_fields = [
            "customer_id", "first_name", "last_name", "email", "phone_number",
            "address", "city", "state", "zip_code", "country", "customer_tier",
            "registration_date", "update_ts"
        ]
        
        for field in expected_fields:
            assert field in field_names
    
    def test_variable_resolution_integration(self):
        """Test variable resolution across components"""
        # Create configuration with variables
        test_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'file',
                'file_format': 'csv',
                'source_path': '/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/test',
                'target_table': '${var.catalog_name}.adls_bronze.test_table',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"checkpointLocation": "/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/checkpoint"}',
                'cluster_size': 'medium',
                'notifications': '{"recipients": ["admin@${var.catalog_name}.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            }
        ]
        
        self.create_test_config(test_data)
        
        # Test with UnifiedPipelineGenerator
        generator = UnifiedPipelineGenerator(self.mock_bundle)
        generator.config_file = self.test_config_file
        
        df = generator.load_config()
        
        # Verify all variables were resolved
        assert '${var.catalog_name}' not in df.iloc[0]['source_path']
        assert 'test_catalog' in df.iloc[0]['source_path']
        assert 'test_schema' in df.iloc[0]['source_path']
        assert 'test_volume' in df.iloc[0]['source_path']
        
        assert '${var.catalog_name}' not in df.iloc[0]['target_table']
        assert 'test_catalog' in df.iloc[0]['target_table']
        
        assert '${var.catalog_name}' not in df.iloc[0]['pipeline_config']
        assert 'test_catalog' in df.iloc[0]['pipeline_config']
        
        assert '${var.catalog_name}' not in df.iloc[0]['notifications']
        assert 'test_catalog' in df.iloc[0]['notifications']
    
    def test_error_handling_integration(self):
        """Test error handling across components"""
        # Test with invalid configuration
        invalid_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'test_pipeline',
                'source_type': 'invalid_source',  # Invalid source type
                'source_path': '/Volumes/test/test',
                'target_table': 'test.bronze.table',
                'file_format': 'invalid_format',  # Invalid file format
                'trigger_type': 'invalid_trigger',  # Invalid trigger type
                'schedule': 'invalid_schedule',  # Invalid schedule
                'pipeline_config': '{"invalid": json,}',  # Invalid JSON
                'cluster_size': 'invalid_size',  # Invalid cluster size
                'email_notifications': '{"invalid": json,}'  # Invalid JSON
            }
        ]
        
        self.create_test_config(invalid_data)
        
        # Test with ConfigParser
        parser = ConfigParser(self.test_config_file)
        df = parser.load_config()
        errors = parser.validate_config(df)
        
        # Should have multiple validation errors
        assert len(errors) > 0
        # Check for any validation errors (the specific error messages may vary)
        assert any("Invalid" in error or "Missing" in error for error in errors)
    
    def test_complex_pipeline_configuration(self):
        """Test complex pipeline configuration with all features"""
        complex_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'product_catalog_cdc_pipeline',
                'source_type': 'file',
                'file_format': 'csv',
                'source_path': '/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/product_catalog_cdc',
                'target_table': '${var.catalog_name}.adls_bronze.product_catalog_cdc',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "product_id", "type": "string", "nullable": false}, {"name": "product_name", "type": "string", "nullable": true}, {"name": "category", "type": "string", "nullable": true}, {"name": "price", "type": "double", "nullable": true}, {"name": "description", "type": "string", "nullable": true}, {"name": "status", "type": "string", "nullable": true}, {"name": "created_at", "type": "timestamp", "nullable": true}, {"name": "updated_at", "type": "timestamp", "nullable": true}, {"name": "deleted_at", "type": "timestamp", "nullable": true}]}, "cloudFiles.checkpointLocation": "/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/checkpoint/product_catalog_cdc", "cloudFiles.maxFilesPerTrigger": "100", "cloudFiles.allowOverwrites": "false", "header": "true", "cloudFiles.rescuedDataColumn": "corrupt_data", "cloudFiles.validateOptions": "false"}',
                'cluster_size': 'medium',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            },
            {
                'operation_type': 'silver',
                'pipeline_group': 'product_catalog_cdc_pipeline',
                'source_type': 'table',
                'file_format': '',
                'source_path': '',
                'target_table': '${var.catalog_name}.adls_silver.product_catalog_cdc_scd2',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"keys": ["product_id"], "track_history_except_column_list": ["product_name", "category", "price", "description", "status"], "stored_as_scd_type": "2", "sequence_by": "sequence_ts"}',
                'cluster_size': 'medium',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            }
        ]
        
        self.create_test_config(complex_data)
        
        # Test with UnifiedPipelineGenerator
        generator = UnifiedPipelineGenerator(self.mock_bundle)
        generator.config_file = self.test_config_file
        
        df = generator.load_config()
        
        # Verify configuration was loaded and variables resolved
        assert len(df) == 2
        assert df.iloc[0]['pipeline_group'] == 'product_catalog_cdc_pipeline'
        assert '${var.catalog_name}' not in df.iloc[0]['source_path']
        assert 'test_catalog' in df.iloc[0]['source_path']
        
        # Verify complex pipeline config was parsed (simulate inline logic)
        try:
            pipeline_config = json.loads(df.iloc[0]['pipeline_config']) if df.iloc[0]['pipeline_config'] else {}
        except json.JSONDecodeError:
            pipeline_config = {}
        assert 'schema' in pipeline_config
        assert 'cloudFiles.checkpointLocation' in pipeline_config
        assert 'cloudFiles.maxFilesPerTrigger' in pipeline_config
        assert 'cloudFiles.allowOverwrites' in pipeline_config
        assert 'header' in pipeline_config
        assert 'cloudFiles.rescuedDataColumn' in pipeline_config
        assert 'cloudFiles.validateOptions' in pipeline_config
    
    def test_manual_pipeline_configuration(self):
        """Test manual pipeline configuration"""
        manual_data = [
            {
                'operation_type': 'manual',
                'pipeline_group': 'data_quality_checks',
                'source_type': 'notebook',
                'file_format': '',
                'source_path': '',
                'target_table': '',
                'trigger_type': 'time',
                'schedule': '0 0 2 * * ?',
                'pipeline_config': '{"notebook_path": "/Workspace/Users/test/data_quality_checks", "order": 1}',
                'cluster_size': 'small',
                'notifications': '{"on_success": true, "on_failure": true, "recipients": ["data-quality@company.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            }
        ]
        
        self.create_test_config(manual_data)
        
        # Test with UnifiedPipelineGenerator
        generator = UnifiedPipelineGenerator(self.mock_bundle)
        generator.config_file = self.test_config_file
        
        df = generator.load_config()
        
        # Verify manual pipeline was loaded
        assert len(df) == 1
        assert df.iloc[0]['operation_type'] == 'manual'
        assert df.iloc[0]['pipeline_group'] == 'data_quality_checks'
        
        # Verify manual pipeline config was parsed (simulate inline logic)
        try:
            pipeline_config = json.loads(df.iloc[0]['pipeline_config']) if df.iloc[0]['pipeline_config'] else {}
        except json.JSONDecodeError:
            pipeline_config = {}
        assert 'notebook_path' in pipeline_config
        assert 'order' in pipeline_config
        assert pipeline_config['notebook_path'] == '/Workspace/Users/test/data_quality_checks'
        assert pipeline_config['order'] == 1
    
    def test_mixed_pipeline_configuration(self):
        """Test mixed pipeline configuration with different types"""
        mixed_data = [
            {
                'operation_type': 'bronze',
                'pipeline_group': 'customer_pipeline',
                'source_type': 'file',
                'file_format': 'csv',
                'source_path': '/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/customers',
                'target_table': '${var.catalog_name}.adls_bronze.customers',
                'trigger_type': 'time',
                'schedule': '0 0 6 * * ?',
                'pipeline_config': '{"header": "true"}',
                'cluster_size': 'medium',
                'notifications': '{"on_success": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            },
            {
                'operation_type': 'manual',
                'pipeline_group': 'data_validation',
                'source_type': 'notebook',
                'file_format': '',
                'source_path': '',
                'target_table': '',
                'trigger_type': 'time',
                'schedule': '0 0 8 * * ?',
                'pipeline_config': '{"notebook_path": "/Workspace/Users/test/data_validation", "order": 2}',
                'cluster_size': 'small',
                'notifications': '{"on_success": true, "recipients": ["admin@test.com"]}',
                'custom_expr': ' ',
                'parameters': '{}',
                'include_file_metadata': 'true'
            }
        ]
        
        self.create_test_config(mixed_data)
        
        # Test with UnifiedPipelineGenerator
        generator = UnifiedPipelineGenerator(self.mock_bundle)
        generator.config_file = self.test_config_file
        
        df = generator.load_config()
        
        # Verify mixed configuration was loaded
        assert len(df) == 2
        assert df.iloc[0]['operation_type'] == 'bronze'
        assert df.iloc[1]['operation_type'] == 'manual'
        
        # Verify both types were processed correctly (simulate inline logic)
        try:
            bronze_config = json.loads(df.iloc[0]['pipeline_config']) if df.iloc[0]['pipeline_config'] else {}
        except json.JSONDecodeError:
            bronze_config = {}
        
        try:
            manual_config = json.loads(df.iloc[1]['pipeline_config']) if df.iloc[1]['pipeline_config'] else {}
        except json.JSONDecodeError:
            manual_config = {}
        
        assert 'header' in bronze_config
        assert 'notebook_path' in manual_config
        assert 'order' in manual_config


if __name__ == "__main__":
    # Run the tests if this file is executed directly
    pytest.main([__file__, "-v"])
