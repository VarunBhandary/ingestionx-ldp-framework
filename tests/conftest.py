#!/usr/bin/env python3
"""
Pytest configuration and shared fixtures for the autoloader framework test suite

This module provides common fixtures and configuration for all test modules.
"""

import pytest
import tempfile
import os
import pandas as pd
from unittest.mock import Mock


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup
    import shutil
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def mock_bundle():
    """Create a mock Databricks bundle for testing"""
    bundle = Mock()
    bundle.variables = {
        "catalog_name": "test_catalog",
        "schema_name": "test_schema",
        "volume_name": "test_volume"
    }
    return bundle


@pytest.fixture
def sample_config_data():
    """Sample configuration data for testing"""
    return [
        {
            'pipeline_type': 'bronze',
            'pipeline_group': 'test_pipeline',
            'source_type': 'file',
            'source_path': '/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/test',
            'target_table': '${var.catalog_name}.adls_bronze.test_table',
            'file_format': 'csv',
            'trigger_type': 'time',
            'schedule': '0 0 6 * * ?',
            'pipeline_config': '{"schema": {"type": "struct", "fields": [{"name": "id", "type": "string", "nullable": false}]}, "header": "true"}',
            'cluster_size': 'medium',
            'email_notifications': '{"on_success": true, "on_failure": true}'
        },
        {
            'pipeline_type': 'silver',
            'pipeline_group': 'test_pipeline',
            'source_type': '',
            'source_path': '',
            'target_table': '${var.catalog_name}.adls_silver.test_table',
            'file_format': '',
            'trigger_type': 'time',
            'schedule': '0 0 6 * * ?',
            'pipeline_config': '{"keys": ["id"], "track_history_except_column_list": ["name"], "stored_as_scd_type": "2"}',
            'cluster_size': 'medium',
            'email_notifications': '{"on_success": true, "on_failure": true}'
        }
    ]


@pytest.fixture
def sample_schema_json():
    """Sample schema JSON for testing"""
    return {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "age", "type": "integer", "nullable": True},
            {"name": "salary", "type": "double", "nullable": True},
            {"name": "is_active", "type": "boolean", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": True}
        ]
    }


@pytest.fixture
def sample_complex_schema_json():
    """Sample complex schema JSON with nested structures for testing"""
    return {
        "type": "struct",
        "fields": [
            {"name": "user_id", "type": "string", "nullable": False},
            {"name": "profile", "type": "struct", "nullable": True, "fields": [
                {"name": "personal", "type": "struct", "nullable": True, "fields": [
                    {"name": "first_name", "type": "string", "nullable": True},
                    {"name": "last_name", "type": "string", "nullable": True},
                    {"name": "date_of_birth", "type": "date", "nullable": True}
                ]},
                {"name": "contact", "type": "struct", "nullable": True, "fields": [
                    {"name": "email", "type": "string", "nullable": True},
                    {"name": "phone", "type": "string", "nullable": True}
                ]}
            ]},
            {"name": "orders", "type": "array", "nullable": True, "elementType": "struct", "fields": [
                {"name": "order_id", "type": "string", "nullable": False},
                {"name": "amount", "type": "double", "nullable": True},
                {"name": "items", "type": "array", "nullable": True, "elementType": "struct", "fields": [
                    {"name": "product_id", "type": "string", "nullable": False},
                    {"name": "quantity", "type": "integer", "nullable": True},
                    {"name": "price", "type": "double", "nullable": True}
                ]}
            ]}
        ]
    }


@pytest.fixture
def sample_expression_cases():
    """Sample expression cases for testing"""
    return [
        # Basic expressions
        ("col1, col2, col3", ["col1", "col2", "col3"]),
        ("col1 as alias1, col2 as alias2", ["col1 as alias1", "col2 as alias2"]),
        
        # Function calls
        ("COALESCE(col1, col2) as result", ["COALESCE(col1, col2) as result"]),
        ("CASE WHEN col1 IS NULL THEN col2 ELSE col3 END as result", 
         ["CASE WHEN col1 IS NULL THEN col2 ELSE col3 END as result"]),
        
        # String literals with commas (critical edge case)
        ("CASE WHEN col1 = \",\" THEN col2 ELSE col3 END as result",
         ["CASE WHEN col1 = \",\" THEN col2 ELSE col3 END as result"]),
        ("CONCAT(col1, \", \", col2) as result", 
         ["CONCAT(col1, \", \", col2) as result"]),
        
        # Nested functions
        ("COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4) as result",
         ["COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4) as result"]),
        
        # Mixed expressions
        ("col1, COALESCE(col2, col3) as result, current_timestamp() as ts",
         ["col1", "COALESCE(col2, col3) as result", "current_timestamp() as ts"])
    ]


@pytest.fixture
def sample_invalid_config_data():
    """Sample invalid configuration data for testing error handling"""
    return [
        {
            'pipeline_type': 'bronze',
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


@pytest.fixture
def sample_data_generation_config():
    """Sample data generation configuration for testing"""
    return {
        'catalog_name': 'test_catalog',
        'schema_name': 'test_schema',
        'volume_name': 'test_volume',
        'customer_count': 100,
        'product_count': 50,
        'transaction_count': 200,
        'inventory_count': 75,
        'shipment_count': 150
    }


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "expression_parser: mark test as expression parser test"
    )
    config.addinivalue_line(
        "markers", "config_parser: mark test as config parser test"
    )
    config.addinivalue_line(
        "markers", "schema_converter: mark test as schema converter test"
    )
    config.addinivalue_line(
        "markers", "pipeline_generator: mark test as pipeline generator test"
    )
    config.addinivalue_line(
        "markers", "notebook_generator: mark test as notebook generator test"
    )
    config.addinivalue_line(
        "markers", "data_generation: mark test as data generation test"
    )
