#!/usr/bin/env python3
"""
Test suite for SchemaConverter utility in src/utils/schema_converter.py

This test suite validates the schema conversion functionality including:
- JSON schema to PySpark StructType conversion
- Data type mapping
- Nested structure handling
- Error handling for invalid schemas
- Edge cases and complex schemas
"""

import pytest
import json
import sys
import os

# Add the src directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.schema_converter import convert_json_schema_to_spark
from pyspark.sql.types import StringType


class TestSchemaConverter:
    """Test cases for SchemaConverter functionality"""
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        pass
    
    def test_convert_simple_schema(self):
        """Test conversion of simple schema with basic data types"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "age", "type": "integer", "nullable": True},
                {"name": "salary", "type": "double", "nullable": True},
                {"name": "is_active", "type": "boolean", "nullable": True}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 5
        
        # Check field names and types
        field_names = [field.name for field in result.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "age" in field_names
        assert "salary" in field_names
        assert "is_active" in field_names
    
    def test_convert_nested_schema(self):
        """Test conversion of nested schema with struct and array types"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "address", "type": "struct", "nullable": True, "fields": [
                    {"name": "street", "type": "string", "nullable": True},
                    {"name": "city", "type": "string", "nullable": True},
                    {"name": "zipcode", "type": "string", "nullable": True}
                ]},
                {"name": "hobbies", "type": "array", "nullable": True, "elementType": "string"},
                {"name": "scores", "type": "array", "nullable": True, "elementType": "double"}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 4
        
        # Check that nested structures are handled
        field_names = [field.name for field in result.fields]
        assert "id" in field_names
        assert "address" in field_names
        assert "hobbies" in field_names
        assert "scores" in field_names
    
    def test_convert_complex_nested_schema(self):
        """Test conversion of complex nested schema with multiple levels"""
        schema_json = {
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
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 3
        
        # Check that deeply nested structures are handled
        field_names = [field.name for field in result.fields]
        assert "user_id" in field_names
        assert "profile" in field_names
        assert "orders" in field_names
    
    def test_convert_all_data_types(self):
        """Test conversion of schema with all supported data types"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "string_field", "type": "string", "nullable": True},
                {"name": "integer_field", "type": "integer", "nullable": True},
                {"name": "long_field", "type": "long", "nullable": True},
                {"name": "double_field", "type": "double", "nullable": True},
                {"name": "float_field", "type": "float", "nullable": True},
                {"name": "boolean_field", "type": "boolean", "nullable": True},
                {"name": "date_field", "type": "date", "nullable": True},
                {"name": "timestamp_field", "type": "timestamp", "nullable": True},
                {"name": "decimal_field", "type": "decimal", "nullable": True, "precision": 10, "scale": 2}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 9
        
        # Check that all data types are handled
        field_names = [field.name for field in result.fields]
        expected_fields = [
            "string_field", "integer_field", "long_field", "double_field",
            "float_field", "boolean_field", "date_field", "timestamp_field", "decimal_field"
        ]
        
        for field in expected_fields:
            assert field in field_names
    
    def test_convert_array_types(self):
        """Test conversion of various array types"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "string_array", "type": "array", "nullable": True, "elementType": "string"},
                {"name": "integer_array", "type": "array", "nullable": True, "elementType": "integer"},
                {"name": "double_array", "type": "array", "nullable": True, "elementType": "double"},
                {"name": "struct_array", "type": "array", "nullable": True, "elementType": "struct", "fields": [
                    {"name": "nested_field", "type": "string", "nullable": True}
                ]}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 4
        
        # Check that array types are handled
        field_names = [field.name for field in result.fields]
        assert "string_array" in field_names
        assert "integer_array" in field_names
        assert "double_array" in field_names
        assert "struct_array" in field_names
    
    def test_convert_map_types(self):
        """Test conversion of map types"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "string_map", "type": "map", "nullable": True, "keyType": "string", "valueType": "string"},
                {"name": "integer_map", "type": "map", "nullable": True, "keyType": "string", "valueType": "integer"},
                {"name": "struct_map", "type": "map", "nullable": True, "keyType": "string", "valueType": "struct", "fields": [
                    {"name": "nested_field", "type": "string", "nullable": True}
                ]}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 3
        
        # Check that map types are handled
        field_names = [field.name for field in result.fields]
        assert "string_map" in field_names
        assert "integer_map" in field_names
        assert "struct_map" in field_names
    
    def test_convert_invalid_schema_missing_type(self):
        """Test error handling for schema missing type field"""
        schema_json = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False}
            ]
        }
        
        with pytest.raises(ValueError, match="Schema definition must have type 'struct'"):
            convert_json_schema_to_spark(schema_json)
    
    def test_convert_invalid_schema_wrong_type(self):
        """Test error handling for schema with wrong type"""
        schema_json = {
            "type": "invalid_type",
            "fields": [
                {"name": "id", "type": "string", "nullable": False}
            ]
        }
        
        with pytest.raises(ValueError, match="Schema definition must have type 'struct'"):
            convert_json_schema_to_spark(schema_json)
    
    def test_convert_invalid_schema_missing_fields(self):
        """Test error handling for schema missing fields"""
        schema_json = {
            "type": "struct"
        }
        
        with pytest.raises(ValueError, match="Schema definition must have fields"):
            convert_json_schema_to_spark(schema_json)
    
    def test_convert_invalid_field_missing_name(self):
        """Test error handling for field missing name"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"type": "string", "nullable": False}
            ]
        }
        
        with pytest.raises(ValueError, match="Field definition must have 'name'"):
            convert_json_schema_to_spark(schema_json)
    
    def test_convert_field_missing_type_defaults_to_string(self):
        """Test that field missing type defaults to string"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "id", "nullable": False}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        assert result.fields[0].dataType == StringType()
        assert result.fields[0].name == "id"
        assert result.fields[0].nullable == False
    
    def test_convert_invalid_field_type_defaults_to_string(self):
        """Test that invalid field type defaults to string with warning"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "invalid_type", "nullable": False}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        assert result.fields[0].dataType == StringType()
        assert result.fields[0].name == "id"
        assert result.fields[0].nullable == False
    
    def test_convert_invalid_array_missing_element_type_defaults_to_string(self):
        """Test that array missing elementType defaults to string"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "array_field", "type": "array", "nullable": True}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        assert result.fields[0].dataType == StringType()
        assert result.fields[0].name == "array_field"
        assert result.fields[0].nullable == True
    
    def test_convert_invalid_map_missing_key_type_defaults_to_string(self):
        """Test that map missing keyType defaults to string"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "map_field", "type": "map", "nullable": True, "valueType": "string"}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        assert result.fields[0].dataType == StringType()
        assert result.fields[0].name == "map_field"
        assert result.fields[0].nullable == True
    
    def test_convert_invalid_map_missing_value_type_defaults_to_string(self):
        """Test that map missing valueType defaults to string"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "map_field", "type": "map", "nullable": True, "keyType": "string"}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        assert result.fields[0].dataType == StringType()
        assert result.fields[0].name == "map_field"
        assert result.fields[0].nullable == True
    
    def test_convert_invalid_struct_missing_fields_defaults_to_string(self):
        """Test that struct missing fields defaults to string"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "struct_field", "type": "struct", "nullable": True}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        assert result.fields[0].dataType == StringType()
        assert result.fields[0].name == "struct_field"
        assert result.fields[0].nullable == True
    
    def test_convert_decimal_precision_scale(self):
        """Test conversion of decimal type with precision and scale"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "price", "type": "decimal", "nullable": True, "precision": 10, "scale": 2},
                {"name": "rate", "type": "decimal", "nullable": True, "precision": 5, "scale": 4}
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 2
        
        # Check that decimal fields are handled
        field_names = [field.name for field in result.fields]
        assert "price" in field_names
        assert "rate" in field_names
    
    def test_convert_empty_schema_raises_error(self):
        """Test that empty schema raises error"""
        schema_json = {
            "type": "struct",
            "fields": []
        }
        
        with pytest.raises(ValueError, match="Schema definition must have fields"):
            convert_json_schema_to_spark(schema_json)
    
    def test_convert_nullable_fields(self):
        """Test conversion of nullable and non-nullable fields"""
        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "required_field", "type": "string", "nullable": False},
                {"name": "optional_field", "type": "string", "nullable": True},
                {"name": "default_nullable_field", "type": "string"}  # Should default to nullable=True
            ]
        }
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 3
        
        # Check that nullable fields are handled correctly
        field_names = [field.name for field in result.fields]
        assert "required_field" in field_names
        assert "optional_field" in field_names
        assert "default_nullable_field" in field_names
    
    def test_convert_real_world_schema(self):
        """Test conversion of real-world schema from the framework"""
        # This is based on the customer_demo_pipeline schema from the actual config
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
        
        result = convert_json_schema_to_spark(schema_json)
        
        assert result is not None
        assert len(result.fields) == 13
        
        # Check that all expected fields are present
        field_names = [field.name for field in result.fields]
        expected_fields = [
            "customer_id", "first_name", "last_name", "email", "phone_number",
            "address", "city", "state", "zip_code", "country", "customer_tier",
            "registration_date", "update_ts"
        ]
        
        for field in expected_fields:
            assert field in field_names


if __name__ == "__main__":
    # Run the tests if this file is executed directly
    pytest.main([__file__, "-v"])
