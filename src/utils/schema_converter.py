#!/usr/bin/env python3
"""
Schema Converter for Unified Pipeline Framework

This module provides utilities to convert JSON schema definitions to PySpark StructType
for use in Auto Loader pipelines with fixed schemas.
"""

import json
from typing import Dict, Any, List

# Try to import PySpark types, fallback to string representations for local development
try:
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType, 
        DoubleType, FloatType, BooleanType, DateType, TimestampType,
        ArrayType, MapType, DecimalType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    # PySpark not available (local development environment)
    PYSPARK_AVAILABLE = False
    # Define placeholder classes for type hints
    class StructType:
        def __init__(self, fields):
            self.fields = fields
        def __repr__(self):
            return f"StructType({self.fields})"
    
    class StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable
        def __repr__(self):
            return f"StructField('{self.name}', {self.dataType}, {self.nullable})"
    
    class StringType:
        def __repr__(self):
            return "StringType()"
    
    class IntegerType:
        def __repr__(self):
            return "IntegerType()"
    
    class LongType:
        def __repr__(self):
            return "LongType()"
    
    class DoubleType:
        def __repr__(self):
            return "DoubleType()"
    
    class FloatType:
        def __repr__(self):
            return "FloatType()"
    
    class BooleanType:
        def __repr__(self):
            return "BooleanType()"
    
    class DateType:
        def __repr__(self):
            return "DateType()"
    
    class TimestampType:
        def __repr__(self):
            return "TimestampType()"
    
    class DecimalType:
        def __repr__(self):
            return "DecimalType()"
    
    class ArrayType:
        def __repr__(self):
            return "ArrayType()"
    
    class MapType:
        def __repr__(self):
            return "MapType()"


def convert_json_schema_to_spark(schema_definition: Dict[str, Any]) -> StructType:
    """
    Convert a JSON schema definition to PySpark StructType.
    
    Args:
        schema_definition: Dictionary containing the schema definition
            Expected format: {"type": "struct", "fields": [...]}
    
    Returns:
        PySpark StructType object
        
    Example:
        schema_def = {
            "type": "struct",
            "fields": [
                {"name": "product_id", "type": "string", "nullable": False},
                {"name": "quantity", "type": "integer", "nullable": True}
            ]
        }
        spark_schema = convert_json_schema_to_spark(schema_def)
    """
    if not isinstance(schema_definition, dict):
        raise ValueError("Schema definition must be a dictionary")
    
    if schema_definition.get("type") != "struct":
        raise ValueError("Schema definition must have type 'struct'")
    
    fields = schema_definition.get("fields", [])
    if not fields:
        raise ValueError("Schema definition must have fields")
    
    spark_fields = []
    for field_def in fields:
        spark_field = _convert_field_definition(field_def)
        spark_fields.append(spark_field)
    
    return StructType(spark_fields)


def _convert_field_definition(field_def: Dict[str, Any]) -> StructField:
    """
    Convert a single field definition to PySpark StructField.
    
    Args:
        field_def: Dictionary containing field definition
            Expected format: {"name": "field_name", "type": "data_type", "nullable": bool}
    
    Returns:
        PySpark StructField object
    """
    if not isinstance(field_def, dict):
        raise ValueError("Field definition must be a dictionary")
    
    name = field_def.get("name")
    if not name:
        raise ValueError("Field definition must have 'name'")
    
    field_type = field_def.get("type", "string")
    nullable = field_def.get("nullable", True)
    
    # Convert string type to PySpark type
    spark_type = _convert_type_string_to_spark_type(field_type)
    
    return StructField(name, spark_type, nullable)


def _convert_type_string_to_spark_type(type_string: str):
    """
    Convert a type string to PySpark DataType.
    
    Args:
        type_string: String representation of the data type
    
    Returns:
        PySpark DataType object
    """
    type_mapping = {
        "string": StringType(),
        "str": StringType(),
        "integer": IntegerType(),
        "int": IntegerType(),
        "long": LongType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "datetime": TimestampType(),
    }
    
    # Handle complex types (arrays, maps, etc.)
    if type_string.startswith("array<"):
        # Extract inner type from array<inner_type>
        inner_type_str = type_string[6:-1]  # Remove "array<" and ">"
        inner_type = _convert_type_string_to_spark_type(inner_type_str)
        return ArrayType(inner_type)
    
    elif type_string.startswith("map<"):
        # Extract key and value types from map<key_type,value_type>
        inner_types = type_string[4:-1].split(",")  # Remove "map<" and ">", split by comma
        if len(inner_types) != 2:
            raise ValueError(f"Map type must have exactly 2 type parameters: {type_string}")
        
        key_type = _convert_type_string_to_spark_type(inner_types[0].strip())
        value_type = _convert_type_string_to_spark_type(inner_types[1].strip())
        return MapType(key_type, value_type)
    
    elif type_string.startswith("decimal"):
        # Handle decimal types like decimal(10,2)
        if "(" in type_string and ")" in type_string:
            # Extract precision and scale from decimal(precision,scale)
            params = type_string[7:-1].split(",")  # Remove "decimal(" and ")"
            if len(params) != 2:
                raise ValueError(f"Decimal type must have precision and scale: {type_string}")
            
            precision = int(params[0].strip())
            scale = int(params[1].strip())
            return DecimalType(precision, scale)
        else:
            return DecimalType(10, 0)  # Default precision and scale
    
    # Handle simple types
    if type_string in type_mapping:
        return type_mapping[type_string]
    
    # Default to StringType for unknown types
    print(f"Warning: Unknown type '{type_string}', defaulting to StringType")
    return StringType()


def validate_schema_definition(schema_definition: Dict[str, Any]) -> List[str]:
    """
    Validate a schema definition and return any errors found.
    
    Args:
        schema_definition: Dictionary containing the schema definition
    
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    
    if not isinstance(schema_definition, dict):
        errors.append("Schema definition must be a dictionary")
        return errors
    
    if schema_definition.get("type") != "struct":
        errors.append("Schema definition must have type 'struct'")
    
    fields = schema_definition.get("fields", [])
    if not fields:
        errors.append("Schema definition must have fields")
        return errors
    
    if not isinstance(fields, list):
        errors.append("Fields must be a list")
        return errors
    
    for i, field_def in enumerate(fields):
        if not isinstance(field_def, dict):
            errors.append(f"Field {i} must be a dictionary")
            continue
        
        if "name" not in field_def:
            errors.append(f"Field {i} must have 'name'")
        
        if "type" not in field_def:
            errors.append(f"Field {i} must have 'type'")
        
        # Validate nullable is boolean if present
        if "nullable" in field_def and not isinstance(field_def["nullable"], bool):
            errors.append(f"Field {i} 'nullable' must be a boolean")
    
    return errors


def convert_json_schema_to_spark_string(schema_definition: Dict[str, Any]) -> str:
    """
    Convert a JSON schema definition to a PySpark StructType string representation.
    This is useful for local development when PySpark is not available.
    
    Args:
        schema_definition: Dictionary containing the schema definition
        
    Returns:
        String representation of PySpark StructType
        
    Raises:
        ValueError: If schema definition is invalid
    """
    if not isinstance(schema_definition, dict):
        raise ValueError("Schema definition must be a dictionary")
    
    if schema_definition.get("type") != "struct":
        raise ValueError("Schema type must be 'struct'")
    
    fields = schema_definition.get("fields", [])
    if not isinstance(fields, list):
        raise ValueError("Schema must contain a 'fields' list")
    
    field_strings = []
    for field in fields:
        if not isinstance(field, dict):
            raise ValueError("Each field must be a dictionary")
        
        field_name = field.get("name")
        field_type = field.get("type")
        field_nullable = field.get("nullable", True)
        
        if not field_name or not field_type:
            raise ValueError("Each field must have 'name' and 'type'")
        
        # Map JSON schema types to PySpark type strings
        spark_type_str = _map_json_type_to_spark_type_string(field_type)
        field_strings.append(f"StructField('{field_name}', {spark_type_str}, {field_nullable})")
    
    return f"StructType([{', '.join(field_strings)}])"


def _map_json_type_to_spark_type_string(json_type: str) -> str:
    """Maps a JSON schema type string to a PySpark DataType string representation."""
    type_map = {
        "string": "StringType()",
        "integer": "IntegerType()",
        "long": "LongType()",
        "double": "DoubleType()",
        "float": "FloatType()",
        "boolean": "BooleanType()",
        "date": "DateType()",
        "timestamp": "TimestampType()",
        "decimal": "DecimalType()"
    }
    return type_map.get(json_type.lower(), "StringType()")  # Default to StringType for unknown types


def get_schema_summary(schema_definition: Dict[str, Any]) -> str:
    """
    Get a human-readable summary of the schema definition.
    
    Args:
        schema_definition: Dictionary containing the schema definition
    
    Returns:
        String summary of the schema
    """
    if not isinstance(schema_definition, dict):
        return "Invalid schema definition"
    
    fields = schema_definition.get("fields", [])
    if not fields:
        return "Empty schema"
    
    field_summaries = []
    for field_def in fields:
        name = field_def.get("name", "unknown")
        field_type = field_def.get("type", "unknown")
        nullable = field_def.get("nullable", True)
        nullable_str = "nullable" if nullable else "not null"
        field_summaries.append(f"  {name}: {field_type} ({nullable_str})")
    
    return f"Schema with {len(fields)} fields:\n" + "\n".join(field_summaries)


# Example usage and testing
if __name__ == "__main__":
    # Test schema conversion
    test_schema = {
        "type": "struct",
        "fields": [
            {"name": "product_id", "type": "string", "nullable": False},
            {"name": "product_name", "type": "string", "nullable": True},
            {"name": "quantity_on_hand", "type": "integer", "nullable": True},
            {"name": "unit_price", "type": "double", "nullable": True},
            {"name": "last_restocked", "type": "date", "nullable": True},
            {"name": "created_ts", "type": "timestamp", "nullable": True}
        ]
    }
    
    print("Testing schema conversion...")
    print(f"Input schema: {test_schema}")
    
    # Validate schema
    errors = validate_schema_definition(test_schema)
    if errors:
        print(f"Validation errors: {errors}")
    else:
        print("Schema validation passed")
    
    # Convert to PySpark schema
    try:
        spark_schema = convert_json_schema_to_spark(test_schema)
        print(f"Converted to PySpark schema: {spark_schema}")
        print(f"Schema summary:\n{get_schema_summary(test_schema)}")
    except Exception as e:
        print(f"Conversion error: {e}")
