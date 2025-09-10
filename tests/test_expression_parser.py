#!/usr/bin/env python3
"""
Test suite for expression parsing logic in notebook_generator.py

This test suite validates that the _parse_expressions_smart function correctly handles
various Spark SQL expression patterns, including:
- Function calls with parentheses
- String literals with commas
- Nested quotes and escapes
- Complex SQL expressions
- Edge cases that could break the parser

Run with: python -m pytest tests/test_expression_parser.py -v
"""

import pytest
import sys
import os

# Add the resources directory to the path so we can import the function
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'resources'))

from notebook_generator import _parse_expressions_smart


class TestExpressionParser:
    """Test cases for expression parsing functionality"""
    
    def test_basic_expressions(self):
        """Test basic comma-separated expressions"""
        test_cases = [
            ("col1, col2, col3", ["col1", "col2", "col3"]),
            ("col1 as alias1, col2 as alias2", ["col1 as alias1", "col2 as alias2"]),
            ("col1, col2 as alias2, col3", ["col1", "col2 as alias2", "col3"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_function_calls(self):
        """Test function calls with parentheses"""
        test_cases = [
            ("COALESCE(col1, col2) as result", ["COALESCE(col1, col2) as result"]),
            ("CASE WHEN col1 IS NULL THEN col2 ELSE col3 END as result", 
             ["CASE WHEN col1 IS NULL THEN col2 ELSE col3 END as result"]),
            ("CONCAT(col1, col2, col3) as full_name", 
             ["CONCAT(col1, col2, col3) as full_name"]),
            ("ARRAY(col1, col2, col3) as arr", ["ARRAY(col1, col2, col3) as arr"]),
            ("SIZE(ARRAY(col1, col2)) as arr_size", ["SIZE(ARRAY(col1, col2)) as arr_size"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_nested_functions(self):
        """Test nested function calls"""
        test_cases = [
            ("COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4) as result",
             ["COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4) as result"]),
            ("CONCAT(COALESCE(col1, \"default\"), \"_\", col2) as result",
             ["CONCAT(COALESCE(col1, \"default\"), \"_\", col2) as result"]),
            ("COALESCE(CASE WHEN col1 = 1 THEN CONCAT(col2, \", \", col3) ELSE col4 END, col5) as result",
             ["COALESCE(CASE WHEN col1 = 1 THEN CONCAT(col2, \", \", col3) ELSE col4 END, col5) as result"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_string_literals_with_commas(self):
        """Test string literals containing commas"""
        test_cases = [
            ("CONCAT(col1, \", \", col2) as result", 
             ["CONCAT(col1, \", \", col2) as result"]),
            ("CASE WHEN col1 = \",\" THEN col2 ELSE col3 END as result",
             ["CASE WHEN col1 = \",\" THEN col2 ELSE col3 END as result"]),
            ("REGEXP_REPLACE(col1, \"[^a-zA-Z0-9,]\", \"_\") as cleaned",
             ["REGEXP_REPLACE(col1, \"[^a-zA-Z0-9,]\", \"_\") as cleaned"]),
            ("ARRAY(\"a\", \"b\", \"c\") as arr", ["ARRAY(\"a\", \"b\", \"c\") as arr"]),
            ("ARRAY(\"a,b\", \"c,d\") as arr", ["ARRAY(\"a,b\", \"c,d\") as arr"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_escaped_quotes(self):
        """Test escaped quotes in string literals"""
        test_cases = [
            ("CASE WHEN col1 = \"He said, \\\"Hello, world!\\\"\" THEN col2 ELSE col3 END as result",
             ["CASE WHEN col1 = \"He said, \\\"Hello, world!\\\"\" THEN col2 ELSE col3 END as result"]),
            ("CONCAT(col1, \"He said, \\\"Hello\\\"\", col2) as result",
             ["CONCAT(col1, \"He said, \\\"Hello\\\"\", col2) as result"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_window_functions(self):
        """Test window functions with OVER clauses"""
        test_cases = [
            ("ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2) as rn",
             ["ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2) as rn"]),
            ("LAG(col1, 1) OVER (PARTITION BY col2 ORDER BY col3) as prev_val",
             ["LAG(col1, 1) OVER (PARTITION BY col2 ORDER BY col3) as prev_val"]),
            ("RANK() OVER (ORDER BY col1 DESC) as rank_val",
             ["RANK() OVER (ORDER BY col1 DESC) as rank_val"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_json_functions(self):
        """Test JSON functions with complex paths"""
        test_cases = [
            ("GET_JSON_OBJECT(col1, \"$.field\") as json_field",
             ["GET_JSON_OBJECT(col1, \"$.field\") as json_field"]),
            ("GET_JSON_OBJECT(col1, \"$.field,with,commas\") as json_field",
             ["GET_JSON_OBJECT(col1, \"$.field,with,commas\") as json_field"]),
            ("FROM_JSON(col1, \"col1 STRING, col2 INT\") as parsed",
             ["FROM_JSON(col1, \"col1 STRING, col2 INT\") as parsed"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_date_functions(self):
        """Test date functions with format strings"""
        test_cases = [
            ("DATE_FORMAT(col1, \"yyyy-MM-dd\") as formatted_date",
             ["DATE_FORMAT(col1, \"yyyy-MM-dd\") as formatted_date"]),
            ("DATEDIFF(col1, col2) as days_diff", ["DATEDIFF(col1, col2) as days_diff"]),
            ("TO_DATE(col1, \"yyyy-MM-dd HH:mm:ss\") as parsed_date",
             ["TO_DATE(col1, \"yyyy-MM-dd HH:mm:ss\") as parsed_date"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_regex_functions(self):
        """Test regex functions with complex patterns"""
        test_cases = [
            ("REGEXP_EXTRACT(col1, \"([0-9]+)\", 1) as extracted",
             ["REGEXP_EXTRACT(col1, \"([0-9]+)\", 1) as extracted"]),
            ("REGEXP_REPLACE(col1, \"[^a-zA-Z0-9]\", \"_\") as cleaned",
             ["REGEXP_REPLACE(col1, \"[^a-zA-Z0-9]\", \"_\") as cleaned"]),
            ("REGEXP_REPLACE(col1, \"[^a-zA-Z0-9,]\", \"_\") as cleaned",
             ["REGEXP_REPLACE(col1, \"[^a-zA-Z0-9,]\", \"_\") as cleaned"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_mixed_expressions(self):
        """Test mixed expressions with multiple parts"""
        test_cases = [
            ("col1, COALESCE(col2, col3) as result, current_timestamp() as ts",
             ["col1", "COALESCE(col2, col3) as result", "current_timestamp() as ts"]),
            ("col1 as alias1, CONCAT(col2, \", \", col3) as full_name, col4",
             ["col1 as alias1", "CONCAT(col2, \", \", col3) as full_name", "col4"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_complex_nested_cases(self):
        """Test complex nested CASE statements"""
        test_cases = [
            ("CASE WHEN col1 = 1 THEN COALESCE(col2, CONCAT(\"default\", \", \", col3)) WHEN col1 = 2 THEN col4 ELSE col5 END as result",
             ["CASE WHEN col1 = 1 THEN COALESCE(col2, CONCAT(\"default\", \", \", col3)) WHEN col1 = 2 THEN col4 ELSE col5 END as result"]),
            ("CASE WHEN col1 = \",\" THEN CONCAT(col2, \", \", col3) WHEN col1 = \";\" THEN col4 ELSE col5 END as result",
             ["CASE WHEN col1 = \",\" THEN CONCAT(col2, \", \", col3) WHEN col1 = \";\" THEN col4 ELSE col5 END as result"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_edge_cases(self):
        """Test edge cases and potential failure points"""
        test_cases = [
            # Empty expression
            ("", []),
            # Single expression
            ("col1", ["col1"]),
            # Expression with only spaces
            ("   ", []),
            # Expression with trailing comma
            ("col1, col2,", ["col1", "col2"]),
            # Expression with leading comma
            (", col1, col2", ["", "col1", "col2"]),
            # Expression with multiple commas
            ("col1,, col2", ["col1", "", "col2"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"
    
    def test_real_world_examples(self):
        """Test real-world examples from the actual config"""
        test_cases = [
            # From product_catalog_cdc_pipeline (without the * prefix)
            ("CASE WHEN deleted_at IS NOT NULL THEN 'DELETE' WHEN created_at = updated_at THEN 'INSERT' ELSE 'UPDATE' END as cdc_operation, COALESCE(updated_at, created_at) as sequence_ts, current_timestamp() as _ingestion_timestamp",
             ["CASE WHEN deleted_at IS NOT NULL THEN 'DELETE' WHEN created_at = updated_at THEN 'INSERT' ELSE 'UPDATE' END as cdc_operation", 
              "COALESCE(updated_at, created_at) as sequence_ts", 
              "current_timestamp() as _ingestion_timestamp"]),
            
            # From product_catalog_cdc_pipeline silver layer
            ("product_id, product_name as product_title, category as product_category, price as unit_price, description as product_description, status as product_status, created_at as effective_start_date, updated_at as last_modified_date, deleted_at as soft_delete_date, cdc_operation, sequence_ts",
             ["product_id", "product_name as product_title", "category as product_category", "price as unit_price", 
              "description as product_description", "status as product_status", "created_at as effective_start_date", 
              "updated_at as last_modified_date", "deleted_at as soft_delete_date", "cdc_operation", "sequence_ts"]),
        ]
        
        for input_expr, expected in test_cases:
            result = _parse_expressions_smart(input_expr)
            assert result == expected, f"Failed for input: {input_expr}"


if __name__ == "__main__":
    # Run the tests if this file is executed directly
    pytest.main([__file__, "-v"])
