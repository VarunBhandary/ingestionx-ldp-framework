# Test Suite for Autoloader Framework PyDAB

This directory contains comprehensive tests for the autoloader framework, covering all major components and functionality.

## Overview

The test suite validates the entire autoloader framework including:

- **Expression Parsing**: Complex Spark SQL expression parsing with string literals and nested functions
- **Configuration Management**: TSV configuration loading, validation, and error handling
- **Schema Conversion**: JSON schema to PySpark StructType conversion
- **Pipeline Generation**: DLT pipeline and job creation with variable resolution
- **Notebook Generation**: Static notebook generation with proper formatting (auto-generated during bundle operations)
- **Data Generation**: Demo data generation with validation
- **Integration Testing**: End-to-end workflow validation

## Test Structure

### Core Component Tests

#### `test_expression_parser.py`
Tests the expression parsing functionality with comprehensive test cases:

- **Basic Expressions**: Simple comma-separated expressions
- **Function Calls**: Functions with parentheses (COALESCE, CASE, CONCAT, etc.)
- **String Literals with Commas**: Critical edge case that previously broke the parser
- **Nested Functions**: Complex nested function calls
- **Window Functions**: OVER clauses with PARTITION BY and ORDER BY
- **JSON Functions**: Functions with complex JSON paths
- **Date Functions**: Date formatting and manipulation functions
- **Regex Functions**: Regular expression functions with complex patterns
- **Mixed Expressions**: Combinations of different expression types
- **Complex Nested Cases**: Multi-level nested CASE statements
- **Edge Cases**: Empty expressions, trailing commas, etc.
- **Real-World Examples**: Actual expressions from the configuration

#### `test_config_parser.py`
Tests the configuration parsing and validation functionality:

- **TSV Loading**: Configuration file loading and parsing
- **Validation**: Required fields, data types, and format validation
- **Error Handling**: Invalid configurations and error reporting
- **Data Type Validation**: Source types, file formats, trigger types, cluster sizes
- **JSON Validation**: Pipeline configuration and email notification parsing
- **Schedule Validation**: Cron expression format validation
- **Edge Cases**: Empty configurations, missing fields, duplicate pipelines

#### `test_schema_converter.py`
Tests the schema conversion functionality:

- **Basic Types**: String, integer, double, boolean, date, timestamp conversion
- **Nested Structures**: Struct and array type handling
- **Complex Schemas**: Multi-level nested structures
- **Data Type Mapping**: JSON schema to PySpark type conversion
- **Error Handling**: Invalid schemas and missing fields
- **Edge Cases**: Empty schemas, invalid types, missing required fields

#### `test_unified_pipeline_generator.py`
Tests the pipeline generation functionality:

- **Bundle Integration**: Variable resolution and bundle interaction
- **Pipeline Creation**: DLT pipeline generation with proper configuration
- **Job Creation**: Manual job creation and scheduling
- **Variable Resolution**: TSV variable substitution
- **Configuration Parsing**: Pipeline and email notification parsing
- **Error Handling**: Missing variables and invalid configurations

#### `test_notebook_generator.py`
Tests the notebook generation functionality:

- **Notebook Creation**: Static notebook generation with proper structure
- **Layer Generation**: Bronze, silver, and gold layer code generation
- **Custom Expressions**: Complex expression handling and formatting
- **Variable Resolution**: TSV variable substitution in notebooks
- **File Operations**: Notebook file creation and content validation
- **Error Handling**: Invalid configurations and missing fields

#### `test_data_generation_demo.py`
Tests the data generation functionality:

- **Widget Handling**: Databricks widget parameter management
- **Data Generation**: Customer, product, transaction, inventory, shipment data
- **Data Validation**: Format validation, uniqueness, and consistency checks
- **Error Handling**: Missing parameters and invalid configurations
- **Edge Cases**: Zero count, negative count, large datasets

### Integration Tests

#### `test_integration.py`
Tests end-to-end functionality:

- **Complete Workflow**: Full pipeline generation and deployment simulation
- **Component Integration**: Cross-component functionality validation
- **Configuration Validation**: End-to-end configuration processing
- **Error Propagation**: Error handling across components
- **Complex Scenarios**: Real-world configuration testing

### Test Configuration

#### `conftest.py`
Pytest configuration and shared fixtures:

- **Common Fixtures**: Reusable test data and mock objects
- **Test Markers**: Categorization of tests by type and component
- **Configuration**: Pytest settings and test discovery

#### `test_cases.json`
Comprehensive test case database:

- **Expression Cases**: All expression parsing test cases
- **Configuration Cases**: Sample configurations for testing
- **Schema Cases**: Sample schemas for conversion testing
- **Regression Cases**: Known issues and their fixes

## Running Tests

### Quick Test Run
```bash
uv run pytest tests/ -v
```

### With Coverage Report
```bash
uv run pytest tests/ --cov=resources --cov=src --cov-report=html --cov-report=term
```

### Run Specific Test File
```bash
uv run pytest tests/test_config_parser.py -v
```

### Run All Tests
```bash
uv run pytest tests/ -v
```

## Test Categories

### 1. Basic Expressions
Tests simple comma-separated expressions:
```python
"col1, col2, col3" → ["col1", "col2", "col3"]
"col1 as alias1, col2 as alias2" → ["col1 as alias1", "col2 as alias2"]
```

### 2. Function Calls
Tests functions with parentheses:
```python
"COALESCE(col1, col2) as result" → ["COALESCE(col1, col2) as result"]
"CASE WHEN col1 IS NULL THEN col2 ELSE col3 END as result" → ["CASE WHEN col1 IS NULL THEN col2 ELSE col3 END as result"]
```

### 3. String Literals with Commas (Critical Edge Case)
Tests the most problematic case that previously broke the parser:
```python
"CASE WHEN col1 = \",\" THEN col2 ELSE col3 END as result" → ["CASE WHEN col1 = \",\" THEN col2 ELSE col3 END as result"]
"CONCAT(col1, \", \", col2) as result" → ["CONCAT(col1, \", \", col2) as result"]
```

### 4. Nested Functions
Tests complex nested function calls:
```python
"COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4) as result" → ["COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4) as result"]
```

### 5. Window Functions
Tests window functions with OVER clauses:
```python
"ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2) as rn" → ["ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2) as rn"]
```

## Known Issues and Fixes

### Issue 1: Commas in String Literals
**Problem**: The original parser would incorrectly split expressions at commas inside string literals.
**Example**: `CASE WHEN col1 = "," THEN col2 ELSE col3 END` would be split at the comma inside the string.
**Fix**: Implemented string literal awareness in the parser to track when we're inside quotes.

### Issue 2: Nested Function Calls
**Problem**: Complex nested function calls weren't handled properly.
**Example**: `COALESCE(CASE WHEN col1 = 1 THEN col2 ELSE col3 END, col4)` would be incorrectly parsed.
**Fix**: Added parentheses counting to track nesting levels.

## Regression Testing

The test suite includes regression tests to catch any future issues:

1. **String Literal Commas**: Ensures commas inside string literals don't break parsing
2. **Nested Functions**: Validates complex nested function calls work correctly
3. **Real-World Examples**: Tests actual expressions from the configuration files

## Adding New Tests

When adding new test cases:

1. Add the test case to the appropriate test method in `test_expression_parser.py`
2. Include a description of what the test validates
3. Add the test case to `test_cases.json` for documentation
4. Run the full test suite to ensure no regressions

## Test Coverage

The test suite covers:
- ✅ Basic comma separation
- ✅ Function calls with parentheses
- ✅ String literals with commas (critical edge case)
- ✅ Nested function calls
- ✅ Window functions
- ✅ JSON functions
- ✅ Date functions
- ✅ Regex functions
- ✅ Mixed expressions
- ✅ Complex nested cases
- ✅ Edge cases
- ✅ Real-world examples

## Continuous Integration

These tests should be run:
- Before any changes to expression parsing logic
- As part of the CI/CD pipeline
- When adding new expression types to the configuration
- Before deploying to production

## Troubleshooting

If tests fail:

1. Check the specific test case that failed
2. Verify the input expression is valid Spark SQL
3. Ensure the expected output matches the actual parsing result
4. Check for any new edge cases not covered by existing tests
5. Update the parser logic if needed and add new test cases

## Future Enhancements

Potential improvements to the test suite:

1. **Performance Tests**: Test parsing performance with very large expressions
2. **Memory Tests**: Ensure parser doesn't consume excessive memory
3. **Error Handling**: Test invalid expressions and error recovery
4. **Unicode Support**: Test expressions with Unicode characters
5. **SQL Dialect Variations**: Test different SQL dialects if supported
