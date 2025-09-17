# Configuration Validation Guide

This guide explains how to validate your TSV configuration before running bundle operations.

## Overview

The autoloader framework provides multiple validation layers to ensure your configuration is correct before deployment:

1. **Standalone Validation Script** - Run validation independently
2. **Pre-hook Validation** - Automatic validation during resource loading
3. **Bundle Validation** - Databricks CLI validation
4. **Runtime Validation** - Validation during pipeline execution

## Quick Start

### Option 1: Python Script (Recommended)

```bash
# Activate virtual environment
source .venv/bin/activate

# Basic validation
python3 src/utils/validate_config.py

# Validation with summary
python3 src/utils/validate_config.py --summary

# Validate specific config file
python3 src/utils/validate_config.py --config-file my_config.tsv

# Quiet validation (errors only)
python3 src/utils/validate_config.py --quiet
```

### Option 2: Shell Script

```bash
# Basic validation
./scripts/validate.sh

# Validation with summary
./scripts/validate.sh --summary

# Validate specific config file
./scripts/validate.sh --config-file my_config.tsv

# Quiet validation
./scripts/validate.sh --quiet
```

### Option 3: Bundle Validation

```bash
# Databricks bundle validation (includes pre-hook validation)
databricks bundle validate --profile dev
```

## Validation Layers

### 1. Standalone Validation Script

The `src/utils/validate_config.py` script provides comprehensive validation:

**Features:**
- ✅ Required column validation
- ✅ Data type validation
- ✅ Value range validation
- ✅ JSON syntax validation
- ✅ Path format validation
- ✅ Email format validation
- ✅ Variable usage validation
- ✅ Schema validation
- ✅ Configuration summary

**Usage:**
```bash
python3 src/utils/validate_config.py [OPTIONS]

Options:
  --config-file FILE    Path to configuration TSV file
  --profile PROFILE     Databricks profile (for future use)
  --summary            Show configuration summary
  --quiet              Only show errors, no success messages
  -h, --help           Show help message
```

### 2. Pre-hook Validation

Validation automatically runs when loading resources:

```python
# This happens automatically in resources/unified_pipeline_generator.py
def load_config(self) -> pd.DataFrame:
    # ... load config ...
    self._validate_config_pre_hook(df)  # ← Pre-hook validation
    # ... continue processing ...
```

**What it validates:**
- All basic validation rules
- Early error detection
- Prevents resource generation with invalid config

### 3. Bundle Validation

Databricks CLI validation:

```bash
databricks bundle validate --profile dev
```

**What it validates:**
- Configuration syntax
- Databricks API compatibility
- Resource definitions
- Variable resolution

## Validation Rules

### Required Columns

Your TSV must contain these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `operation_type` | ✅ | bronze, silver, gold, or manual |
| `pipeline_group` | ✅ | Groups related operations |
| `source_type` | ✅ | file, table, or notebook |
| `source_path` | ✅ | Path to source data or notebook |
| `target_table` | ✅ | Target table name |
| `file_format` | ✅ | csv, json, parquet, etc. (empty for manual) |
| `trigger_type` | ✅ | file or time |
| `schedule` | ✅ | Cron expression |
| `pipeline_config` | ✅ | JSON configuration |
| `cluster_size` | ✅ | small, medium, large, or serverless |
| `notifications` | ✅ | JSON notification settings |
| `custom_expr` | ✅ | Custom expressions (optional) |
| `parameters` | ✅ | JSON parameters for manual operations |
| `cluster_config` | ❌ | Optional JSON cluster configuration |

### Data Validation

#### Operation Types
- **Valid**: `bronze`, `silver`, `gold`, `manual`
- **Error**: `Invalid operation types: ['invalid_type']`

#### Source Types
- **Valid**: `file`, `table`, `notebook`
- **Error**: `Invalid source types: ['invalid_source']`

#### File Formats
- **Valid**: `json`, `parquet`, `csv`, `avro`, `orc`, `''` (empty)
- **Error**: `Invalid file formats: ['invalid_format']`

#### Trigger Types
- **Valid**: `file`, `time`
- **Error**: `Invalid trigger types: ['invalid_trigger']`

#### Cluster Sizes
- **Valid**: `small`, `medium`, `large`, `serverless`
- **Error**: `Invalid cluster sizes: ['invalid_size']`

### Path Validation

#### File Sources
Must start with:
- `/Volumes/` (Unity Catalog volumes)
- `s3://` (S3 paths)
- `abfss://` (Azure Data Lake Storage)
- `src/` (local source files)

#### Table Sources
Must be in format: `catalog.schema.table`

#### Notebook Sources
Must start with: `src/`

### JSON Validation

#### Pipeline Config
```json
{
  "schema": {"type": "struct", "fields": [...]},
  "cloudFiles.checkpointLocation": "/Volumes/...",
  "header": "true"
}
```

#### Notifications
```json
{
  "on_success": true,
  "on_failure": true,
  "recipients": ["admin@company.com"]
}
```

#### Parameters (Manual Operations)
```json
{
  "batch_size": 1000,
  "debug_mode": true,
  "catalog": "${var.catalog_name}"
}
```

### Variable Validation

#### Valid Variables
- `${var.catalog_name}`
- `${var.schema_name}`
- `${var.volume_name}`

#### Error Examples
- `Unknown variable '${var.invalid_var}'`
- Variables must be defined in `databricks.yml`

## Common Validation Errors

### 1. Missing Required Columns
```
Missing required columns: ['operation_type', 'pipeline_group']
```
**Fix**: Ensure all required columns are present in your TSV.

### 2. Invalid JSON Syntax
```
Row 8: Invalid JSON in pipeline_config: {"header": "true", "invalid_json": }
```
**Fix**: Validate JSON syntax using a JSON validator.

### 3. Invalid File Paths
```
Row 3: Invalid file path format: /invalid/path. Should start with /Volumes/, s3://, abfss://, or src/
```
**Fix**: Use valid path formats for your source type.

### 4. Invalid Email Formats
```
Row 11: Invalid email format in recipients: invalid-email-format
```
**Fix**: Use valid email addresses in notification recipients.

### 5. Serverless Cluster Requirements
```
Row 13: serverless cluster requires warehouse_id in cluster_config
```
**Fix**: Add `warehouse_id` to cluster_config for serverless clusters.

### 6. Parameter Validation
```
Row 16: Parameter 'invalid_param' must be a string, number, or boolean
```
**Fix**: Use only basic data types for parameters.

## Integration with CI/CD

### Pre-commit Hook

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: validate-config
        name: Validate autoloader config
        entry: python3 src/utils/validate_config.py
        language: system
        files: ^config/.*\.tsv$
```

### GitHub Actions

```yaml
name: Validate Configuration
on: [push, pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install pandas
      - name: Validate configuration
        run: python3 src/utils/validate_config.py --summary
```

### GitLab CI

```yaml
validate_config:
  stage: validate
  script:
    - python3 src/utils/validate_config.py --summary
  only:
    changes:
      - config/*.tsv
```

## Troubleshooting

### Virtual Environment Issues

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install pandas

# Run validation
python3 src/utils/validate_config.py
```

### Permission Issues

```bash
# Make scripts executable
chmod +x scripts/validate.sh
chmod +x validate_config.py
```

### Path Issues

```bash
# Run from project root
cd /path/to/autoloader-framework-pydab
python3 src/utils/validate_config.py
```

## Best Practices

1. **Run validation early**: Validate before making changes
2. **Use summary mode**: Get overview of your configuration
3. **Fix errors immediately**: Don't accumulate validation errors
4. **Test with different profiles**: Validate for all environments
5. **Integrate with CI/CD**: Catch errors in automated pipelines
6. **Document your config**: Keep track of complex configurations

## Examples

### Valid Configuration Example

```tsv
operation_type	pipeline_group	source_type	file_format	source_path	target_table	trigger_type	schedule	pipeline_config	cluster_size	notifications	custom_expr	parameters
bronze	customer_pipeline	file	csv	/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/customers	${var.catalog_name}.adls_bronze.customers	time	0 0 6 * * ?	{"header": "true", "cloudFiles.checkpointLocation": "/Volumes/${var.catalog_name}/${var.schema_name}/${var.volume_name}/checkpoint/customers"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}
```

### Validation Output

```
🔍 Validating configuration: config/unified_pipeline_config.tsv
============================================================
✅ Configuration validation passed!
   - Loaded 16 pipeline configurations
   - All validations passed successfully

📊 Configuration Summary:
------------------------------
Operation Types:
  - bronze: 6
  - silver: 6
  - manual: 4

Source Types:
  - file: 6
  - table: 6
  - notebook: 4

Pipeline Groups:
  - customer_demo_pipeline: 3
  - transaction_demo_pipeline: 2
  - inventory_demo_pipeline: 2
  - shipment_demo_pipeline: 2
  - manual_test_pipeline: 4
  - product_catalog_cdc_pipeline: 2
  - silver_only_test_pipeline: 1

File Formats:
  - csv: 4
  - json: 2

🎉 Configuration is ready for bundle operations!
```

## Support

If you encounter validation issues:

1. Check the error messages carefully
2. Refer to this validation guide
3. Use the `--help` option for script usage
4. Check the main README.md for configuration examples
5. Review the DEVELOPER.md for advanced usage
