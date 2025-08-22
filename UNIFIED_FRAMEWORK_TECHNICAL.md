# Unified Framework Technical Documentation

## Overview

The Unified Framework is a sophisticated data pipeline architecture that combines bronze and silver layer operations into single, declarative Delta Live Tables (DLT) pipelines. This document provides comprehensive technical details for developers and architects.

## Architecture Deep Dive

### Core Design Principles

1. **Unified Processing**: Single DLT pipeline per business domain handles both ingestion and transformation
2. **Static Generation**: Pre-generated notebooks ensure DLT compatibility and eliminate runtime errors
3. **Configuration-Driven**: TSV-based configuration enables rapid pipeline deployment
4. **Unity Catalog Native**: Full compatibility with modern Databricks data governance

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           unified_pipeline_config.tsv                   │   │
│  │  • Pipeline grouping by business domain                 │   │
│  │  • Bronze/silver operation definitions                  │   │
│  │  • Unified configuration parameters                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Generation Layer                             │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐ │
│  │ notebook_generator  │  │   unified_pipeline_generator    │ │
│  │ • Static notebooks  │  │ • DLT pipeline definitions      │ │
│  │ • Unity Catalog     │  │ • Resource management           │ │
│  │   compatibility     │  │ • Scheduling & notifications    │ │
│  └─────────────────────┘  └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Execution Layer                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Generated DLT Pipelines                    │   │
│  │  • Bronze: Auto Loader + audit columns                 │   │
│  │  • Silver: Auto CDC + SCD Type 2                       │   │
│  │  • Unified scheduling & monitoring                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration Schema

### TSV Structure

The configuration file uses tab-separated values with the following schema:

```tsv
operation_type	pipeline_group	source_type	file_format	source_path	target_table	trigger_type	schedule	pipeline_config	cluster_size	notifications
```

### Field Definitions

| Field | Type | Required | Description | Constraints |
|-------|------|----------|-------------|-------------|
| `operation_type` | String | Yes | `bronze` or `silver` | Must be exact values |
| `pipeline_group` | String | Yes | Business domain identifier | Alphanumeric + underscore |
| `source_type` | String | Bronze only | `file` for bronze operations | Required for bronze |
| `file_format` | String | Bronze only | `csv`, `json`, `parquet` | Supported formats |
| `source_path` | String | Bronze only | Source data location | Valid cloud storage path |
| `target_table` | String | Yes | Target table path | Unity Catalog format |
| `trigger_type` | String | Yes | `time` or `file` | Currently supports time |
| `schedule` | String | Yes | Cron expression | Valid cron format |
| `pipeline_config` | JSON | Yes | Operation-specific config | Valid JSON structure |
| `cluster_size` | String | Yes | `small`, `medium`, `large` | Predefined sizes |
| `notifications` | JSON | No | Email notification config | Valid JSON structure |

### Configuration Examples

#### Bronze Operation Configuration
```tsv
bronze	product_pipeline	file	csv	abfss://container@storage.dfs.core.windows.net/data/	vbdemos.adls_bronze.products_new	time	0 */10 * * *	{"schema_location": "abfss://.../schema", "checkpoint_location": "abfss://.../checkpoint", "cloudFiles.maxFilesPerTrigger": "200", "cloudFiles.allowOverwrites": "false", "header": "true", "inferSchema": "true"}	medium	{"email_on_success": "true", "email_on_failure": "true", "recipients": ["admin@company.com"]}
```

#### Silver Operation Configuration
```tsv
silver	product_pipeline	table		vbdemos.adls_bronze.products_new	vbdemos.adls_silver.products_scd2	time	0 */10 * * *	{"keys": ["product_id"], "track_history_except_column_list": ["product_name", "category", "price"], "stored_as_scd_type": "2", "sequence_by": "_ingestion_timestamp"}	medium	{"email_on_success": "true", "email_on_failure": "true", "recipients": ["data-team@company.com"]}
```

## Implementation Details

### Notebook Generation Process

The `notebook_generator.py` script performs the following steps:

1. **Configuration Parsing**: Reads TSV file and groups by `pipeline_group`
2. **Template Processing**: Uses Python f-strings for dynamic content generation
3. **Code Generation**: Creates static Python notebooks with proper DLT syntax
4. **File Output**: Writes generated notebooks to `src/notebooks/generated/`

### Generated Notebook Structure

Each generated notebook follows this pattern:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Pipeline - [Pipeline Group]
# MAGIC 
# MAGIC This notebook implements a unified pipeline for [pipeline_group] with:
# MAGIC - Bronze Layer: File ingestion using Autoloader
# MAGIC - Silver Layer: SCD Type 2 transformations using Auto CDC

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# BRONZE LAYER - File Ingestion with Autoloader

@dlt.table(
    name="[table_name]",
    table_properties={
        "quality": "bronze",
        "operation": "[operation_name]",
        "pipelines.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def [table_name]():
    return (spark.readStream
            .format("cloudFiles")
            .options(**[autoloader_options])
            .option("cloudFiles.format", "[file_format]")
            .load("[source_path]")
            .selectExpr("*", 
                        "current_timestamp() as _ingestion_timestamp",
                        "_metadata.file_name as _filename", 
                        "_metadata.file_path as _file_path",
                        "_metadata.file_modification_time as _file_modification_time"))

# COMMAND ----------

# SILVER LAYER - SCD Type 2 with Auto CDC

dlt.create_streaming_table("[target_table_name]")

@dlt.view
def bronze_[target_table_name]_source():
    return spark.readStream.table("[bronze_table_name]")

dlt.create_auto_cdc_flow(
    target="[target_table_name]",
    source="bronze_[target_table_name]_source",
    keys=[keys],
    sequence_by="[sequence_by]",
    except_column_list=[except_column_list],
    stored_as_scd_type="[stored_as_scd_type]"
)
```

### Resource Generation Process

The `unified_pipeline_generator.py` script:

1. **Reads Configuration**: Parses TSV and groups operations by pipeline group
2. **Creates DLT Pipelines**: Generates pipeline definitions with proper configuration
3. **Manages Resources**: Handles clusters, notifications, and scheduling
4. **Returns Resources**: Provides Databricks Asset Bundle compatible resource definitions

## Advanced Features

### Audit Column Implementation

The framework automatically adds comprehensive audit columns to bronze tables:

```python
.selectExpr("*", 
            "current_timestamp() as _ingestion_timestamp",
            "_metadata.file_name as _filename", 
            "_metadata.file_path as _file_path",
            "_metadata.file_modification_time as _file_modification_time")
```

#### Audit Column Details

| Column | Source | Purpose | Data Type |
|--------|--------|---------|-----------|
| `_ingestion_timestamp` | `current_timestamp()` | Processing timestamp for sequencing | Timestamp |
| `_filename` | `_metadata.file_name` | Source filename for audit trail | String |
| `_file_path` | `_metadata.file_path` | Full file path for lineage | String |
| `_file_modification_time` | `_metadata.file_modification_time` | File system modification time | Timestamp |

### Change Data Feed (CDF) Integration

Bronze tables automatically enable CDF:

```python
table_properties={
    "delta.enableChangeDataFeed": "true"
}
```

This enables:
- **Change Tracking**: All modifications are recorded
- **Audit Queries**: Historical change analysis
- **Compliance**: Regulatory and governance requirements

### Auto CDC Configuration

Silver layer operations use Auto CDC for SCD Type 2:

```python
dlt.create_auto_cdc_flow(
    target="target_table",
    source="bronze_source_view",
    keys=["primary_key"],
    sequence_by="_ingestion_timestamp",
    except_column_list=["columns_to_track"],
    stored_as_scd_type="2"
)
```

#### Auto CDC Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `target` | Target streaming table | `"products_scd2"` |
| `source` | Source view or table | `"bronze_products_source"` |
| `keys` | Primary key columns | `["product_id"]` |
| `sequence_by` | Ordering column | `"_ingestion_timestamp"` |
| `except_column_list` | Columns to track for changes | `["product_name", "category"]` |
| `stored_as_scd_type` | SCD type | `"2"` |

## Performance Optimization

### Auto Loader Optimizations

```python
table_properties={
    "pipelines.autoOptimize.optimizeWrite": "true",
    "pipelines.autoOptimize.autoCompact": "true"
}
```

### Batch Processing

```python
"cloudFiles.maxFilesPerTrigger": "200"
```

### Schema Inference

```python
"inferSchema": "true"
```

## Error Handling and Monitoring

### Pipeline Metrics

Auto CDC operations provide:
- `num_upserted_rows`: Number of rows inserted/updated
- `num_deleted_rows`: Number of rows deleted

### Error Scenarios

1. **Configuration Errors**: Invalid TSV format or JSON syntax
2. **Generation Errors**: Template processing failures
3. **Deployment Errors**: Databricks bundle deployment issues
4. **Runtime Errors**: DLT pipeline execution failures

### Monitoring Best Practices

1. **Pipeline Health**: Monitor pipeline status and update frequency
2. **Data Quality**: Track record counts and processing times
3. **Error Rates**: Monitor failure rates and error types
4. **Resource Utilization**: Track cluster usage and costs

## Security and Governance

### Unity Catalog Integration

- **Table Ownership**: Proper catalog and schema permissions
- **Data Lineage**: Complete audit trail from source to target
- **Access Control**: Row and column-level security support

### Data Privacy

- **Audit Logging**: All operations are logged
- **Change Tracking**: Complete modification history
- **Compliance**: GDPR and regulatory compliance support

## Troubleshooting Guide

### Common Issues

#### 1. Metadata Column Errors
**Error**: `No such struct field 'file_name' in _metadata`
**Solution**: Ensure Unity Catalog compatibility by using correct metadata fields

#### 2. Operation Column Errors
**Error**: `A column, variable, or function parameter with name 'operation' cannot be resolved`
**Solution**: Remove `apply_as_deletes` parameter for file-based ingestion

#### 3. DLT Compatibility Issues
**Error**: `Cannot redefine dataset`
**Solution**: Use static notebook generation instead of dynamic logic

### Debugging Steps

1. **Check Configuration**: Validate TSV format and JSON syntax
2. **Verify Generation**: Ensure notebooks are properly generated
3. **Review Logs**: Check Databricks pipeline logs for errors
4. **Test Incrementally**: Deploy and test one pipeline group at a time

## Best Practices

### Configuration Management

1. **Version Control**: Track configuration changes in Git
2. **Environment Separation**: Use different configs for dev/staging/prod
3. **Validation**: Implement configuration validation scripts
4. **Documentation**: Maintain clear configuration documentation

### Pipeline Design

1. **Logical Grouping**: Group related operations by business domain
2. **Consistent Naming**: Use consistent naming conventions
3. **Error Handling**: Implement proper error handling and notifications
4. **Monitoring**: Set up comprehensive monitoring and alerting

### Performance Tuning

1. **Batch Sizes**: Optimize `maxFilesPerTrigger` based on data volume
2. **Cluster Sizing**: Choose appropriate cluster sizes for workloads
3. **Scheduling**: Balance pipeline frequency with resource utilization
4. **Optimization**: Enable auto-optimize and auto-compact features

## Future Enhancements

### Planned Features

1. **Multi-Format Support**: Additional file format support
2. **Advanced Scheduling**: More sophisticated scheduling options
3. **Data Quality**: Built-in data quality checks
4. **Monitoring Dashboard**: Centralized monitoring interface

### Extension Points

1. **Custom Transformations**: User-defined transformation functions
2. **Plugin Architecture**: Modular component system
3. **API Integration**: REST API for pipeline management
4. **Multi-Cloud Support**: Additional cloud provider support

## Conclusion

The Unified Framework provides a robust, scalable solution for modern data pipeline architecture. By combining bronze and silver operations into unified DLT pipelines, it simplifies management while maintaining performance and reliability. The configuration-driven approach enables rapid deployment and the static generation ensures DLT compatibility.

For additional support and examples, refer to the main README and Databricks documentation.
