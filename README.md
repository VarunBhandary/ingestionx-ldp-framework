# Azure Databricks Unified Pipeline Framework

A simplified framework that combines bronze (ingestion) and silver (transformation) layer pipelines into unified DLT pipelines using Azure Databricks, Delta Live Tables (DLT), and Databricks Asset Bundles (PyDABs).

## 🏗️ Architecture

```
Azure Data Lake Storage → Unified Pipeline → Delta Live Tables → Delta Tables
                                    ↓
                              Bronze + Silver Operations
                                    ↓
                              Grouped by Pipeline Group
```

**Unified Data Architecture:**
- **Bronze Layer**: Raw data ingestion with Auto Loader
- **Silver Layer**: Clean, quality-checked data with Auto CDC SCD Type 2
- **Unified Processing**: Single pipeline handles both operations efficiently

## 🚀 Features

- **Unified Pipeline Processing**: Single pipeline handles both bronze (ingestion) and silver (transformation) operations
- **Automatic Grouping**: Related operations are automatically grouped by pipeline_group for efficient processing
- **DLT Pipelines**: Delta Live Tables for streaming data processing
- **Flexible Cluster Configuration**: Support for small, medium, large, and serverless compute
- **Email Notifications**: Configurable success/failure notifications
- **Resource Tagging**: Automatic tagging for cost tracking and monitoring
- **Simplified Configuration**: Single TSV file for all pipeline configurations

## 📁 Project Structure

```
autloader-framework-pydab/
├── src/
│   ├── notebooks/                    # Core pipeline notebooks
│   │   └── unified_pipeline.py       # Unified bronze & silver pipeline
│   └── utils/
│       └── config_parser.py          # TSV configuration parser
├── resources/
│   └── unified_pipeline_generator.py # Unified resource generation
├── config/
│   └── unified_pipeline_config.tsv   # Unified configuration for all pipelines
├── docs/
│   ├── DEVELOPER.md                  # Framework extension guide
│   └── USER_GUIDE.md                 # User deployment guide
├── databricks.yml                    # Main bundle configuration
├── UNIFIED_FRAMEWORK_README.md       # Unified framework documentation
└── README.md                         # This file
```



## 📋 Unified Configuration Fields

| Field | Description | Required | Example |
|-------|-------------|----------|---------|
| `pipeline_type` | Type of operation: `bronze` or `silver` | Yes | `bronze` |
| `pipeline_group` | Group name for related operations | Yes | `customer_pipeline` |
| `source_type` | Source storage type (bronze only) | Bronze only | `adls` |
| `source_path` | Source data path (bronze only) | Bronze only | `abfss://container@storage.dfs.core.windows.net/path/` |
| `target_table` | Target table name | Yes | `catalog.schema.table_name` |
| `file_format` | Source file format (bronze only) | Bronze only | `csv`, `json`, `parquet` |
| `trigger_type` | Pipeline trigger type | Yes | `file`, `time` |
| `schedule` | Cron schedule for time-based triggers | Silver only | `0 */10 * * *` |
| `pipeline_config` | **Unified configuration** (JSON) | Yes | See examples below |
| `cluster_size` | Compute size (small/medium/large/serverless) | Yes | `medium` |
| `cluster_config` | Additional cluster configuration | No | JSON string or empty |
| `email_notifications` | Email notification settings | No | JSON with recipients and triggers |

## 🔧 Pipeline Configuration Examples

### Bronze Layer (File Ingestion)
```json
{
  "schema_location": "abfss://.../schema",
  "checkpoint_location": "abfss://.../checkpoint",
  "cloudFiles.maxFilesPerTrigger": "100",
  "cloudFiles.allowOverwrites": "false",
  "header": "true"
}
```

### Silver Layer (SCD Type 2)
```json
{
  "keys": ["customer_id"],
  "track_history_except_column_list": ["name", "email", "phone"],
  "stored_as_scd_type": "2",
  "sequence_by": "_ingestion_timestamp"
}
```

## 🚀 Quick Start

1. **Clone and Configure**:
   ```bash
   git clone <repository-url>
   cd autloader-framework-pydab
   ```

2. **Update Configuration**:
   - Edit `config/unified_pipeline_config.tsv` with your data sources
   - Group related bronze and silver operations by `pipeline_group`

3. **Deploy**:
   ```bash
   databricks bundle deploy
   ```

## 📧 Email Notifications

Configure email notifications for pipeline success/failure:

```json
{
  "email_on_success": true,
  "email_on_failure": true,
  "recipients": ["admin@company.com", "data-team@company.com"]
}
```

## 🏷️ Resource Tagging

All deployed resources are automatically tagged with:
- `deployment_type: unified_framework`
- `framework: unified-autoloader-pydab`
- `pipeline_type: bronze/silver`
- `pipeline_group: [group_name]`

## 📚 Documentation

- **[Developer Guide](docs/DEVELOPER.md)**: Extending the framework
- **[User Guide](docs/USER_GUIDE.md)**: Deploying unified pipelines
- **[Unified Framework Guide](UNIFIED_FRAMEWORK_README.md)**: Complete unified framework documentation

## 🔗 References

- [Azure Databricks Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Delta Live Tables](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
- [Databricks Asset Bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- [Auto CDC Documentation](https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc)

## 🤝 Contributing

See [DEVELOPER.md](docs/DEVELOPER.md) for guidelines on extending the framework.

## 📄 License

[Add your license information here]
