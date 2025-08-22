# AutoLoader Framework - Unified Pipeline Architecture

A modern, declarative data pipeline framework for Databricks that combines bronze and silver layer operations into unified, configurable pipelines using Delta Live Tables (DLT) and Auto Loader.

## 🚀 Features

- **Unified Pipeline Architecture**: Single declarative pipeline per pipeline group handling both bronze and silver operations
- **Auto Loader Integration**: Seamless file ingestion with Unity Catalog compatible metadata tracking
- **SCD Type 2 Support**: Built-in Slowly Changing Dimensions with Auto CDC for historical data tracking
- **Change Data Feed (CDF)**: Enabled on bronze tables for audit and change tracking
- **Declarative Configuration**: Simple TSV-based configuration for pipeline definitions
- **Static Notebook Generation**: Pre-generated notebooks for each pipeline group ensuring DLT compatibility
- **Unity Catalog Ready**: Full compatibility with Databricks Unity Catalog
- **Automated Scheduling**: Cron-based job scheduling with proper resource references
- **Serverless Compute**: Optimized for quick testing and development

## 🏗️ Architecture

The framework uses a **unified pipeline approach** where each `pipeline_group` contains:
- **Bronze Layer**: File ingestion using Auto Loader with audit columns
- **Silver Layer**: SCD Type 2 transformations using Auto CDC
- **Unified Configuration**: Single config row per operation type (bronze/silver)

### Key Components

1. **Configuration Layer** (`config/unified_pipeline_config.tsv`)
   - Pipeline grouping by business domain
   - Bronze and silver operation definitions
   - Quartz cron scheduling configuration

2. **Notebook Generator** (`resources/notebook_generator.py`)
   - Generates static DLT notebooks for each pipeline group
   - Ensures Unity Catalog compatibility
   - Creates proper audit column structure

3. **Resource Generator** (`resources/unified_pipeline_generator.py`)
   - Creates DLT pipeline definitions from configuration
   - Manages pipeline scheduling and configuration
   - Handles cluster and notification settings

4. **Generated Notebooks** (`src/notebooks/generated/`)
   - Static, DLT-compatible notebooks
   - One notebook per pipeline group
   - Bronze and silver operations in single pipeline

## 📁 Project Structure

```
autloader-framework-pydab/
├── config/
│   └── unified_pipeline_config.tsv          # Unified configuration
├── resources/
│   ├── notebook_generator.py                # Static notebook generator
│   └── unified_pipeline_generator.py        # DLT pipeline generator
├── src/notebooks/
│   └── generated/                           # Generated DLT notebooks
│       ├── unified_product_pipeline.py
│       ├── unified_customer_pipeline.py
│       └── ...
├── docs/
│   └── DEVELOPER.md                         # Developer documentation
├── databricks.yml                           # Bundle configuration
└── README.md
```

## ⚙️ Configuration

### Configuration Schema

The `unified_pipeline_config.tsv` file defines:

| Column | Description | Example |
|--------|-------------|---------|
| `operation_type` | Layer type (bronze/silver) | `bronze`, `silver` |
| `pipeline_group` | Business domain grouping | `product_pipeline` |
| `source_type` | Source data type | `file`, `table` |
| `file_format` | File format for bronze | `csv`, `json` |
| `source_path` | Source location | `abfss://...` |
| `target_table` | Target table path | `vbdemos.adls_bronze.products_new` |
| `trigger_type` | Pipeline trigger | `time` |
| `schedule` | Quartz cron schedule | `0 0 6 * * ?` |
| `pipeline_config` | JSON configuration | `{"keys": ["product_id"], ...}` |
| `cluster_size` | Cluster configuration | `medium`, `serverless` |
| `notifications` | Email notifications | `{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}` |

### Quartz Cron Scheduling

The framework uses **Quartz cron syntax** for precise scheduling:

- **Daily**: `0 0 6 * * ?` (6:00 AM daily)
- **Weekly**: `0 0 9 ? * 2` (Monday at 9:00 AM)
- **Monthly**: `0 0 12 1 * ?` (1st of month at 12:00 PM)
- **Interval**: `0 0 */20 * * ?` (Every 20 minutes)

### Notification Configuration

The `notifications` column supports email notifications for scheduled jobs:

```json
{
  "on_success": true,
  "on_failure": true,
  "recipients": ["admin@company.com", "data-team@company.com"]
}
```

**Notification Options:**
- **`on_success`**: Send email on successful job completion (boolean)
- **`on_failure`**: Send email on job failure (boolean, defaults to true)
- **`recipients`**: List of email addresses to notify

**Examples:**
```json
// Notify on both success and failure
{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}

// Notify only on failure
{"on_success": false, "on_failure": true, "recipients": ["data-team@company.com"]}

// Multiple recipients
{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com", "product-team@company.com"]}
```

### Pipeline Configuration JSON

#### Bronze Operations
```json
{
  "cloudFiles.schemaLocation": "abfss://.../schema",
  "cloudFiles.checkpointLocation": "abfss://.../checkpoint",
  "cloudFiles.maxFilesPerTrigger": "200",
  "cloudFiles.allowOverwrites": "false",
  "header": "true",
  "inferSchema": "true"
}
```

#### Silver Operations
```json
{
  "keys": ["product_id"],
  "track_history_except_column_list": ["product_name", "category"],
  "stored_as_scd_type": "2",
  "sequence_by": "_ingestion_timestamp"
}
```

## 🚀 Quick Start

### 1. Configure Your Pipelines

Edit `config/unified_pipeline_config.tsv` to define your pipeline groups:

#### Unified Pipeline (Bronze + Silver)
```tsv
bronze	product_pipeline	file	csv	abfss://container@storage.dfs.core.windows.net/data/	vbdemos.adls_bronze.products_new	time	0 0 6 * * ?	{"cloudFiles.schemaLocation": "abfss://...", "cloudFiles.checkpointLocation": "abfss://..."}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
silver	product_pipeline	table		vbdemos.adls_bronze.products_new	vbdemos.adls_silver.products_scd2	time	0 0 6 * * ?	{"keys": ["product_id"], "track_history_except_column_list": ["product_name"], "stored_as_scd_type": "2", "sequence_by": "_ingestion_timestamp"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

#### Silver-Only Pipeline (Existing Bronze Table)
```tsv
silver	existing_customers_pipeline	table		vbdemos.adls_bronze.customers_existing	vbdemos.adls_silver.customers_scd2	time	0 0 6 * * ?	{"keys": ["customer_id"], "track_history_except_column_list": ["first_name", "last_name", "email"], "stored_as_scd_type": "2", "sequence_by": "update_ts"}	medium	{"email_on_success": "true"}
```

### 2. Generate Notebooks

```bash
python resources/notebook_generator.py
```

### 3. Deploy to Databricks

```bash
databricks bundle deploy --profile dev
```

## 🔧 Generated Pipeline Features

### Bronze Layer
- **Auto Loader Integration**: Handles file ingestion with schema inference
- **Audit Columns**: 
  - `_ingestion_timestamp`: Processing timestamp
  - `_filename`: Source filename
  - `_file_path`: Full file path
  - `_file_modification_time`: File modification time
- **CDF Enabled**: Change Data Feed for tracking changes
- **Optimization**: Auto-optimize and auto-compact enabled

### Silver Layer
- **SCD Type 2**: Historical data tracking with start/end timestamps
- **Auto CDC**: Change Data Capture for incremental processing
- **Sequencing**: Uses `_ingestion_timestamp` for proper ordering
- **History Tracking**: Configurable columns for change tracking

## 📊 Example Pipeline

### Product Pipeline Group

1. **Bronze Operation**: Ingests CSV files from Azure Data Lake Storage
2. **Silver Operation**: Creates SCD Type 2 table with product history
3. **Unified Processing**: Both operations run in the same DLT pipeline

```python
# Bronze: File ingestion with audit columns
@dlt.table(name="products_new", table_properties={"delta.enableChangeDataFeed": "true"})
def products_new():
    return spark.readStream.format("cloudFiles")...

# Silver: SCD Type 2 with Auto CDC
dlt.create_streaming_table("products_scd2")
dlt.create_auto_cdc_flow(
    target="products_scd2",
    source="bronze_products_scd2_source",
    keys=["product_id"],
    sequence_by="_ingestion_timestamp",
    stored_as_scd_type="2"
)
```

## 🔄 Silver-Only Pipeline Support

The framework supports creating **silver-only pipelines** for existing bronze tables, making it perfect for:

- **Legacy Data Migration**: Transform existing bronze tables to SCD2
- **External ETL Integration**: Use tables created by other tools or frameworks
- **Incremental Adoption**: Start with silver-only, add bronze operations later
- **Hybrid Approach**: Mix framework-created and external bronze tables

### Silver-Only Configuration Example

```tsv
# Silver-only pipeline for existing bronze table
silver	existing_customers_pipeline	table		vbdemos.adls_bronze.customers_existing	vbdemos.adls_silver.customers_scd2	time	0 0 6 * * ?	{"keys": ["customer_id"], "track_history_except_column_list": ["first_name", "last_name", "email"], "stored_as_scd_type": "2", "sequence_by": "update_ts"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

### How It Works

1. **Existing Bronze Tables**: Your bronze tables can be created by any process (external ETL, manual creation, other frameworks)
2. **Silver Pipeline Only**: Configure just the `silver` operation in the TSV
3. **Source Reference**: Point `source_path` to your existing bronze table
4. **Auto CDC**: The framework creates SCD2 tables using Auto CDC from existing bronze tables
5. **Scheduling**: Runs on your specified cron schedule with proper resource management

### Key Benefits

- **Flexibility**: Works with any bronze table source
- **No Duplication**: Don't recreate bronze tables you already have
- **Incremental**: Can add bronze operations later if needed
- **Consistent**: Same SCD2 logic and scheduling across all pipelines

## 🎯 Benefits

- **Simplified Management**: Single pipeline per business domain
- **Consistent Scheduling**: Uniform timing across bronze and silver operations
- **Audit Compliance**: Complete data lineage and change tracking
- **Performance**: Optimized DLT pipelines with proper metadata handling
- **Maintainability**: Configuration-driven approach with generated code
- **Unity Catalog Ready**: Full compatibility with modern Databricks features
- **Automated Scheduling**: Cron-based jobs with proper resource references
- **Flexible Architecture**: Support for silver-only pipelines from existing bronze tables

## 🔍 Monitoring

- **Pipeline Metrics**: Track upserted and deleted rows
- **CDF Queries**: Query change history using Delta Lake change data feed
- **Audit Trail**: File-level metadata for data lineage
- **Error Handling**: Comprehensive error reporting and notifications
- **Job Scheduling**: Monitor scheduled job execution and pipeline runs

## 📚 Additional Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/data-engineering/delta-live-tables/)
- [Auto Loader Options](https://docs.databricks.com/ingestion/auto-loader/options.html)
- [Change Data Capture with DLT](https://docs.databricks.com/dlt/cdc)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

For developer-specific information, see [docs/DEVELOPER.md](docs/DEVELOPER.md).

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
