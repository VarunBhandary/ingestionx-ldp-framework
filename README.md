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
- **Audit Trail**: Comprehensive file metadata tracking including filename, path, and modification time

## 🏗️ Architecture

The framework uses a **unified pipeline approach** where each `pipeline_group` contains:
- **Bronze Layer**: File ingestion using Auto Loader with audit columns
- **Silver Layer**: SCD Type 2 transformations using Auto CDC
- **Unified Configuration**: Single config row per operation type (bronze/silver)

### Key Components

1. **Configuration Layer** (`config/unified_pipeline_config.tsv`)
   - Pipeline grouping by business domain
   - Bronze and silver operation definitions
   - Unified configuration for both layers

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
| `schedule` | Cron schedule | `0 */10 * * *` |
| `pipeline_config` | JSON configuration | `{"keys": ["product_id"], ...}` |
| `cluster_size` | Cluster configuration | `medium` |
| `notifications` | Email notifications | `{"email_on_success": "true"}` |

### Pipeline Configuration JSON

#### Bronze Operations
```json
{
  "schema_location": "abfss://.../schema",
  "checkpoint_location": "abfss://.../checkpoint",
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

```tsv
bronze	product_pipeline	file	csv	abfss://container@storage.dfs.core.windows.net/data/	vbdemos.adls_bronze.products_new	time	0 */10 * * *	{"schema_location": "abfss://...", "checkpoint_location": "abfss://..."}	medium	{"email_on_success": "true"}
silver	product_pipeline	table		vbdemos.adls_bronze.products_new	vbdemos.adls_silver.products_scd2	time	0 */10 * * * *	{"keys": ["product_id"], "track_history_except_column_list": ["product_name"], "stored_as_scd_type": "2", "sequence_by": "_ingestion_timestamp"}	medium	{"email_on_success": "true"}
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

## 🎯 Benefits

- **Simplified Management**: Single pipeline per business domain
- **Consistent Scheduling**: Uniform timing across bronze and silver operations
- **Audit Compliance**: Complete data lineage and change tracking
- **Performance**: Optimized DLT pipelines with proper metadata handling
- **Maintainability**: Configuration-driven approach with generated code
- **Unity Catalog Ready**: Full compatibility with modern Databricks features

## 🔍 Monitoring

- **Pipeline Metrics**: Track upserted and deleted rows
- **CDF Queries**: Query change history using Delta Lake change data feed
- **Audit Trail**: File-level metadata for data lineage
- **Error Handling**: Comprehensive error reporting and notifications

## 📚 Additional Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/data-engineering/delta-live-tables/)
- [Auto Loader Options](https://docs.databricks.com/ingestion/auto-loader/options.html)
- [Change Data Capture with DLT](https://docs.databricks.com/dlt/cdc)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
