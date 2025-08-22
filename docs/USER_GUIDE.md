# User Guide - Azure Databricks Unified Pipeline Framework

This guide is for users who want to clone this repository and deploy their own unified bronze and silver layer pipelines using Azure Databricks.

## 🎯 Overview

This framework allows you to quickly set up unified data pipelines that handle both bronze (ingestion) and silver (transformation) layer operations using Azure Databricks, Delta Live Tables, and Auto CDC.

### What You'll Build

```
Your ADLS Data → Unified Pipeline → Delta Live Tables → Your Delta Tables
                                    ↓
                              Bronze + Silver Operations
                                    ↓
                              Grouped by Pipeline Group
```

## 📋 Prerequisites

Before you start, ensure you have:

### 1. Azure Databricks Workspace
- **Unity Catalog enabled** workspace
- **Premium or Standard tier** (required for DLT)
- **Workspace admin access** or sufficient permissions

### 2. Azure Data Lake Storage Gen2
- **Storage account** with hierarchical namespace enabled
- **Container** for your data files
- **Service principal** with `Storage Blob Data Contributor` role

### 3. Development Environment
- **Python 3.8+** installed
- **Git** for cloning the repository
- **Text editor** for configuration

## 🚀 Getting Started

### Step 1: Clone and Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/autoloader-framework-pydab.git
   cd autoloader-framework-pydab
   ```

2. **Install Databricks CLI**
   ```bash
   pip install databricks-cli
   ```

3. **Configure authentication**
   ```bash
   databricks configure --profile mycompany
   ```
   
   Enter:
   - **Databricks workspace URL**: `https://adb-xxxxx.xx.azuredatabricks.net`
   - **Personal access token**: Generate from User Settings > Access Tokens

4. **Verify connection**
   ```bash
   databricks workspace list --profile mycompany
   ```

### Step 2: Prepare Your Data

Organize your data in Azure Data Lake Storage:

```
your-container/
├── raw-data/
│   ├── customers/          # CSV files
│   │   ├── customers_001.csv
│   │   └── customers_002.csv
│   ├── orders/             # JSON files
│   │   ├── orders_001.json
│   │   └── orders_002.json
│   └── products/           # CSV files
│       ├── products_001.csv
│       └── products_002.csv
└── checkpoints/            # Auto Loader checkpoints
    ├── customers/
    ├── orders/
    └── products/
```

### Step 3: Configure Your Pipelines

Edit `config/unified_pipeline_config.tsv` to define your data sources and transformations:

**Important**: The unified configuration combines both bronze (ingestion) and silver (transformation) operations. Group related operations using the same `pipeline_group` name.

```tsv
pipeline_type	pipeline_group	source_type	source_path	target_table	file_format	trigger_type	schedule	autoloader_options	cluster_size	cluster_config	email_notifications
dlt	customer_data	adls	abfss://mycontainer@mystorage.dfs.core.windows.net/raw-data/customers/	mycompany.bronze.customers	csv	file	0 */5 * * *	{"header": "true", "schema_location": "abfss://mycontainer@mystorage.dfs.core.windows.net/checkpoints/customers/schema", "checkpoint_location": "abfss://mycontainer@mystorage.dfs.core.windows.net/checkpoints/customers/data", "cloudFiles.maxFilesPerTrigger": "100", "cloudFiles.allowOverwrites": "false"}	medium	{"email_on_success": true, "email_on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}	
dlt	order_data	adls	abfss://mycontainer@mystorage.dfs.core.windows.net/raw-data/orders/	mycompany.bronze.orders	json	time	0 */10 * * *	{"schema_location": "abfss://mycontainer@mystorage.dfs.core.windows.net/checkpoints/orders/schema", "checkpoint_location": "abfss://mycontainer@mystorage.dfs.core.windows.net/checkpoints/orders/data", "cloudFiles.maxFilesPerTrigger": "50"}	large	{"email_on_success": false, "email_on_failure": true, "recipients": ["data-team@company.com"]}	
dlt	product_data	adls	abfss://mycontainer@mystorage.dfs.core.windows.net/raw-data/products/	mycompany.bronze.products	csv	file	0 */15 * * *	{"header": "true", "schema_location": "abfss://mycontainer@mystorage.dfs.core.windows.net/checkpoints/products/schema", "checkpoint_location": "abfss://mycontainer@mystorage.dfs.core.windows.net/checkpoints/products/data", "cloudFiles.maxFilesPerTrigger": "200"}	serverless	{"email_on_success": true, "email_on_failure": true, "recipients": ["admin@company.com", "data-team@company.com", "product-team@company.com"]}	
```

### Step 4: Update Bundle Configuration

Edit `databricks.yml` to match your environment:

```yaml
bundle:
  name: mycompany-autoloader-framework

workspace:
  host: https://adb-xxxxx.xx.azuredatabricks.net  # Your workspace URL

targets:
  dev:
    mode: development
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
  
  prod:
    mode: production
    workspace:
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
```

## ⚙️ Configuration Reference

### TSV Configuration Fields

| Field | Description | Example Values |
|-------|-------------|----------------|
| **pipeline_type** | Type of Databricks resource | `dlt`, `notebook` |
| **pipeline_group** | Logical grouping name | `customer_data`, `sales_data` |
| **source_type** | Data source type | `adls`, `s3`, `unity` |
| **source_path** | Full path to source files | `abfss://container@storage.dfs.core.windows.net/data/` |
| **target_table** | Destination table | `catalog.schema.table` |
| **file_format** | Source file format | `csv`, `json`, `parquet` |
| **trigger_type** | Processing trigger | `file` (event-driven), `time` (scheduled) |
| **schedule** | Cron expression for time triggers | `0 */5 * * *` (every 5 minutes) |
| **autoloader_options** | Auto Loader configuration (JSON) | See examples below |
| **cluster_size** | Compute size | `small`, `medium`, `large`, `serverless` |
| **cluster_config** | Additional cluster config (JSON) | `{}` for defaults |
| **email_notifications** | Email notification config (JSON) | `{}` for no notifications |

### Auto Loader Options Examples

Based on the [official Azure Databricks Auto Loader documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options), here are the correct configuration examples:

#### **Key Auto Loader Options Reference**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| **`cloudFiles.maxFilesPerTrigger`** | String | None | Maximum number of files to process per batch |
| **`cloudFiles.allowOverwrites`** | Boolean | false | Allow file overwrites to be processed again |
| **`cloudFiles.includeExistingFiles`** | Boolean | true | Include existing files on first run |
| **`cloudFiles.inferColumnTypes`** | Boolean | false | Infer exact column types (not just strings) |
| **`cloudFiles.schemaEvolutionMode`** | String | None | Schema evolution: `addNewColumns`, `failOnNewColumns`, `rescue`, `none` |
| **`cloudFiles.rescuedDataColumn`** | String | None | Column name for unparseable data |
| **`cloudFiles.backfillInterval`** | String | None | Trigger backfills at intervals (e.g., "1 day") |
| **`cloudFiles.cleanSource`** | String | OFF | Clean source files: `OFF`, `DELETE`, `MOVE` |
| **`cloudFiles.maxBytesPerTrigger`** | String | None | Maximum bytes to process per batch |

#### **Common Auto Loader Options**

```json
{
  "cloudFiles.maxFilesPerTrigger": "100",
  "cloudFiles.allowOverwrites": "false",
  "cloudFiles.includeExistingFiles": "true",
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaEvolutionMode": "addNewColumns",
  "cloudFiles.rescuedDataColumn": "_rescued_data"
}
```

#### **CSV Files**
```json
{
  "header": "true",           // Use first row as headers (no cloudFiles. prefix)
  "delimiter": ",",           // Field separator
  "encoding": "UTF-8",        // File encoding
  "nullValue": "",            // String to treat as null
  "emptyValue": "",           // String to treat as empty
  "schema_location": "abfss://container@storage.dfs.core.windows.net/checkpoints/schema",
  "checkpoint_location": "abfss://container@storage.dfs.core.windows.net/checkpoints/data",
  "cloudFiles.maxFilesPerTrigger": "100",
  "cloudFiles.allowOverwrites": "false"
}
```

#### **JSON Files**
```json
{
  "multiLine": "true",                    // Handle multi-line JSON (no cloudFiles. prefix)
  "allowBackslashEscaping": "true",       // Allow backslash escaping
  "allowSingleQuotes": "true",            // Allow single quotes
  "allowUnquotedFieldNames": "true",      // Allow unquoted field names
  "schema_location": "abfss://container@storage.dfs.core.windows.net/checkpoints/schema",
  "checkpoint_location": "abfss://container@storage.dfs.core.windows.net/checkpoints/data",
  "cloudFiles.maxFilesPerTrigger": "50"
}
```

#### **Parquet Files**
```json
{
  "mergeSchema": "true",                  // Merge schemas across files (no cloudFiles. prefix)
  "datetimeRebaseMode": "EXCEPTION",      // Handle date rebasing
  "int96RebaseMode": "EXCEPTION",         // Handle INT96 timestamp rebasing
  "schema_location": "abfss://container@storage.dfs.core.windows.net/checkpoints/schema",
  "checkpoint_location": "abfss://container@storage.dfs.core.windows.net/checkpoints/data",
  "cloudFiles.maxFilesPerTrigger": "200"
}
```

#### **Advanced Options**

```json
{
  "cloudFiles.backfillInterval": "1 day",                   // Trigger periodic backfills
  "cloudFiles.cleanSource": "OFF",                          // Clean source files: OFF/DELETE/MOVE
  "cloudFiles.cleanSource.retentionDuration": "30 days",    // Wait time before cleaning
  "cloudFiles.cleanSource.moveDestination": "abfss://container@storage.dfs.core.windows.net/archive/",
  "cloudFiles.maxBytesPerTrigger": "1GB"                    // Max bytes per batch
}
```

#### **Email Notifications**

Configure email notifications for pipeline success and failure events:

```json
{
  "email_on_success": true,                    // Send email on successful completion
  "email_on_failure": true,                    // Send email on failure
  "recipients": [                              // List of email addresses
    "admin@company.com",
    "data-team@company.com",
    "oncall@company.com"
  ]
}
```

**Configuration Options:**
- **`email_on_success`**: Boolean - Send notification when pipeline completes successfully
- **`email_on_failure`**: Boolean - Send notification when pipeline fails
- **`recipients`**: Array of strings - Email addresses to notify

**Example Use Cases:**
- **High Priority**: `{"email_on_success": false, "email_on_failure": true, "recipients": ["admin@company.com"]}`
- **Full Monitoring**: `{"email_on_success": true, "email_on_failure": true, "recipients": ["data-team@company.com"]}`
- **No Notifications**: `{}` (empty or omit the field)

### Cluster Size Options

| Size | Node Type | Workers | Use Case |
|------|-----------|---------|----------|
| **small** | Standard_D2s_v5 | 1-2 | Light workloads, testing |
| **medium** | Standard_D4s_v5 | 1-3 | Standard workloads |
| **large** | Standard_D8s_v5 | 2-5 | Heavy workloads, large files |
| **serverless** | Managed | Auto-scaling | Variable workloads, cost optimization |

### Schedule Examples

| Schedule | Cron Expression | Description |
|----------|----------------|-------------|
| Every 5 minutes | `0 */5 * * *` | High-frequency ingestion |
| Every hour | `0 0 * * *` | Standard batch processing |
| Daily at 2 AM | `0 2 * * *` | Daily data loads |
| Weekdays only | `0 8 * * 1-5` | Business hours processing |

## 🚀 Deployment

### Step 1: Validate Configuration

```bash
# Check your configuration
databricks bundle validate --profile mycompany

# Should show: "✓ Configuration is valid"
```

### Step 2: Deploy to Development

```bash
# Deploy to development environment
databricks bundle deploy --profile mycompany --target dev

# Check deployment status
databricks bundle summary --profile mycompany --target dev
```

### Step 3: Verify Deployment

```bash
# List deployed pipelines
databricks pipelines list --profile mycompany

# Check specific pipeline
databricks pipelines get <pipeline-id> --profile mycompany
```

### Step 4: Deploy to Production

```bash
# Deploy to production environment
databricks bundle deploy --profile mycompany --target prod
```

## 📊 Monitoring Your Pipelines

### Databricks UI

1. **Navigate to Delta Live Tables**
   - Go to your Databricks workspace
   - Click "Delta Live Tables" in the sidebar
   - Find your deployed pipelines

2. **Monitor Pipeline Status**
   - Click on a pipeline name
   - View run history and logs
   - Check data quality metrics

3. **View Data Lineage**
   - Click "Lineage" tab
   - See data flow from source to target
   - Track dependencies

### Command Line Monitoring

```bash
# Get pipeline status
databricks pipelines get <pipeline-id> --profile mycompany

# List recent runs
databricks pipelines list-updates <pipeline-id> --profile mycompany

# Get run details
databricks pipelines get-update <pipeline-id> <update-id> --profile mycompany
```

### Unity Catalog Integration

Your data will be automatically registered in Unity Catalog:

```sql
-- Query your ingested data
SELECT * FROM mycompany.bronze.customers LIMIT 10;

-- Check data freshness
SELECT 
  COUNT(*) as row_count,
  MAX(_ingestion_timestamp) as latest_ingestion
FROM mycompany.bronze.customers;

-- View table history
DESCRIBE HISTORY mycompany.bronze.customers;
```

## 🔧 Common Configurations

### High-Frequency Ingestion

For real-time or near-real-time processing:

```tsv
trigger_type: file
schedule: 
autoloader_options: {"cloudFiles.maxFilesPerTrigger": "10", "cloudFiles.allowOverwrites": "false"}
cluster_size: medium
```

### Batch Processing

For scheduled batch loads:

```tsv
trigger_type: time
schedule: 0 2 * * *
autoloader_options: {"cloudFiles.maxFilesPerTrigger": "1000"}
cluster_size: large
```

### Cost-Optimized Setup

For variable workloads:

```tsv
trigger_type: file
cluster_size: serverless
autoloader_options: {"cloudFiles.maxFilesPerTrigger": "100"}
```

### Large File Processing

For processing large files:

```tsv
cluster_size: large
autoloader_options: {"cloudFiles.maxFilesPerTrigger": "1", "cloudFiles.maxBytesPerTrigger": "1GB"}
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Auto Loader Option Errors

**Error**: `Found unknown option keys: cloudFiles.invalidOption`

**Solution**: Use only valid options from the [official Auto Loader documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options). Common mistakes:
- `cloudFiles.header` → Use `header` (no prefix)
- `cloudFiles.multiline` → Use `multiLine` (correct case)
- `cloudFiles.allowbackslashescaping` → Use `allowBackslashEscaping` (correct case)

**Quick Fix**: Start with minimal options and add complexity gradually:
```json
{
  "cloudFiles.maxFilesPerTrigger": "100",
  "cloudFiles.allowOverwrites": "false"
}
```

#### 2. Authentication Errors

**Error**: `403 Forbidden` or authentication failures

**Solution**:
```bash
# Verify your profile
databricks configure --profile mycompany

# Test connection
databricks workspace list --profile mycompany

# Check token expiration in User Settings > Access Tokens
```

#### 2. Permission Errors

**Error**: `Access denied` on storage or tables

**Solution**:
- Ensure service principal has `Storage Blob Data Contributor` on ADLS
- Verify Unity Catalog permissions: `USE CATALOG`, `CREATE SCHEMA` on target catalog
- Check workspace permissions: `Can Manage` or `Workspace Admin`

#### 3. Schema Evolution Issues

**Error**: Schema mismatch or evolution failures

**Solution**:
```json
{
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaEvolutionMode": "addNewColumns",
  "cloudFiles.mergeSchema": "true"
}
```

#### 4. File Format Issues

**CSV Headers Not Recognized**:
```json
{
  "cloudFiles.header": "true",
  "cloudFiles.inferSchema": "true"
}
```

**JSON Parsing Errors**:
```json
{
  "cloudFiles.multiLine": "true",
  "cloudFiles.allowBackslashEscaping": "true",
  "cloudFiles.allowSingleQuotes": "true"
}
```

#### 5. Performance Issues

**Slow Processing**:
- Increase `cloudFiles.maxFilesPerTrigger`
- Use larger cluster size
- Consider serverless for variable workloads

**High Costs**:
- Reduce `cloudFiles.maxFilesPerTrigger`
- Use smaller cluster size
- Switch to serverless compute

### Debug Commands

```bash
# Validate bundle configuration
databricks bundle validate --profile mycompany

# Check deployed resources
databricks bundle summary --profile mycompany

# View pipeline configuration
databricks pipelines get <pipeline-id> --profile mycompany

# Check recent pipeline runs
databricks pipelines list-updates <pipeline-id> --profile mycompany

# View workspace files
databricks workspace list /path/to/bundle --profile mycompany
```

### Log Analysis

1. **Pipeline Logs**
   - Go to DLT UI → Your Pipeline → Latest Run
   - Click "View Logs" for detailed error messages
   - Look for Auto Loader specific errors

2. **Common Log Patterns**
   ```
   # Authentication issues
   "403 Forbidden" or "Access denied"
   
   # Schema issues  
   "Schema mismatch" or "Column not found"
   
   # File format issues
   "Malformed CSV" or "Invalid JSON"
   
   # Performance issues
   "Task timeout" or "Out of memory"
   ```

## 💡 Best Practices

### 1. Auto Loader Configuration

- **Start Simple**: Begin with minimal options and add complexity gradually
- **Validate Options**: Always refer to the [official Auto Loader options documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options)
- **Test Incrementally**: Test each new option before adding more
- **Monitor Performance**: Use `cloudFiles.maxFilesPerTrigger` to control batch sizes
- **Handle Schema Evolution**: Use `cloudFiles.schemaEvolutionMode` for changing data structures
- **Use File Cleanup**: Configure `cloudFiles.cleanSource` for production environments
- **Leverage Azure Integration**: Use Azure-specific options for better ADLS performance

### 2. Data Organization

```
container/
├── raw/                    # Raw source data
│   ├── domain1/
│   └── domain2/
└── checkpoints/            # Auto Loader state
    ├── domain1/
    └── domain2/
```

### 2. Naming Conventions

- **Pipeline Groups**: Use domain-based naming (`customer_data`, `sales_data`)
- **Target Tables**: Follow `catalog.schema.table` pattern (`mycompany.bronze.customers`)
- **Paths**: Use consistent folder structure

### 3. Environment Management

```yaml
# Use different targets for environments
targets:
  dev:
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/dev
  
  prod:
    workspace:
      root_path: /Workspace/Shared/.bundle/${bundle.name}/prod
```

### 4. Security

- Use **service principals** instead of personal tokens for production
- Store secrets in **Azure Key Vault** and reference in Databricks
- Follow **principle of least privilege** for permissions
- Enable **audit logging** for compliance

### 5. Performance Optimization

- Start with **smaller clusters** and scale up based on monitoring
- Use **serverless** for variable workloads
- Tune `maxFilesPerTrigger` based on file sizes and frequency
- Monitor **data freshness** and **processing latency**

## 📞 Getting Help

### Documentation
- **[Auto Loader Overview](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)** - Complete Auto Loader documentation
- **[Auto Loader Options Reference](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options)** - Complete list of all Auto Loader options
- **[Common Auto Loader Options](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#common-auto-loader-options)** - Core configuration options
- **[File Format Options](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#file-format-options)** - Format-specific configuration
- **[Azure-Specific Options](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#azure-specific-options)** - Azure ADLS integration options
- **[Delta Live Tables Guide](https://docs.databricks.com/azure/en/delta-live-tables/index.html)**
- **[Unity Catalog Documentation](https://docs.databricks.com/azure/en/data-governance/unity-catalog/index.html)**

### Community Support
- **[Databricks Community](https://community.databricks.com/)** - Ask questions and share experiences
- **[GitHub Issues](https://github.com/your-org/autoloader-framework-pydab/issues)** - Report framework-specific issues
- **[Azure Databricks Documentation](https://docs.databricks.com/azure/)** - Official documentation

### Professional Support
- **Databricks Support** - For platform-specific issues
- **Azure Support** - For Azure integration issues
- **Partner Solutions** - For implementation assistance

---

**Happy data engineering! 🚀**
