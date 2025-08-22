# Unified Framework - Bronze & Silver Layer Pipeline

## 🎯 Overview

This simplified framework combines bronze (ingestion) and silver (transformation) layer pipelines into a single declarative pipeline system using a unified configuration table. The framework automatically groups related operations by `pipeline_group` and creates efficient DLT pipelines.

## 🏗️ Architecture

### Before (Complex)
- Separate bronze and silver configuration files
- Multiple pipeline types and notebooks
- Complex resource generation logic
- Separate deployment processes

### After (Simplified)
- **Single configuration table** (`config/unified_pipeline_config.tsv`)
- **Unified pipeline notebook** (`src/notebooks/unified_pipeline.py`)
- **Grouped by pipeline_group** for efficient processing
- **Single deployment process**

## 📋 Configuration Structure

The unified configuration table contains all necessary information:

| Column | Description | Example |
|--------|-------------|---------|
| `pipeline_type` | Type of operation: `bronze` or `silver` | `bronze` |
| `pipeline_group` | Group name for related operations | `customer_pipeline` |
| `source_type` | Source storage type (bronze only) | `adls` |
| `source_path` | Source file path (bronze only) | `abfss://...` |
| `target_table` | Target table name | `vbdemos.adls_bronze.customers_new` |
| `file_format` | File format (bronze only) | `csv`, `json` |
| `trigger_type` | Trigger mechanism | `file`, `time` |
| `schedule` | Cron schedule for time-based triggers | `0 */10 * * *` |
| `pipeline_config` | **Unified configuration** (JSON) | See examples below |
| `cluster_size` | Compute size | `small`, `medium`, `large`, `serverless` |
| `email_notifications` | Email alerts configuration | JSON format |

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

## 🚀 Key Benefits

### 1. **Flexible Configuration**
- Single TSV file for all pipeline configurations
- Each row represents a single operation (bronze or silver)
- Users can choose which operations to implement

### 2. **Automatic Grouping**
- Related operations are automatically grouped by pipeline_group
- Efficient resource utilization
- Single pipeline per logical group

### 3. **Unified Processing**
- One notebook handles both ingestion and transformation
- Consistent error handling and logging
- Easier maintenance and debugging

### 4. **Flexible Deployment**
- Serverless and traditional cluster support
- Automatic schedule parsing
- Email notification integration

## 🎯 Flexibility Options

### **Bronze + Silver (Full Pipeline)**
- Implement both ingestion and transformation
- Automatic coordination between operations
- Single pipeline handles the complete data flow

### **Bronze Only (Ingestion Only)**
- Implement only file ingestion
- Useful when you don't need SCD Type 2 tracking
- Can add silver operations later by adding new rows

### **Silver Only (Transformation Only)**
- Implement only SCD Type 2 transformation
- Useful when bronze tables already exist
- Can process data from existing ingestion pipelines

### **Mixed Groups**
- Different pipeline groups can have different operation mixes
- Some groups can be bronze+silver, others bronze-only, etc.
- Maximum flexibility for different use cases

## 📁 File Structure

```
autloader-framework-pydab/
├── config/
│   └── unified_pipeline_config.tsv    # 🆕 Single config for all pipelines
├── src/notebooks/
│   └── unified_pipeline.py            # 🆕 Unified pipeline notebook
├── resources/
│   └── unified_pipeline_generator.py  # 🆕 Simplified resource generator
├── databricks.yml                     # Updated to use unified generator
└── UNIFIED_FRAMEWORK_README.md        # This file
```

## 🎯 Usage Examples

### Example 1: Customer Data Pipeline
```tsv
pipeline_type	pipeline_group	source_type	source_path	target_table	file_format	trigger_type	schedule	pipeline_config
bronze	customer_pipeline	adls	abfss://.../customers/	vbdemos.adls_bronze.customers_new	csv	file	0 */5 * * *	{"schema_location": "...", "checkpoint_location": "..."}
silver	customer_pipeline		vbdemos.adls_bronze.customers_new	vbdemos.adls_silver.customers_scd2		time	0 */10 * * *	{"keys": ["customer_id"], "track_history_except_column_list": ["name", "email"]}
```

**Result**: Creates `unified_customer_pipeline` that handles both ingestion and SCD Type 2 transformation.

### Example 2: Order Data Pipeline
```tsv
pipeline_type	pipeline_group	source_type	source_path	target_table	file_format	trigger_type	schedule	pipeline_config
bronze	order_pipeline	adls	abfss://.../orders/	vbdemos.adls_bronze.orders_new	json	file	0 */10 * * *	{"schema_location": "...", "checkpoint_location": "..."}
silver	order_pipeline		vbdemos.adls_bronze.orders_new	vbdemos.adls_silver.orders_scd2		time	0 */15 * * *	{"keys": ["order_id"], "track_history_except_column_list": ["order_status", "total_amount"]}
```

**Result**: Creates `unified_order_pipeline` for order data processing.

### Example 3: Bronze Only Pipeline
```tsv
pipeline_type	pipeline_group	source_type	source_path	target_table	file_format	trigger_type	schedule	pipeline_config	cluster_size	cluster_config	email_notifications
bronze	products_only	adls	abfss://.../products/	vbdemos.adls_bronze.products_new	csv	file	0 */15 * * *	{"schema_location": "...", "checkpoint_location": "..."}	medium		{"email_on_success": "true", "email_on_failure": "true", "recipients": ["admin@company.com"]}
```

**Result**: Creates `unified_products_only` pipeline that handles only file ingestion.

### Example 4: Silver Only Pipeline (from existing bronze table)
```tsv
pipeline_type	pipeline_group	source_type	source_path	target_table	file_format	trigger_type	schedule	pipeline_config	cluster_size	cluster_config	email_notifications
silver	transform_only		vbdemos.adls_bronze.existing_table	vbdemos.adls_silver.transformed_table		time	0 */20 * * *	{"keys": ["id"], "track_history_except_column_list": ["name", "status"]}	medium		{"email_on_success": "true", "email_on_failure": "true", "recipients": ["admin@company.com"]}
```

**Result**: Creates `unified_transform_only` pipeline that handles only SCD Type 2 transformation from an existing bronze table.

## 🚀 Deployment

### 1. **Update Configuration**
Edit `config/unified_pipeline_config.tsv` with your pipeline definitions.

### 2. **Deploy**
```bash
databricks bundle deploy
```

### 3. **Monitor**
- Check Databricks workspace for new unified pipelines
- Monitor pipeline health and performance
- Review logs for any configuration issues

## 🔄 Migration from Old Framework

### Step 1: Consolidate Configurations
- Copy bronze configurations from `ingestion_config.tsv`
- Copy silver configurations from `silver_layer_config.tsv`
- Combine into `unified_pipeline_config.tsv`
- Group related operations by `pipeline_group`

### Step 2: Update Deployment
- Update `databricks.yml` to use `unified_pipeline_generator:load_resources`
- Remove old resource files if no longer needed

### Step 3: Test and Deploy
- Validate configuration syntax
- Deploy using `databricks bundle deploy`
- Verify pipeline functionality

## 🎉 What's Simplified

| Aspect | Before | After |
|--------|--------|-------|
| **Config Files** | 2 separate TSV files | 1 unified TSV file |
| **Pipeline Types** | Multiple notebook types | 1 unified notebook |
| **Resource Generation** | Complex logic for each type | Simple grouped generation |
| **Deployment** | Multiple deployment steps | Single deployment |
| **Maintenance** | Multiple files to update | Single config file |
| **Grouping** | Manual pipeline grouping | Automatic by `pipeline_group` |

## 🚨 Important Notes

### 1. **Pipeline Group Naming**
- Use descriptive names like `customer_pipeline`, `order_pipeline`
- Ensure bronze and silver operations use the same group name
- Group names must be unique across all configurations

### 2. **Configuration Validation**
- JSON in `pipeline_config` must be valid
- Required fields vary by `pipeline_type`
- Bronze: requires `source_path`, `file_format`
- Silver: requires `bronze_table` (in pipeline_config)

### 3. **Cluster Configuration**
- `serverless` pipelines don't require cluster config
- Traditional clusters use size-based configuration
- Email notifications are inherited from the first row in each group

## 🔍 Troubleshooting

### Common Issues

1. **Invalid JSON in pipeline_config**
   - Use JSON validator to check syntax
   - Ensure proper escaping of quotes

2. **Missing Required Fields**
   - Bronze: check `source_path`, `file_format`
   - Silver: check `bronze_table` in pipeline_config

3. **Pipeline Group Mismatch**
   - Ensure bronze and silver rows use identical `pipeline_group` values
   - Check for typos in group names

### Debug Commands

```bash
# Validate configuration syntax
python -c "import pandas as pd; pd.read_csv('config/unified_pipeline_config.tsv', sep='\t')"

# Check deployment status
databricks bundle status

# View deployment logs
databricks bundle logs
```

## 🎯 Next Steps

1. **Customize Configuration**: Update `unified_pipeline_config.tsv` with your data sources
2. **Deploy**: Run `databricks bundle deploy` to create pipelines
3. **Monitor**: Check pipeline health and performance
4. **Extend**: Add more pipeline groups as needed

---

**🎉 Congratulations!** You now have a simplified, unified framework that combines bronze and silver layer operations into efficient, grouped DLT pipelines. The framework automatically handles the complexity while providing a clean, maintainable configuration approach.
