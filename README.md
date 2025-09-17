# Auto Loader Framework with PyDAB

A unified pipeline framework for Databricks that supports both DLT pipelines and manual notebook-based jobs, featuring comprehensive Auto Loader demos showcasing real-world data ingestion scenarios.

## 🚀 **Framework Overview**

This framework provides a unified approach to data pipeline management in Databricks, supporting multiple operation types:

- **`bronze`**: File ingestion operations using [Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options)
- **`silver`**: SCD Type 2 transformations with Auto CDC
- **`gold`**: Analytics and reporting operations
- **`manual`**: Custom notebook-based jobs with full control

### **📋 Prerequisites**

- **Python**: 3.10 or above (see `pyproject.toml` for exact requirements)
- **Databricks Runtime**: 16.4 LTS or above (required for `cloudFiles.cleanSource` functionality)
- **Unity Catalog**: Enabled workspace with appropriate permissions
- **Volumes**: Access to create and manage volumes in your catalog/schema
- **Package Manager**: `uv` (recommended) or `pip`

## 🎯 **Auto Loader Demo Scenarios**

The framework includes comprehensive demo scenarios that showcase real-world Auto Loader use cases:

### **1. Customer Data Demo (Fixed Schema + SCD Type 2)**
- **Scenario**: Incremental customer data updates with full history tracking
- **Format**: CSV files with incremental changes
- **Features**: 
  - **Fixed schema enforcement** with inline schema definition in TSV config
  - **Corrupt data handling** with `cloudFiles.rescuedDataColumn`
  - SCD Type 2 implementation in silver layer
  - Historical change tracking for customer attributes
- **Auto Loader Options**: Fixed schema, rescued data column, checkpoint location, max files per trigger

### **2. Transaction Data Demo (SCD Type 1)**
- **Scenario**: Transaction data with soft delete flags
- **Format**: CSV files with transaction records
- **Features**:
  - Soft delete handling with `is_deleted` flag
  - SCD Type 1 implementation (overwrite changes)
  - Real-time transaction processing
- **Auto Loader Options**: Schema inference, checkpoint management

### **3. Inventory Data Demo (Fixed Schema + Archive Management)**
- **Scenario**: Daily inventory files with fixed schema and archive management
- **Format**: CSV files with strict schema requirements
- **Features**:
  - **Fixed schema enforcement** with inline schema definition in TSV config
  - **File archiving** using `cloudFiles.cleanSource: move` (moves files 30 days after processing)
  - **Corrupt data handling** with `cloudFiles.rescuedDataColumn`
  - **Schema validation** to ensure data quality
  - **Single file processing** with `maxFilesPerTrigger: 1` for controlled batch processing
- **Auto Loader Options**: Fixed schema, clean source move, rescued data column, schema validation

### **4. Shipment Data Demo (File Notification Mode)**
- **Scenario**: Real-time shipment tracking with evolving schema
- **Format**: JSON files with nested objects and dynamic structure
- **Features**:
  - **File notification mode** using `cloudFiles.useManagedFileEvents: true`
  - **Schema evolution** with `cloudFiles.schemaEvolutionMode: rescue`
  - **Multiline JSON support** with `multiline: true` for nested objects
  - **Real-time processing** triggered by file arrivals
  - **Schema flexibility** to handle new fields and nested structures
- **Auto Loader Options**: Managed file events, schema evolution mode, multiline JSON

### **5. Manual Pipeline Demo (Custom Notebooks)**
- **Scenario**: Custom notebook-based jobs with full control and dependencies
- **Format**: Python notebooks with parameter passing
- **Features**:
  - **Order-based execution** using `{"order": 1, 2, 3}` for sequencing
  - **Dependency management** with `{"depends_on": 1}` for parallel steps
  - **Parameter passing** for pipeline group, operation type, and execution context
  - **Custom logic** without DLT dependencies
- **Use Cases**: Custom transformations, external API calls, complex business logic

### **6. Silver-Only Pipeline Demo (DLT without Bronze)**
- **Scenario**: Silver layer processing from existing bronze tables
- **Format**: DLT pipeline reading from pre-existing bronze tables
- **Features**:
  - **SCD Type 2 processing** with full history tracking
  - **External bronze table** integration (e.g., from other systems)
  - **Independent execution** without bronze layer dependencies
  - **Same DLT capabilities** as full bronze+silver pipelines
- **Use Cases**: Data warehouse migrations, external system integration, bronze table reuse

### **7. Product Catalog CDC Demo (Change Data Capture with Custom Expressions)**
- **Scenario**: CDC data with create, updated, deleted timestamps
- **Format**: CSV files with CDC timestamp columns
- **Features**:
  - **CDC timestamp handling** with `created_at`, `updated_at`, `deleted_at` columns
  - **Custom expressions** for CDC operation detection in bronze layer
  - **Business column mapping** in silver layer (technical to business names)
  - **SCD Type 2 processing** with CDC-aware AUTO CDC
  - **Soft delete support** with proper timestamp handling
- **Auto Loader Options**: Fixed schema, custom expressions, checkpoint location

## 📁 **Demo Directory Structure**

The framework uses a clean, organized directory structure following [Auto Loader best practices](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options):

```
/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/
├── schema/                    # Schema tracking directories
│   ├── customers/            # Customer schema evolution
│   ├── transactions/         # Transaction schema evolution
│   ├── inventory/            # Inventory fixed schema
│   └── shipments/            # Shipment schema evolution
├── checkpoint/               # Auto Loader state tracking
│   ├── customers/           # Customer processing state
│   ├── transactions/        # Transaction processing state
│   ├── inventory/           # Inventory processing state
│   └── shipments/           # Shipment processing state
├── archive/                  # Archived processed files
│   └── inventory/           # Inventory archive location
└── [data folders]/          # Raw data files
    ├── customers/           # Customer CSV files
    ├── transactions/        # Transaction CSV files
    ├── inventory/           # Inventory CSV files
    └── shipments/           # Shipment JSON files
```

### **Configuration Variables**
- **`{CATALOG}`**: Your Unity Catalog catalog name (e.g., `vbdemos`, `main`, `production`)
- **`{SCHEMA}`**: Your Unity Catalog schema name (e.g., `dbdemos_autoloader`, `autoloader_demo`)
- **`{VOLUME_NAME}`**: Your volume name (e.g., `raw_data`, `landing_zone`)

**Example**: If you configure `CATALOG=main`, `SCHEMA=autoloader_demo`, `VOLUME_NAME=landing_zone`, your paths will be:
```
/Volumes/main/autoloader_demo/landing_zone/
```

## 🏗️ **Architecture**

### **Pipeline Types**

#### **1. DLT Pipelines (bronze + silver)**
- **Automatic generation**: Framework creates unified DLT notebooks
- **Autoloader integration**: File ingestion with schema evolution
- **SCD Type 2**: Automatic change data capture and history tracking
- **Streaming tables**: Real-time data processing capabilities

#### **2. Manual Jobs (manual operation type)**
- **Full control**: Write notebooks exactly how you want
- **No DLT dependencies**: Skip pipeline creation entirely
- **Flexible chaining**: Any number of steps, any execution order
- **Framework integration**: Leverage scheduling, notifications, cluster config

### **Folder Structure**
```
src/notebooks/
├── generated/          # Framework-generated notebooks (DLT pipelines)
└── manual/            # Manual notebook examples and custom notebooks
    ├── README.md      # Comprehensive documentation and examples
    ├── manual_test_step1.py
    ├── manual_test_step2.py
    └── manual_test_step3.py
```

## 🔧 **Configuration**

### **TSV Configuration Format**
```tsv
operation_type	pipeline_group	source_type	file_format	source_path	target_table	trigger_type	schedule	pipeline_config	cluster_size	notifications	custom_expr	parameters
```

### **Parameters Support for Manual Operations**

The framework supports **user-defined parameters** via the `parameters` column for manual operations, enabling dynamic configuration of notebook behavior:

#### **Parameter Features**
- **Variable Support**: Use the same variable syntax as the rest of the framework (`${var.catalog_name}`, `${var.schema_name}`, `${var.volume_name}`)
- **Type Safety**: Supports string, number, and boolean parameter types
- **JSON Format**: Parameters are defined as JSON objects in the TSV configuration
- **Default Values**: Notebooks can provide default values for parameters
- **Environment Flexibility**: Parameters can be different per environment (dev/staging/prod)

#### **Parameter Configuration**
```tsv
manual	my_pipeline	notebook		src/notebooks/manual/my_step1.py	output_table1	time	0 0 9 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/my_step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}	{"batch_size": 1000, "debug_mode": true, "catalog": "${var.catalog_name}", "schema": "${var.schema_name}"}
```

#### **Notebook Usage**
```python
# Access user-defined parameters with defaults
batch_size = dbutils.widgets.get("batch_size", "500")
debug_mode = dbutils.widgets.get("debug_mode", "false").lower() == "true"
catalog = dbutils.widgets.get("catalog", "default")
schema = dbutils.widgets.get("schema", "default")

# Use parameters in processing logic
if debug_mode:
    print(f"Processing batch size: {batch_size}")
    print(f"Target catalog: {catalog}")
```

### **Custom Expressions Support**

The framework supports **custom expressions** via the `custom_expr` column for both bronze and silver operations:

#### **Bronze Layer Custom Expressions**
- **CDC Operation Detection**: Convert timestamps to CDC operations
- **Data Preprocessing**: Add computed columns, filters, transformations
- **Audit Columns**: Add ingestion timestamps and metadata

**Example**:
```tsv
bronze	product_catalog_cdc_pipeline	file	csv	/Volumes/.../product_catalog_cdc	vbdemos.adls_bronze.product_catalog_cdc	time	0 0 6 * * ?	{"schema": {...}}	medium	{"on_success": true}	"CASE WHEN deleted_at IS NOT NULL THEN 'DELETE' WHEN created_at = updated_at THEN 'INSERT' ELSE 'UPDATE' END as cdc_operation, COALESCE(updated_at, created_at) as sequence_ts, current_timestamp() as _ingestion_timestamp"	{}
```

#### **Silver Layer Custom Expressions**
- **Business Column Mapping**: Map technical to business-friendly names
- **Data Enrichment**: Add computed fields and transformations
- **Column Selection**: Choose specific columns for processing

**Example**:
```tsv
silver	product_catalog_cdc_pipeline	table		vbdemos.adls_bronze.product_catalog_cdc	vbdemos.adls_silver.product_catalog_cdc_scd2	time	0 0 6 * * ?	{"keys": ["product_id"], "stored_as_scd_type": "2", "sequence_by": "sequence_ts"}	medium	{"on_success": true}	"product_id, product_name as product_title, category as product_category, price as unit_price, description as product_description, status as product_status, created_at as effective_start_date, updated_at as last_modified_date, deleted_at as soft_delete_date, cdc_operation, sequence_ts"	{}
```

### **Operation Types**

#### **Bronze Operations**
```tsv
bronze	customer_pipeline	file	csv	abfss://path/to/source	vbdemos.adls_bronze.customers	time	0 0 6 * * ?	{"cloudFiles.schemaLocation": "...", "cloudFiles.checkpointLocation": "..."}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}
```

#### **Silver Operations**
```tsv
silver	customer_pipeline	table		vbdemos.adls_bronze.customers	vbdemos.adls_silver.customers_scd2	time	0 0 6 * * ?	{"keys": ["customer_id"], "track_history_except_column_list": ["name", "email"], "stored_as_scd_type": "2"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}
```

#### **Gold Operations**
```tsv
gold	customer_pipeline	notebook		src/notebooks/generated/gold_analytics.py	vbdemos.adls_gold.customer_analytics	time	0 0 6 * * ?	{"notebook_path": "src/notebooks/generated/gold_analytics.py"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}
```

#### **Manual Operations**
```tsv
# Step 1: Data preparation with parameters
manual	my_pipeline	notebook		src/notebooks/manual/my_step1.py	output_table1	time	0 0 9 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/my_step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}	{"batch_size": 1000, "debug_mode": true, "catalog": "${var.catalog_name}"}

# Step 2: Data transformation with parameters
manual	my_pipeline	notebook		src/notebooks/manual/my_step2.py	output_table2	time	0 0 9 * * ?	{"order": 2, "notebook_path": "src/notebooks/manual/my_step2.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}	{"environment": "production", "retry_count": 3, "volume": "${var.volume_name}"}
```

## ⚡ **Compute Options**

### **Cluster Sizes**

| Size | Use Case | Performance | Cost | Control |
|------|----------|-------------|------|---------|
| **serverless** | Production, variable workloads | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **small** | Development, testing, light workloads | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **medium** | Balanced workloads, team development | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **large** | Heavy workloads, ML, high throughput | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |

### **When to Use Each**

- **🚀 Serverless**: Best performance, automatic optimization, production workloads
- **🔧 Small**: Cost-sensitive, development, non-critical jobs
- **⚖️ Medium**: Balanced workloads, standard ETL, team development
- **🚀 Large**: Heavy processing, ML workloads, performance-critical jobs

## 📋 **Usage Examples**

### **1. DLT Pipeline (Bronze + Silver + Gold)**
```tsv
# Bronze: File ingestion
bronze	product_pipeline	file	csv	abfss://path/to/products	vbdemos.adls_bronze.products	time	0 0 */20 * * ?	{"cloudFiles.schemaLocation": "...", "cloudFiles.checkpointLocation": "..."}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}

# Silver: SCD Type 2 transformation
silver	product_pipeline	table		vbdemos.adls_bronze.products	vbdemos.adls_silver.products_scd2	time	0 0 */20 * * ?	{"keys": ["product_id"], "track_history_except_column_list": ["name", "price"], "stored_as_scd_type": "2"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}

# Gold: Analytics
gold	product_pipeline	notebook		src/notebooks/generated/gold_analytics.py	vbdemos.adls_gold.product_analytics	time	0 0 */20 * * ?	{"notebook_path": "src/notebooks/generated/gold_analytics.py"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}		{}
```

### **2. Manual Pipeline (Custom Notebooks with Parameters)**
```tsv
# Step 1: Data preparation with parameters
manual	custom_pipeline	notebook		src/notebooks/manual/step1.py	table1	time	0 0 8 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}	{"batch_size": 1000, "debug_mode": true, "catalog": "${var.catalog_name}"}

# Step 2: Data transformation with parameters
manual	custom_pipeline	notebook		src/notebooks/manual/step2.py	table2	time	0 0 8 * * ?	{"order": 2, "notebook_path": "src/notebooks/manual/step2.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}	{"environment": "production", "retry_count": 3, "volume": "${var.volume_name}"}

# Step 3: Final analytics with parameters
manual	custom_pipeline	notebook		src/notebooks/manual/step3.py	table3	time	0 0 8 * * ?	{"order": 3, "notebook_path": "src/notebooks/manual/step3.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}	{"analytics_mode": "advanced", "report_format": "json", "catalog": "${var.catalog_name}", "schema": "${var.schema_name}"}
```

## ⚙️ **Configuration Setup**

### **Step 1: Configure Your Environment**

Before running the demo, you need to configure your Unity Catalog settings in two places:

#### **1.1 Update TSV Configuration**
Edit `config/unified_pipeline_config.tsv` and replace the placeholder values:

```tsv
# Replace these placeholder values with your actual Unity Catalog settings:
# {CATALOG} -> your_catalog_name
# {SCHEMA} -> your_schema_name  
# {VOLUME_NAME} -> your_volume_name

# Example: If your catalog is "main", schema is "autoloader_demo", volume is "raw_data"
# Change: /Volumes/vbdemos/dbdemos_autoloader/raw_data/
# To:     /Volumes/main/autoloader_demo/raw_data/
```

**Key paths to update in TSV:**
- `source_path`: `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/[data_folder]`
- `cloudFiles.schemaLocation`: `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/schema/[demo_name]`
- `cloudFiles.checkpointLocation`: `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/checkpoint/[demo_name]`
- `cloudFiles.archiveLocation`: `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/archive/[demo_name]`

#### **1.2 Update Data Generation Notebook**
Edit `src/notebooks/manual/data_generation_demo.py` and update these variables:

```python
# Configuration - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
CATALOG = "your_catalog_name"        # e.g., "main", "vbdemos", "production"
SCHEMA = "your_schema_name"          # e.g., "autoloader_demo", "dbdemos_autoloader"
VOLUME_NAME = "your_volume_name"     # e.g., "raw_data", "landing_zone"
```

### **Step 2: Verify Your Configuration**

After updating both files, verify your configuration:

```bash
# Check that all paths in TSV match your Unity Catalog setup
grep -E "Volumes.*CATALOG|Volumes.*SCHEMA|Volumes.*VOLUME" config/unified_pipeline_config.tsv

# Verify data generation notebook has correct variables
grep -E "CATALOG =|SCHEMA =|VOLUME_NAME =" src/notebooks/manual/data_generation_demo.py
```

### **Step 3: Unity Catalog Prerequisites**

Ensure you have the following in your Databricks workspace:

1. **Unity Catalog enabled**
2. **Permissions to create**:
   - Catalogs (or use existing catalog)
   - Schemas (or use existing schema)
   - Volumes
3. **Access to**:
   - Create tables in bronze/silver/gold schemas
   - Run DLT pipelines
   - Execute notebooks

### **Configuration Examples**

#### **Example 1: Using Main Catalog**
```python
# In data_generation_demo.py
CATALOG = "main"
SCHEMA = "autoloader_demo"
VOLUME_NAME = "raw_data"
```

```tsv
# In unified_pipeline_config.tsv
source_path: /Volumes/main/autoloader_demo/raw_data/customers
cloudFiles.schemaLocation: /Volumes/main/autoloader_demo/raw_data/schema/customers
cloudFiles.checkpointLocation: /Volumes/main/autoloader_demo/raw_data/checkpoint/customers
```

#### **Example 2: Using Production Environment**
```python
# In data_generation_demo.py
CATALOG = "production"
SCHEMA = "data_engineering"
VOLUME_NAME = "landing_zone"
```

```tsv
# In unified_pipeline_config.tsv
source_path: /Volumes/production/data_engineering/landing_zone/customers
cloudFiles.schemaLocation: /Volumes/production/data_engineering/landing_zone/schema/customers
cloudFiles.checkpointLocation: /Volumes/production/data_engineering/landing_zone/checkpoint/customers
```

#### **Example 3: Using Custom Names**
```python
# In data_generation_demo.py
CATALOG = "my_company"
SCHEMA = "analytics"
VOLUME_NAME = "data_lake"
```

```tsv
# In unified_pipeline_config.tsv
source_path: /Volumes/my_company/analytics/data_lake/customers
cloudFiles.schemaLocation: /Volumes/my_company/analytics/data_lake/schema/customers
cloudFiles.checkpointLocation: /Volumes/my_company/analytics/data_lake/checkpoint/customers
```

## 🚀 **Getting Started**

### **1. Setup**
```bash
# Clone the repository
git clone <repository-url>
cd autloader-framework-pydab

# Install dependencies using uv (recommended)
uv sync

# Or install dependencies using pip
pip install -e .
```

### **Package Management**
This project uses **uv** as the primary package manager for fast, reliable dependency management:

```bash
# Install all dependencies
uv sync

# Install with development dependencies (includes pyspark, pytest, black, flake8)
uv sync --group dev

# Add new dependencies
uv add package-name

# Add development dependencies
uv add --group dev package-name

# Run commands in the virtual environment
uv run python script.py
uv run pytest
uv run databricks bundle validate --profile dev
```

### **Development Setup**
```bash
# Install with development dependencies using uv (recommended)
uv sync --group dev

# Or install with development dependencies using pip
pip install -e ".[dev]"

# Run all tests
uv run pytest

# Run specific test files
uv run pytest tests/test_integration.py
uv run pytest tests/test_config_parser.py

# Run tests with verbose output
uv run pytest -v

# Run code formatting
uv run black .

# Run linting
uv run flake8 .

# Run standalone configuration validation
uv run python src/utils/validate_config.py --summary
```

### **2. Configuration**
1. **Edit** `config/unified_pipeline_config.tsv` (see Configuration Setup above)
2. **Edit** `src/notebooks/manual/data_generation_demo.py` (see Configuration Setup above)
3. **Choose** appropriate cluster sizes
4. **Set** schedules and notifications

### **3. Notebooks**
- **DLT pipelines**: Framework generates automatically during bundle operations
- **Manual jobs**: Create notebooks in `src/notebooks/manual/`
- **Note**: DLT notebooks are auto-generated when you run `databricks bundle validate` or `databricks bundle deploy`

### **4. Deployment**
```bash
# Validate configuration
databricks bundle validate --profile dev

# Deploy to Databricks
databricks bundle deploy --profile dev

# Run jobs manually
databricks bundle run <job_name> --profile dev
```

### **5. Manual Notebook Generation (Optional)**
If you need to generate notebooks independently (e.g., for development or testing):
```bash
# Generate notebooks manually
uv run python resources/notebook_generator.py

# This will create notebooks in src/notebooks/generated/
# Note: This is usually not needed as notebooks are auto-generated during bundle operations
```

## 🎮 **Running the Auto Loader Demo**

### **Prerequisites**
- Databricks workspace with Unity Catalog enabled
- Access to create volumes and schemas
- Databricks Asset Bundle (DAB) configured

### **Step 1: Deploy the Framework**
```bash
# Deploy all demo pipelines to Databricks
databricks bundle deploy --profile dev
```

### **Step 2: Run Data Generation Notebook**
1. **Open** the data generation notebook: `src/notebooks/manual/data_generation_demo.py`
2. **Execute** the notebook in your Databricks workspace
3. **Follow** the step-by-step instructions to generate sample data

The notebook will:
- ✅ Create the volume and directory structure
- ✅ Generate sample data for all 4 demo scenarios
- ✅ Create fixed schema files for inventory demo
- ✅ Set up proper Auto Loader configurations

### **Step 3: Start the Demo Pipelines**
```bash
# Start all demo pipelines
databricks bundle run customer_demo_pipeline --profile dev
databricks bundle run transaction_demo_pipeline --profile dev
databricks bundle run inventory_demo_pipeline --profile dev
databricks bundle run shipment_demo_pipeline --profile dev
```

### **Step 4: Monitor and Test**
1. **Monitor** pipeline execution in Databricks UI
2. **Generate** additional data files to test incremental processing
3. **Observe** schema evolution and archive functionality
4. **Test** different scenarios (corrupt data, schema changes, etc.)

## 🔧 **Auto Loader Options Reference**

The demo showcases key [Auto Loader options](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options) from the Microsoft documentation:

### **Common Options Used in Demo**

| Option | Purpose | Demo Usage | Example Path |
|--------|---------|------------|--------------|
| `cloudFiles.checkpointLocation` | Processing state tracking | All demos - separate checkpoint directories | `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/checkpoint/{demo}` |
| `cloudFiles.maxFilesPerTrigger` | Batch size control | Customer: 100, Transaction: 50, Inventory: 1, Shipment: 100 | N/A |
| `cloudFiles.allowOverwrites` | File overwrite handling | All demos: false (safety) | N/A |
| `cloudFiles.cleanSource` | File archiving after processing | Inventory demo: "move" (30 days after processing) | N/A |
| `cloudFiles.cleanSource.moveDestination` | Archive destination | Inventory demo | `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/archive/inventory` |
| `cloudFiles.cleanSource.retentionDuration` | Archive retention duration | Configurable (minimum 7 days, default 30 days) | N/A |
| `cloudFiles.rescuedDataColumn` | Corrupt data handling | Customer, Inventory demos: "corrupt_data" | N/A |
| `cloudFiles.useManagedFileEvents` | File notification mode | Shipment demo only | N/A |
| `cloudFiles.schemaEvolutionMode` | Schema change handling | Shipment demo: "rescue" | N/A |
| `multiline` | JSON multiline support | Shipment demo: true (for nested objects) | N/A |
| `cloudFiles.validateOptions` | Option validation | All demos: false (temporary) | N/A |
| `.schema(schema)` | Fixed schema enforcement | Customer, Inventory demos | Inline schema definition |

**Note**: `cloudFiles.cleanSource` requires Databricks Runtime 16.4 LTS or above.

### **File Format Options**

| Format | Demo | Options Used |
|--------|------|--------------|
| **CSV** | Customer, Inventory | `header: true`, `.schema(schema)` (fixed schema) |
| **CSV** | Transaction | `header: true`, `inferSchema: true` |
| **JSON** | Shipment | `multiline: true`, `cloudFiles.schemaEvolutionMode: rescue` |

### **Schema Management Strategies**

| Strategy | Demo | Implementation | Benefits |
|----------|------|----------------|----------|
| **Fixed Schema** | Customer, Inventory | Inline schema in TSV config + `.schema(schema)` | Data validation, corrupt data handling, type safety |
| **Schema Inference** | Transaction | `cloudFiles.inferSchema: true` | Automatic schema detection, flexible data structure |
| **Schema Evolution** | Shipment | `cloudFiles.schemaEvolutionMode: rescue` | Dynamic schema changes, new field handling |

## 📋 **Schema Definition Guide**

### **Fixed Schema Configuration**

For demos requiring data validation and corrupt data handling, schemas are defined inline in the TSV configuration:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {"name": "product_id", "type": "string", "nullable": false},
      {"name": "product_name", "type": "string", "nullable": true},
      {"name": "quantity_on_hand", "type": "integer", "nullable": true},
      {"name": "unit_price", "type": "double", "nullable": true},
      {"name": "last_restocked", "type": "date", "nullable": true}
    ]
  }
}
```

### **Schema Definition Best Practices**

1. **Field Naming**: Use descriptive, consistent field names (snake_case recommended)
2. **Data Types**: Choose appropriate PySpark types (`string`, `integer`, `double`, `date`, `timestamp`)
3. **Nullable Fields**: Set `nullable: false` for required fields, `true` for optional fields
4. **Type Safety**: Fixed schemas provide compile-time validation and prevent data quality issues
5. **Corrupt Data Handling**: Use `cloudFiles.rescuedDataColumn` to capture mismatched data

### **Supported Data Types**

| JSON Type | PySpark Type | Description | Example |
|-----------|--------------|-------------|---------|
| `string` | StringType | Text data | `"product_name"` |
| `integer` | IntegerType | Whole numbers | `123` |
| `long` | LongType | Large integers | `1234567890` |
| `double` | DoubleType | Decimal numbers | `99.99` |
| `float` | FloatType | Single precision decimals | `99.9` |
| `boolean` | BooleanType | True/false values | `true` |
| `date` | DateType | Date values | `"2024-01-15"` |
| `timestamp` | TimestampType | Date and time values | `"2024-01-15 10:30:00"` |

### **Schema Validation Features**

- **Type Validation**: Ensures data matches expected types
- **Nullability Enforcement**: Validates required vs optional fields
- **Corrupt Data Rescue**: Captures data that doesn't match schema in `rescuedDataColumn`
- **Data Quality**: Prevents downstream processing issues

### **File Archiving Behavior**

- **Runtime Requirement**: Databricks Runtime 16.4 LTS or above (required for `cloudFiles.cleanSource`)
- **Default Retention**: Files are archived 30 days after processing (not immediately)
- **Configurable Retention**: Use `cloudFiles.cleanSource.retentionDuration` to set custom retention (minimum 7 days)
- **Archive Modes**: 
  - `MOVE`: Moves files to specified destination
  - `DELETE`: Deletes files from source location
  - `OFF`: No automatic file management (default)

## 📊 **Demo Data Scenarios**

### **Scenario 1: Customer Data (SCD Type 2)**
```python
# Generate incremental customer updates
customer_batch1 = generate_customer_data(batch_number=1, num_customers=50, include_updates=False)
customer_batch2 = generate_customer_data(batch_number=2, num_customers=30, include_updates=True)
```

**What to Observe:**
- Fixed schema enforcement with data validation
- Corrupt data handling in `rescuedDataColumn`
- Historical tracking in silver layer
- SCD Type 2 implementation with effective dates

### **Scenario 2: Transaction Data (SCD Type 1)**
```python
# Generate transaction data with soft deletes
transaction_batch1 = generate_transaction_data(batch_number=1, num_transactions=100, include_deletes=False)
transaction_batch2 = generate_transaction_data(batch_number=2, num_transactions=50, include_deletes=True)
```

**What to Observe:**
- Schema inference from data structure
- Real-time transaction processing
- Soft delete handling with `is_deleted` flag
- SCD Type 1 implementation (overwrite changes)

### **Scenario 3: Inventory Data (Fixed Schema + Archive)**
```python
# Generate inventory data with corrupt records
inventory_batch1 = generate_inventory_data(batch_number=1, num_products=200, include_corrupt=False)
inventory_batch2 = generate_inventory_data(batch_number=2, num_products=150, include_corrupt=True)
```

**What to Observe:**
- Fixed schema enforcement with data validation
- Corrupt data handling in `rescuedDataColumn`
- File archiving after 30 days (configurable with `cloudFiles.cleanSource.retentionDuration`)
- Single file processing for controlled batch ingestion

### **Scenario 4: Shipment Data (File Notification Mode)**
```python
# Generate shipment data with evolving schema
shipment_batch1 = generate_shipment_data(batch_number=1, num_shipments=75, include_new_fields=False)
shipment_batch2 = generate_shipment_data(batch_number=2, num_shipments=50, include_new_fields=True)
```

**What to Observe:**
- File notification mode triggering
- Schema evolution with new fields
- Real-time processing on file arrival
- Rescue mode for unexpected data

## 🧹 **Cleanup and Reset**

The demo includes a comprehensive cleanup function:

```python
# Reset all demo data and directories
reset_demo_data()
```

This function:
- 🗑️ Removes all generated data files
- 🗑️ Cleans up schema and checkpoint directories
- 🗑️ Removes archive directories
- ✅ Recreates clean directory structure
- ✅ Regenerates initial schema files

## 🧪 **Testing**

The framework includes comprehensive test coverage with 103 tests across multiple components.

### **Test Categories**
- **Integration Tests** (8 tests): End-to-end pipeline generation and validation
- **Unit Tests** (69 tests): Individual component testing
  - Config Parser (26 tests): TSV configuration validation
  - Unified Pipeline Generator (31 tests): Resource generation logic
  - Notebook Generator (12 tests): Notebook generation functionality
- **Schema Converter Tests** (26 tests): JSON schema to PySpark conversion

### **Running Tests**
```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test categories
uv run pytest tests/test_integration.py      # Integration tests
uv run pytest tests/test_config_parser.py    # Configuration validation
uv run pytest tests/test_schema_converter.py # Schema conversion

# Run tests with coverage
uv run pytest --cov=src --cov-report=html

# Run tests in parallel (faster execution)
uv run pytest -n auto
```

### **Test Dependencies**
The test suite requires additional dependencies that are automatically installed with the dev group:
- **pyspark**: For schema converter tests and PySpark type validation
- **pytest**: Testing framework
- **black**: Code formatting
- **flake8**: Code linting

## ✅ **Configuration Validation**

The framework provides comprehensive validation at multiple levels to ensure your configuration is correct before deployment.

### **🚀 Quick Validation (Recommended)**

```bash
# Validate with summary (shows configuration overview)
uv run python src/utils/validate_config.py --summary

# Basic validation (errors only)
uv run python src/utils/validate_config.py

# Shell script validation
./scripts/validate.sh --summary
```

**What you get:**
- ✅ Instant validation feedback
- ✅ Configuration summary and statistics
- ✅ Detailed error messages with row numbers
- ✅ Pre-hook validation during bundle operations
- ✅ CI/CD integration ready

### **Bundle Validation Process**

When you run `databricks bundle validate`, the framework performs extensive validation:

```bash
# Validate your configuration
databricks bundle validate --profile dev
```

### **Validation Layers**

#### **1. Framework-Level Validation (ConfigParser)**
The framework validates your TSV configuration and catches errors like:

- **Missing required columns**: `Missing required columns: ['operation_type', 'pipeline_group']`
- **Invalid operation types**: `Invalid operation types: ['invalid_type']. Valid types: ['bronze', 'silver', 'gold', 'manual']`
- **Invalid source types**: `Invalid source types: ['invalid_source']. Valid types: ['file', 'table', 'notebook']`
- **Invalid file formats**: `Invalid file formats: ['invalid_format']. Valid formats: ['json', 'parquet', 'csv', 'avro', 'orc', '']`
- **Invalid file paths**: `Row 3: Invalid file path format: /invalid/path. Should start with /Volumes/, s3://, abfss://, or src/`
- **Invalid JSON**: `Row 8: Invalid JSON in pipeline_config: {"header": "true", "invalid_json": }`
- **Invalid email formats**: `Row 11: Invalid email format in recipients: invalid-email-format`
- **Serverless cluster requirements**: `Row 13: serverless cluster requires warehouse_id in cluster_config`
- **Parameter validation**: `Row 16: Parameter 'invalid_param' must be a string, number, or boolean`

#### **2. Databricks API Validation**
The Databricks bundle validation catches API-level errors:

```
Error: expected string, found map
  at resources.jobs.manual_test_pipeline17_job.tasks[0].notebook_task.base_parameters.invalid_param
```

### **What Gets Validated**

| Category | Validation | Example Error |
|----------|------------|---------------|
| **Required Fields** | All required columns present | `Missing required columns: ['operation_type']` |
| **Operation Types** | Valid operation types | `Invalid operation types: ['invalid_type']` |
| **Source Types** | Valid source types | `Invalid source types: ['invalid_source']` |
| **File Formats** | Valid file formats | `Invalid file formats: ['invalid_format']` |
| **Path Formats** | Unity Catalog/S3/ADLS paths | `Invalid file path format: /invalid/path` |
| **JSON Syntax** | Valid JSON in all fields | `Invalid JSON in pipeline_config: {...}` |
| **Email Formats** | Valid email addresses | `Invalid email format in recipients: invalid-email` |
| **Cluster Config** | Valid cluster sizes and requirements | `serverless cluster requires warehouse_id` |
| **Parameters** | Valid parameter types and names | `Parameter 'invalid_param' must be a string, number, or boolean` |
| **API Compatibility** | Databricks API requirements | `expected string, found map` |

### **Validation Commands**

```bash
# Quick validation with summary
uv run python src/utils/validate_config.py --summary

# Basic validation
uv run python src/utils/validate_config.py

# Shell script validation
./scripts/validate.sh --summary

# Databricks bundle validation (includes pre-hook validation)
uv run databricks bundle validate --profile dev

# Check configuration parsing
uv run python -c "import pandas as pd; df = pd.read_csv('config/unified_pipeline_config.tsv', sep='\t'); print(df.head())"

# Validate JSON configuration
uv run python -c "import json; json.loads('{\"test\": \"value\"}'); print('JSON valid')"

# Test resource generation
uv run python -c "from resources.unified_pipeline_generator import UnifiedPipelineGenerator; print('Generator imported successfully')"
```

### **Error Resolution**

1. **Read the error message carefully** - Each error tells you exactly what's wrong
2. **Check the row number** - Errors reference specific rows in your TSV
3. **Verify the format** - Ensure paths, JSON, and email formats are correct
4. **Check required fields** - Make sure all required columns are present
5. **Validate JSON syntax** - Use a JSON validator for complex configurations

## 🔍 **Troubleshooting**

### **Common Issues**

1. **Volume Not Found**
   - Ensure Unity Catalog is enabled
   - Check catalog and schema permissions
   - Verify volume creation in data generation notebook
   - **Configuration Issue**: Ensure `CATALOG`, `SCHEMA`, and `VOLUME_NAME` match between TSV and data generation notebook

2. **Path Mismatch Errors**
   - **Root Cause**: TSV paths don't match data generation notebook configuration
   - **Solution**: Verify both files use the same `{CATALOG}/{SCHEMA}/{VOLUME_NAME}` values
   - **Check**: Run the verification commands in Configuration Setup section

3. **Schema Validation Errors**
   - Check TSV configuration for correct option names
   - Verify JSON formatting in pipeline_config
   - Use `cloudFiles.validateOptions: false` for testing
   - **Configuration Issue**: Ensure all paths in TSV use your actual catalog/schema/volume names

4. **JSON Schema Inference Errors**
   - **Error**: `Failed to infer schema for format json`
   - **Root Cause**: JSON files with nested objects require `multiline: true`
   - **Solution**: Add `"multiline": "true"` to JSON pipeline configurations
   - **Example**: Shipment demo uses nested objects like `package_dimensions`, `customs_declaration`

5. **Pipeline Failures**
   - Check Auto Loader option names against [Microsoft documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options)
   - Verify directory permissions
   - Check cluster configuration and permissions
   - **Configuration Issue**: Ensure target table names match your catalog/schema structure

### **Configuration Checklist**

Before running the demo, verify these items:

- [ ] **Unity Catalog enabled** in your Databricks workspace
- [ ] **Permissions** to create catalogs, schemas, and volumes
- [ ] **TSV configuration** updated with your catalog/schema/volume names
- [ ] **Data generation notebook** updated with matching catalog/schema/volume names
- [ ] **Target table names** in TSV match your catalog/schema structure
- [ ] **All paths** use consistent naming convention
- [ ] **JSON formatting** in TSV pipeline_config is valid

### **Debug Commands**
```bash
# Check pipeline status
databricks bundle run <pipeline_name> --profile dev --debug

# Validate configuration (comprehensive validation)
databricks bundle validate --profile dev

# Check generated notebooks
ls -la src/notebooks/generated/

# Verify configuration consistency
grep -E "CATALOG =|SCHEMA =|VOLUME_NAME =" src/notebooks/manual/data_generation_demo.py
grep -E "/Volumes/" config/unified_pipeline_config.tsv | head -5

# Test configuration parsing
uv run python -c "import pandas as pd; df = pd.read_csv('config/unified_pipeline_config.tsv', sep='\t'); print(df.head())"

# Validate JSON configuration
uv run python -c "import json; json.loads('{\"test\": \"value\"}'); print('JSON valid')"

# Test resource generation
uv run python -c "from resources.unified_pipeline_generator import UnifiedPipelineGenerator; print('Generator imported successfully')"
```

## 📚 **Documentation**

- **Main README**: This file - Framework overview and quick start
- **Manual Notebooks**: `src/notebooks/manual/README.md` - Comprehensive manual operation guide
- **Examples**: Working examples in `src/notebooks/manual/`

## 🎯 **Key Benefits**

### **Unified Framework**
- **Single configuration**: Manage all pipeline types in one TSV file
- **Consistent patterns**: Same scheduling, notifications, and cluster config
- **Mixed workloads**: Run DLT and manual jobs side by side

### **Auto Loader Integration**
- **Comprehensive demos**: Real-world scenarios showcasing Auto Loader capabilities
- **Best practices**: Clean directory structure and proper option configuration
- **Extensible**: Dynamic option parsing from TSV configuration
- **Documentation aligned**: All options reference [Microsoft Auto Loader documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options)

### **Flexibility**
- **DLT pipelines**: Automatic generation for standard patterns
- **Manual jobs**: Full control for custom logic
- **Compute options**: Choose the right resources for your workload
- **Schema strategies**: Support for inference, fixed schema, and evolution

### **Production Ready**
- **Scheduling**: Cron-based scheduling with timezone support
- **Notifications**: Email notifications for success/failure
- **Error handling**: Proper dependency management and failure handling
- **Monitoring**: Job execution tracking and logging
- **Data quality**: Corrupt data handling and schema validation
- **Configuration validation**: Comprehensive validation at multiple levels
- **Clear error messages**: Specific, actionable error messages for easy troubleshooting

## 🔍 **Advanced Features**

### **Chained Execution**
- **Order-based sequencing**: Use `{"order": 1, 2, 3}` for manual pipelines
- **Dependency management**: Each step waits for previous step success
- **Error propagation**: Failed steps prevent subsequent execution

### **Smart Detection**
- **Automatic routing**: Framework detects operation types and handles appropriately
- **Pipeline creation**: DLT pipelines created automatically for bronze/silver groups
- **Job creation**: Manual jobs created for manual-only groups

### **Resource Management**
- **Cluster sizing**: Automatic cluster configuration based on size specification
- **Serverless support**: Full serverless cluster integration
- **Resource optimization**: Framework optimizes resource allocation

### **Auto Loader Extensibility**
- **Dynamic option parsing**: All Auto Loader options from TSV are automatically included
- **No hardcoding**: Framework picks up any new options without code changes
- **Future-proof**: Automatically supports new Auto Loader features as they're released
- **Option validation**: Built-in validation against Microsoft documentation standards

## ❓ **Frequently Asked Questions (FAQ)**

### **Getting Started**

#### **Q: What is this framework and why should I use it?**
A: This is a unified pipeline framework for Databricks that supports both DLT (Delta Live Tables) pipelines and manual notebook-based jobs. It provides:
- **Single configuration**: Manage all pipeline types in one TSV file
- **Auto Loader integration**: Comprehensive demos showcasing real-world data ingestion scenarios
- **Variable resolution**: Environment-specific configuration using Databricks bundle variables
- **Comprehensive validation**: Multiple layers of validation to catch errors before deployment

#### **Q: I'm new to Databricks Asset Bundles (DAB). How does this work?**
A: Databricks Asset Bundles is a deployment framework that allows you to:
- **Define resources** (pipelines, jobs, notebooks) in code
- **Use variables** for environment-specific configuration
- **Deploy consistently** across dev/staging/prod environments
- **Validate configurations** before deployment

This framework uses DAB to automatically generate and deploy your pipelines based on TSV configuration.

#### **Q: What's the difference between DLT pipelines and manual jobs?**
A: 
- **DLT Pipelines**: Auto-generated Delta Live Tables pipelines for bronze/silver operations with Auto Loader integration
- **Manual Jobs**: Custom notebook-based jobs where you write the code yourself, with full control and parameter passing

#### **Q: Do I need to manually generate notebooks?**
A: **No!** DLT notebooks are generated automatically when you run:
- `databricks bundle validate --profile dev`
- `databricks bundle deploy --profile dev`
- `databricks bundle run --profile dev`

The framework generates notebooks using your bundle variables, so they're always up-to-date with your configuration. You can still run `uv run python resources/notebook_generator.py` manually if needed for development or testing.

### **Configuration**

#### **Q: How do I configure my environment?**
A: You need to update two files:
1. **`config/unified_pipeline_config.tsv`**: Replace `${var.catalog_name}`, `${var.schema_name}`, `${var.volume_name}` with your actual values
2. **`src/notebooks/manual/data_generation_demo.py`**: Update `CATALOG`, `SCHEMA`, `VOLUME_NAME` variables

#### **Q: What are the required columns in the TSV configuration?**
A: The TSV must have these columns:
- `operation_type`: bronze, silver, gold, or manual
- `pipeline_group`: Groups related operations together
- `source_type`: file, table, or notebook
- `source_path`: Path to source data or notebook
- `target_table`: Target table name
- `file_format`: csv, json, parquet, etc. (empty for manual)
- `trigger_type`: file or time
- `schedule`: Cron expression
- `pipeline_config`: JSON configuration
- `cluster_size`: small, medium, large, or serverless
- `cluster_config`: JSON cluster configuration
- `notifications`: JSON notification settings
- `custom_expr`: Custom expressions (optional)
- `parameters`: JSON parameters for manual operations

#### **Q: How do I use variables in my configuration?**
A: Use Databricks bundle variables in your TSV:
- `${var.catalog_name}`: Your Unity Catalog catalog
- `${var.schema_name}`: Your Unity Catalog schema  
- `${var.volume_name}`: Your volume name

These are defined in `databricks.yml` and resolved during deployment.

### **Validation and Errors**

#### **Q: How do I validate my configuration?**
A: Run `databricks bundle validate --profile dev` to validate your configuration. This will catch:
- Missing required columns
- Invalid operation types, source types, file formats
- Invalid file paths and JSON syntax
- Email format errors
- Cluster configuration issues
- Parameter validation errors

#### **Q: What if I get validation errors?**
A: The framework provides clear, specific error messages:
1. **Read the error message** - It tells you exactly what's wrong
2. **Check the row number** - Errors reference specific rows in your TSV
3. **Verify the format** - Ensure paths, JSON, and email formats are correct
4. **Check required fields** - Make sure all required columns are present

#### **Q: Why does my bundle validate pass but deployment fails?**
A: Bundle validate checks configuration syntax and basic requirements. Deployment failures usually indicate:
- **Permission issues**: Check Unity Catalog permissions
- **Resource conflicts**: Names already exist in your workspace
- **Network issues**: Connectivity to Databricks workspace
- **Runtime errors**: Issues that only appear when actually running the code

### **Auto Loader**

#### **Q: What Auto Loader features are supported?**
A: The framework supports all major Auto Loader features:
- **File formats**: CSV, JSON, Parquet, Avro, ORC
- **Schema strategies**: Fixed schema, schema inference, schema evolution
- **File management**: Checkpointing, archiving, clean source
- **Data quality**: Corrupt data handling, rescued data columns
- **Performance**: Batch processing, file notification mode

#### **Q: How do I configure Auto Loader options?**
A: Add Auto Loader options to the `pipeline_config` column as JSON:
```json
{
  "cloudFiles.checkpointLocation": "/Volumes/catalog/schema/volume/checkpoint/table",
  "cloudFiles.maxFilesPerTrigger": "100",
  "cloudFiles.allowOverwrites": "false",
  "header": "true",
  "cloudFiles.rescuedDataColumn": "corrupt_data"
}
```

#### **Q: What's the difference between file and time triggers?**
A: 
- **File trigger**: Processes files as they arrive (real-time)
- **Time trigger**: Processes files on a schedule (batch)

### **Manual Operations**

#### **Q: How do I create manual jobs?**
A: Set `operation_type` to `manual` and provide:
- `source_path`: Path to your notebook (must start with `src/`)
- `pipeline_config`: Order and dependency configuration
- `parameters`: User-defined parameters as JSON

#### **Q: How do I chain manual operations?**
A: Use the `pipeline_config` column:
```json
{
  "order": 1,
  "depends_on": 2
}
```
- `order`: Execution order (1, 2, 3, etc.)
- `depends_on`: Wait for this order to complete first

#### **Q: How do I pass parameters to manual notebooks?**
A: Use the `parameters` column:
```json
{
  "batch_size": 1000,
  "debug_mode": true,
  "catalog": "${var.catalog_name}"
}
```

In your notebook, access them with:
```python
batch_size = dbutils.widgets.get("batch_size", "500")
debug_mode = dbutils.widgets.get("debug_mode", "false").lower() == "true"
```

### **Deployment and Operations**

#### **Q: How do I deploy my pipelines?**
A: 
```bash
# Validate configuration
databricks bundle validate --profile dev

# Deploy to Databricks
databricks bundle deploy --profile dev

# Run specific pipeline
databricks bundle run <pipeline_name> --profile dev
```

#### **Q: How do I monitor my pipelines?**
A: 
- **Databricks UI**: Go to Workflows → Delta Live Tables or Jobs
- **Pipeline logs**: Check the pipeline execution logs
- **Data quality**: Monitor data quality metrics in Unity Catalog
- **Notifications**: Set up email notifications for success/failure

#### **Q: How do I update my configuration?**
A: 
1. Update your TSV configuration
2. Run `databricks bundle validate --profile dev`
3. Run `databricks bundle deploy --profile dev`
4. The framework will update existing resources

### **Troubleshooting**

#### **Q: My pipeline fails with "Volume not found" error.**
A: This usually means:
- Unity Catalog is not enabled in your workspace
- You don't have permissions to create volumes
- The volume path in your TSV doesn't match your actual catalog/schema/volume names
- You need to run the data generation notebook first

#### **Q: I get "Path mismatch" errors.**
A: Ensure both files use the same catalog/schema/volume names:
- Check `config/unified_pipeline_config.tsv`
- Check `src/notebooks/manual/data_generation_demo.py`
- Run the verification commands in the Configuration Setup section

#### **Q: My JSON configuration is invalid.**
A: Common JSON issues:
- Missing quotes around keys: `{key: "value"}` should be `{"key": "value"}`
- Trailing commas: `{"a": 1, "b": 2,}` should be `{"a": 1, "b": 2}`
- Unescaped quotes: `{"message": "He said "hello""}` should be `{"message": "He said \"hello\""}`

#### **Q: My serverless cluster fails.**
A: Serverless clusters require:
- Valid `warehouse_id` in `cluster_config`
- Not a placeholder value like `"your_warehouse_id"`
- Proper permissions to use the warehouse

### **Advanced Usage**

#### **Q: How do I add custom Auto Loader options?**
A: Add any Auto Loader option to your `pipeline_config` JSON. The framework automatically includes all options without code changes.

#### **Q: How do I create a silver-only pipeline?**
A: Create a pipeline group with only silver operations. The framework will create a DLT pipeline that reads from existing bronze tables.

#### **Q: How do I handle schema evolution?**
A: Use `cloudFiles.schemaEvolutionMode: "rescue"` in your pipeline config. This captures new fields in a rescued data column.

#### **Q: How do I archive processed files?**
A: Use `cloudFiles.cleanSource: "MOVE"` with `cloudFiles.cleanSource.moveDestination` to archive files after processing.

## 🤝 **Contributing**

1. **Fork** the repository
2. **Create** a feature branch
3. **Add** your changes
4. **Test** thoroughly
5. **Submit** a pull request

## 📄 **License**

[Add your license information here]

---

**Need help?** Check the comprehensive documentation in `src/notebooks/manual/README.md` or create an issue in the repository.
