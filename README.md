# Auto Loader Framework with PyDAB

A unified pipeline framework for Databricks that supports both DLT pipelines and manual notebook-based jobs, featuring comprehensive Auto Loader demos showcasing real-world data ingestion scenarios.

## 🚀 **Framework Overview**

This framework provides a unified approach to data pipeline management in Databricks, supporting multiple operation types:

- **`bronze`**: File ingestion operations using [Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options)
- **`silver`**: SCD Type 2 transformations with Auto CDC
- **`gold`**: Analytics and reporting operations
- **`manual`**: Custom notebook-based jobs with full control

## 🎯 **Auto Loader Demo Scenarios**

The framework includes comprehensive demo scenarios that showcase real-world Auto Loader use cases:

### **1. Customer Data Demo (SCD Type 2)**
- **Scenario**: Incremental customer data updates with full history tracking
- **Format**: CSV files with incremental changes
- **Features**: 
  - Schema inference with `cloudFiles.inferSchema: true`
  - SCD Type 2 implementation in silver layer
  - Historical change tracking for customer attributes
- **Auto Loader Options**: Schema location, checkpoint location, max files per trigger

### **2. Transaction Data Demo (SCD Type 1)**
- **Scenario**: Transaction data with soft delete flags
- **Format**: CSV files with transaction records
- **Features**:
  - Soft delete handling with `is_deleted` flag
  - SCD Type 1 implementation (overwrite changes)
  - Real-time transaction processing
- **Auto Loader Options**: Schema inference, checkpoint management

### **3. Inventory Data Demo (Fixed Schema + Immediate Archive)**
- **Scenario**: Daily inventory files with fixed schema and immediate archive management
- **Format**: CSV files with strict schema requirements
- **Features**:
  - **Fixed schema enforcement** with predefined schema file
  - **Immediate file archiving** using `cloudFiles.cleanSource: move`
  - **Corrupt data handling** with `cloudFiles.rescuedDataColumn`
  - **Schema validation** to ensure data quality
  - **Single file processing** with `maxFilesPerTrigger: 1` for immediate archiving
- **Auto Loader Options**: Clean source move, rescued data column, schema validation

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
operation_type	pipeline_group	source_type	file_format	source_path	target_table	trigger_type	schedule	pipeline_config	cluster_size	notifications
```

### **Operation Types**

#### **Bronze Operations**
```tsv
bronze	customer_pipeline	file	csv	abfss://path/to/source	vbdemos.adls_bronze.customers	time	0 0 6 * * ?	{"cloudFiles.schemaLocation": "...", "cloudFiles.checkpointLocation": "..."}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

#### **Silver Operations**
```tsv
silver	customer_pipeline	table		vbdemos.adls_bronze.customers	vbdemos.adls_silver.customers_scd2	time	0 0 6 * * ?	{"keys": ["customer_id"], "track_history_except_column_list": ["name", "email"], "stored_as_scd_type": "2"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

#### **Gold Operations**
```tsv
gold	customer_pipeline	notebook		src/notebooks/generated/gold_analytics.py	vbdemos.adls_gold.customer_analytics	time	0 0 6 * * ?	{"notebook_path": "src/notebooks/generated/gold_analytics.py"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

#### **Manual Operations**
```tsv
manual	my_pipeline	notebook		src/notebooks/manual/my_step1.py	output_table1	time	0 0 9 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/my_step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
manual	my_pipeline	notebook		src/notebooks/manual/my_step2.py	output_table2	time	0 0 9 * * ?	{"order": 2, "notebook_path": "src/notebooks/manual/my_step2.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
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
bronze	product_pipeline	file	csv	abfss://path/to/products	vbdemos.adls_bronze.products	time	0 0 */20 * * ?	{"cloudFiles.schemaLocation": "...", "cloudFiles.checkpointLocation": "..."}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}

# Silver: SCD Type 2 transformation
silver	product_pipeline	table		vbdemos.adls_bronze.products	vbdemos.adls_silver.products_scd2	time	0 0 */20 * * ?	{"keys": ["product_id"], "track_history_except_column_list": ["name", "price"], "stored_as_scd_type": "2"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}

# Gold: Analytics
gold	product_pipeline	notebook		src/notebooks/generated/gold_analytics.py	vbdemos.adls_gold.product_analytics	time	0 0 */20 * * ?	{"notebook_path": "src/notebooks/generated/gold_analytics.py"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

### **2. Manual Pipeline (Custom Notebooks)**
```tsv
# Step 1: Data preparation
manual	custom_pipeline	notebook		src/notebooks/manual/step1.py	table1	time	0 0 8 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}

# Step 2: Data transformation
manual	custom_pipeline	notebook		src/notebooks/manual/step2.py	table2	time	0 0 8 * * ?	{"order": 2, "notebook_path": "src/notebooks/manual/step2.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}

# Step 3: Final analytics
manual	custom_pipeline	notebook		src/notebooks/manual/step3.py	table3	time	0 0 8 * * ?	{"order": 3, "notebook_path": "src/notebooks/manual/step3.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
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

# Install dependencies
pip install -r requirements.txt
```

### **2. Configuration**
1. **Edit** `config/unified_pipeline_config.tsv` (see Configuration Setup above)
2. **Edit** `src/notebooks/manual/data_generation_demo.py` (see Configuration Setup above)
3. **Choose** appropriate cluster sizes
4. **Set** schedules and notifications

### **3. Notebooks**
- **DLT pipelines**: Framework generates automatically
- **Manual jobs**: Create notebooks in `src/notebooks/manual/`

### **4. Deployment**
```bash
# Validate configuration
databricks bundle validate --profile dev

# Deploy to Databricks
databricks bundle deploy --profile dev

# Run jobs manually
databricks bundle run <job_name> --profile dev
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
| `cloudFiles.schemaLocation` | Schema tracking and evolution | All demos - separate schema directories | `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/schema/{demo}` |
| `cloudFiles.checkpointLocation` | Processing state tracking | All demos - separate checkpoint directories | `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/checkpoint/{demo}` |
| `cloudFiles.maxFilesPerTrigger` | Batch size control | Customer: 100, Transaction: 50, Inventory: 1, Shipment: 100 | N/A |
| `cloudFiles.allowOverwrites` | File overwrite handling | All demos: false (safety) | N/A |
| `cloudFiles.cleanSource` | Immediate file archiving | Inventory demo: "move" | N/A |
| `cloudFiles.cleanSource.moveDestination` | Archive destination | Inventory demo | `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/archive/inventory` |
| `cloudFiles.rescuedDataColumn` | Corrupt data handling | Inventory demo: "corrupt_data" | N/A |
| `cloudFiles.useManagedFileEvents` | File notification mode | Shipment demo only | N/A |
| `cloudFiles.schemaEvolutionMode` | Schema change handling | Shipment demo: "rescue" | N/A |
| `multiline` | JSON multiline support | Shipment demo: true (for nested objects) | N/A |
| `cloudFiles.validateOptions` | Option validation | All demos: false (temporary) | N/A |

### **File Format Options**

| Format | Demo | Options Used |
|--------|------|--------------|
| **CSV** | Customer, Transaction, Inventory | `header: true`, `inferSchema: true` |
| **JSON** | Shipment | `multiline: true`, `cloudFiles.schemaEvolutionMode: rescue` |

### **Schema Management**

| Strategy | Demo | Implementation |
|----------|------|----------------|
| **Schema Inference** | Customer, Transaction | `cloudFiles.inferSchema: true` |
| **Fixed Schema** | Inventory | Predefined schema file with validation |
| **Schema Evolution** | Shipment | `cloudFiles.schemaEvolutionMode: rescue` |

## 📊 **Demo Data Scenarios**

### **Scenario 1: Customer Data (SCD Type 2)**
```python
# Generate incremental customer updates
customer_batch1 = generate_customer_data(batch_number=1, num_customers=50, include_updates=False)
customer_batch2 = generate_customer_data(batch_number=2, num_customers=30, include_updates=True)
```

**What to Observe:**
- Schema inference from first batch
- Historical tracking in silver layer
- SCD Type 2 implementation with effective dates

### **Scenario 2: Transaction Data (SCD Type 1)**
```python
# Generate transaction data with soft deletes
transaction_batch1 = generate_transaction_data(batch_number=1, num_transactions=100, include_deletes=False)
transaction_batch2 = generate_transaction_data(batch_number=2, num_transactions=50, include_deletes=True)
```

**What to Observe:**
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
- Fixed schema enforcement
- Corrupt data handling in `corrupt_data` column
- File archiving to archive directory
- Schema validation failures

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

# Validate configuration
databricks bundle validate --profile dev

# Check generated notebooks
ls -la src/notebooks/generated/

# Verify configuration consistency
grep -E "CATALOG =|SCHEMA =|VOLUME_NAME =" src/notebooks/manual/data_generation_demo.py
grep -E "/Volumes/" config/unified_pipeline_config.tsv | head -5
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
