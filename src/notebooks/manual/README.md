# Manual Notebook Examples

This folder contains the Auto Loader demo data generation notebook and examples for the `manual` operation type in the unified pipeline framework.

## 📊 Auto Loader Demo Data Generation

### `data_generation_demo.py`
The main demo notebook that generates sample data for all 4 Auto Loader scenarios:

1. **Customer Data Demo (SCD Type 2)**: Incremental customer updates with schema inference
2. **Transaction Data Demo (SCD Type 1)**: Transaction data with soft delete flags  
3. **Inventory Data Demo (Fixed Schema + Archive)**: Daily inventory with fixed schema and archiving
4. **Shipment Data Demo (File Notification Mode)**: Real-time shipment tracking with schema evolution

**Features:**
- ✅ Dynamic data generation using Faker library
- ✅ Clean directory structure creation
- ✅ Fixed schema file generation for inventory demo
- ✅ Comprehensive cleanup and reset functionality
- ✅ Step-by-step execution for learning

## 🔧 Manual Operation Type

The `manual` operation type allows you to create notebook-based jobs instead of DLT pipelines, giving you full control over notebook execution while leveraging the framework's scheduling and notification capabilities.

### Key Features
- **Full control**: Write notebooks exactly how you want
- **No DLT dependencies**: Skip pipeline creation entirely
- **Flexible chaining**: Any number of steps, any execution order
- **Framework integration**: Leverage scheduling, notifications, cluster config
- **Seamless coexistence**: Works alongside existing DLT pipelines

## 📋 Configuration Example

```tsv
manual	my_pipeline	notebook		src/notebooks/manual/my_step1.py	output_table1	time	0 0 9 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/my_step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
manual	my_pipeline	notebook		src/notebooks/manual/my_step2.py	output_table2	time	0 0 9 * * ?	{"order": 2, "notebook_path": "src/notebooks/manual/my_step2.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

### Execution Flow
1. **Step 1** (`my_step1.py`): Data preparation and initial processing
2. **Step 2** (`my_step2.py`): Data transformation and enrichment (waits for Step 1)

### Key Features Demonstrated
- **Chained execution**: Each step waits for the previous step to complete
- **Parameter passing**: Framework automatically passes pipeline context to notebooks
- **Scheduling**: Daily execution at 9 AM
- **Notifications**: Success/failure emails to specified recipients
- **Error handling**: If any step fails, subsequent steps are skipped

## 🖥️ Compute Selection Guide

The framework supports multiple compute options to match your performance and cost requirements:

### 🚀 **Serverless (Recommended for Most Use Cases)**
**Configuration:** `cluster_size: serverless`

**When to Use:**
- **Best performance** - Databricks automatically optimizes compute resources
- **Production workloads** - Critical jobs requiring maximum performance
- **Variable workloads** - Jobs with unpredictable resource requirements
- **Cost optimization** - Pay only for actual compute time used

### 🔧 **Small Cluster**
**Configuration:** `cluster_size: small`

**When to Use:**
- **Non-critical jobs** - Jobs where SLA is flexible
- **Development/testing** - Prototyping and experimentation
- **Light workloads** - Simple data transformations
- **Cost-sensitive** - When budget is a primary concern

### ⚖️ **Medium Cluster**
**Configuration:** `cluster_size: medium`

**When to Use:**
- **Balanced workloads** - Jobs requiring moderate resources
- **Standard processing** - Typical ETL and analytics jobs
- **Mixed workloads** - Jobs with varying complexity
- **Cost-performance balance** - Good performance without premium cost

### 🚀 **Large Cluster**
**Configuration:** `cluster_size: large`

**When to Use:**
- **Heavy workloads** - Jobs requiring significant resources
- **Complex processing** - Advanced analytics and ML workloads
- **High throughput** - Jobs processing large volumes of data
- **Performance critical** - When speed is essential

## 📝 Usage

1. **Create your notebooks** in this folder or any location
2. **Choose appropriate cluster size** based on your requirements
3. **Add configuration** to `config/unified_pipeline_config.tsv`
4. **Deploy** using `databricks bundle deploy --profile dev`
5. **Run manually** using `databricks bundle run <job_name> --profile dev`

## 🎯 Benefits

- **Full control**: Write notebooks exactly how you want
- **No DLT dependencies**: Skip pipeline creation entirely
- **Flexible chaining**: Any number of steps, any execution order
- **Framework integration**: Leverage scheduling, notifications, cluster config
- **Seamless coexistence**: Works alongside existing DLT pipelines
- **Compute flexibility**: Choose the right compute for your workload
- **Cost optimization**: Balance performance and cost requirements
- **Performance tuning**: Optimize based on your specific needs