# Autoloader Framework with PyDAB

A unified pipeline framework for Databricks that supports both DLT pipelines and manual notebook-based jobs.

## 🚀 **Framework Overview**

This framework provides a unified approach to data pipeline management in Databricks, supporting multiple operation types:

- **`bronze`**: File ingestion operations using Autoloader
- **`silver`**: SCD Type 2 transformations with Auto CDC
- **`gold`**: Analytics and reporting operations
- **`manual`**: Custom notebook-based jobs with full control

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
1. **Edit** `config/unified_pipeline_config.tsv`
2. **Add** your pipeline configurations
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

## 📚 **Documentation**

- **Main README**: This file - Framework overview and quick start
- **Manual Notebooks**: `src/notebooks/manual/README.md` - Comprehensive manual operation guide
- **Examples**: Working examples in `src/notebooks/manual/`

## 🎯 **Key Benefits**

### **Unified Framework**
- **Single configuration**: Manage all pipeline types in one TSV file
- **Consistent patterns**: Same scheduling, notifications, and cluster config
- **Mixed workloads**: Run DLT and manual jobs side by side

### **Flexibility**
- **DLT pipelines**: Automatic generation for standard patterns
- **Manual jobs**: Full control for custom logic
- **Compute options**: Choose the right resources for your workload

### **Production Ready**
- **Scheduling**: Cron-based scheduling with timezone support
- **Notifications**: Email notifications for success/failure
- **Error handling**: Proper dependency management and failure handling
- **Monitoring**: Job execution tracking and logging

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
