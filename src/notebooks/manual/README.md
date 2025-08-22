# Manual Notebook Examples

This folder contains example notebooks that demonstrate the new `manual` operation type in the unified pipeline framework.

## Overview

The `manual` operation type allows you to create notebook-based jobs instead of DLT pipelines, giving you full control over notebook execution while leveraging the framework's scheduling and notification capabilities.

## Example: Manual Test Pipeline

### Configuration
```tsv
manual	manual_test_pipeline	notebook		src/notebooks/manual/manual_test_step1.py	vbdemos.adls_bronze.manual_step1_data	time	0 0 9 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/manual_test_step1.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}
manual	manual_test_pipeline	notebook		src/notebooks/manual/manual_test_step2.py	vbdemos.adls_bronze.manual_step2_data	time	0 0 9 * * ?	{"order": 2, "notebook_path": "src/notebooks/manual/manual_test_step2.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}
manual	manual_test_pipeline	notebook		src/notebooks/manual/manual_test_step3.py	vbdemos.adls_gold.manual_final_analytics	time	0 0 9 * * ?	{"order": 3, "notebook_path": "src/notebooks/manual/manual_test_step3.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}
```

### Execution Flow
1. **Step 1** (`manual_test_step1.py`): Data preparation and initial processing
2. **Step 2** (`manual_test_step2.py`): Data transformation and enrichment (waits for Step 1)
3. **Step 3** (`manual_test_step3.py`): Final analytics and reporting (waits for Step 2)

### Key Features Demonstrated
- **Chained execution**: Each step waits for the previous step to complete
- **Parameter passing**: Framework automatically passes pipeline context to notebooks
- **Scheduling**: Daily execution at 9 AM
- **Notifications**: Success/failure emails to specified recipients
- **Error handling**: If any step fails, subsequent steps are skipped

## Notebook Structure

Each notebook receives these parameters from the framework:
- `pipeline_group`: The pipeline group name (e.g., "manual_test_pipeline")
- `operation_type`: Always "manual" for manual operations
- `operation_index`: The step number (0-based)
- `total_operations`: Total number of steps in the pipeline

## Compute Selection Guide

The framework supports multiple compute options to match your performance and cost requirements:

### 🚀 **Serverless (Recommended for Most Use Cases)**
**Configuration:** `cluster_size: serverless`

**When to Use:**
- **Best performance** - Databricks automatically optimizes compute resources
- **Production workloads** - Critical jobs requiring maximum performance
- **Variable workloads** - Jobs with unpredictable resource requirements
- **Cost optimization** - Pay only for actual compute time used
- **Quick startup** - No cluster provisioning delays

**Benefits:**
- Automatic scaling based on workload demands
- No cluster management overhead
- Optimal performance for most workloads
- Cost-effective for variable workloads

**Example Use Cases:**
- Data processing pipelines
- Analytics jobs
- ETL operations
- Real-time data processing

### 🔧 **Small Cluster**
**Configuration:** `cluster_size: small`

**When to Use:**
- **Non-critical jobs** - Jobs where SLA is flexible
- **Development/testing** - Prototyping and experimentation
- **Light workloads** - Simple data transformations
- **Cost-sensitive** - When budget is a primary concern
- **Predictable workloads** - Jobs with consistent resource needs

**Specifications:**
- Node type: Standard_D2s_v5
- Max workers: 2
- Memory: ~8 GB per node
- CPU: 2 vCPUs per node

**Example Use Cases:**
- Data validation jobs
- Simple reporting
- Development testing
- Non-critical data processing

### ⚖️ **Medium Cluster**
**Configuration:** `cluster_size: medium`

**When to Use:**
- **Balanced workloads** - Jobs requiring moderate resources
- **Standard processing** - Typical ETL and analytics jobs
- **Mixed workloads** - Jobs with varying complexity
- **Cost-performance balance** - Good performance without premium cost
- **Team development** - Shared development environments

**Specifications:**
- Node type: Standard_D4s_v5
- Max workers: 3
- Memory: ~16 GB per node
- CPU: 4 vCPUs per node

**Example Use Cases:**
- Standard ETL pipelines
- Data quality checks
- Medium-scale analytics
- Team development work

### 🚀 **Large Cluster**
**Configuration:** `cluster_size: large`

**When to Use:**
- **Heavy workloads** - Jobs requiring significant resources
- **Complex processing** - Advanced analytics and ML workloads
- **High throughput** - Jobs processing large volumes of data
- **Performance critical** - When speed is essential
- **Resource-intensive** - Jobs with high memory/CPU requirements

**Specifications:**
- Node type: Standard_D8s_v5
- Max workers: 5
- Memory: ~32 GB per node
- CPU: 8 vCPUs per node

**Example Use Cases:**
- Large-scale data processing
- Machine learning workloads
- Complex analytics
- High-throughput ETL

## Compute Selection Decision Matrix

**🚀 Serverless**: Choose serverless when you need maximum performance and have variable workloads. It's perfect for production environments where you want Databricks to automatically optimize resources and you're willing to pay for the best performance. Use this for critical jobs where SLA is important and you want the fastest possible execution.

**🔧 Small**: Choose small clusters when cost is your primary concern and you're working with predictable, light workloads. This is ideal for development, testing, and non-critical jobs where you can afford to wait a bit longer for completion. Perfect for data validation, simple reporting, and prototyping.

**⚖️ Medium**: Choose medium clusters for balanced workloads that need moderate resources without breaking the bank. This is the sweet spot for standard ETL jobs, data quality checks, and team development work where you want good performance at a reasonable cost. Great for jobs that run regularly but aren't performance-critical.

**🚀 Large**: Choose large clusters when you have heavy workloads that require significant resources and performance is critical. This is for complex processing, machine learning workloads, and high-throughput operations where you need maximum power and are willing to pay for it. Use this when speed is essential and you're processing large volumes of data.

## Configuration Examples

### High-Performance Production Job
```tsv
manual	production_pipeline	notebook		src/notebooks/manual/production_step.py	output_table	time	0 0 6 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/production_step.py"}	serverless	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com"]}
```

### Cost-Optimized Development Job
```tsv
manual	dev_pipeline	notebook		src/notebooks/manual/dev_step.py	dev_table	time	0 0 10 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/dev_step.py"}	small	{"on_success": false, "on_failure": true, "recipients": ["dev-team@company.com"]}
```

### Balanced Workload Job
```tsv
manual	balanced_pipeline	notebook		src/notebooks/manual/balanced_step.py	balanced_table	time	0 0 8 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/balanced_step.py"}	medium	{"on_success": true, "on_failure": true, "recipients": ["data-team@company.com"]}
```

### High-Throughput Processing Job
```tsv
manual	throughput_pipeline	notebook		src/notebooks/manual/throughput_step.py	throughput_table	time	0 0 */4 * * ?	{"order": 1, "notebook_path": "src/notebooks/manual/throughput_step.py"}	large	{"on_success": true, "on_failure": true, "recipients": ["admin@company.com", "data-team@company.com"]}
```

## Best Practices

### 1. **Start with Serverless**
- Use serverless for new jobs unless you have specific requirements
- Let Databricks optimize performance automatically
- Monitor costs and adjust if needed

### 2. **Right-Size Based on Workload**
- **Small**: Simple transformations, validation, testing
- **Medium**: Standard ETL, moderate analytics
- **Large**: Complex processing, ML workloads, high throughput
- **Serverless**: Variable workloads, production, performance-critical

### 3. **Consider SLA Requirements**
- **Critical SLA**: Use serverless or large clusters
- **Flexible SLA**: Small or medium clusters are fine
- **Development**: Small clusters for cost optimization

### 4. **Monitor and Optimize**
- Track job execution times
- Monitor resource utilization
- Adjust cluster sizes based on performance data
- Consider cost vs. performance trade-offs

## Usage

1. **Create your notebooks** in this folder or any location
2. **Choose appropriate cluster size** based on your requirements
3. **Add configuration** to `config/unified_pipeline_config.tsv`
4. **Deploy** using `databricks bundle deploy --profile dev`
5. **Run manually** using `databricks bundle run <job_name> --profile dev`

## Benefits

- **Full control**: Write notebooks exactly how you want
- **No DLT dependencies**: Skip pipeline creation entirely
- **Flexible chaining**: Any number of steps, any execution order
- **Framework integration**: Leverage scheduling, notifications, cluster config
- **Seamless coexistence**: Works alongside existing DLT pipelines
- **Compute flexibility**: Choose the right compute for your workload
- **Cost optimization**: Balance performance and cost requirements
- **Performance tuning**: Optimize based on your specific needs