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

## Usage

1. **Create your notebooks** in this folder or any location
2. **Add configuration** to `config/unified_pipeline_config.tsv`
3. **Deploy** using `databricks bundle deploy --profile dev`
4. **Run manually** using `databricks bundle run <job_name> --profile dev`

## Benefits

- **Full control**: Write notebooks exactly how you want
- **No DLT dependencies**: Skip pipeline creation entirely
- **Flexible chaining**: Any number of steps, any execution order
- **Framework integration**: Leverage scheduling, notifications, cluster config
- **Seamless coexistence**: Works alongside existing DLT pipelines
