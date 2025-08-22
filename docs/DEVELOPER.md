# Developer Guide

This guide is for developers who want to extend, modify, or contribute to the Azure Databricks Autoloader Framework.

## 🏗️ Architecture Overview

### Framework Components

```
autoloader-framework-pydab/
├── databricks.yml                           # Bundle configuration
├── resources/
│   ├── notebook_generator.py                # Static notebook generator
│   └── unified_pipeline_generator.py        # DLT pipeline generator
├── src/
│   └── notebooks/
│       └── generated/                       # Generated DLT notebooks
├── config/
│   └── unified_pipeline_config.tsv          # Pipeline definitions
└── sample_data/                             # Test data and examples
```

### Data Flow

```mermaid
graph TD
    A[TSV Config] --> B[Notebook Generator]
    A --> C[Resource Generator]
    B --> D[Generated DLT Notebooks]
    C --> E[DLT Pipelines]
    C --> F[Scheduled Jobs]
    D --> G[Delta Tables]
    E --> G
    F --> E
```

## 🔧 Core Components

### 1. Notebook Generator (`resources/notebook_generator.py`)

**Purpose**: Generates static, DLT-compatible notebooks for each pipeline group.

**Key Methods**:
- `generate_pipeline_group_notebook()`: Creates unified notebook for a pipeline group
- `generate_bronze_table_notebook()`: Generates bronze ingestion logic
- `generate_silver_table_notebook()`: Generates silver SCD2 transformation logic

**Extension Points**:
```python
# Add new file format support
def generate_custom_format_notebook(self, op_name, op_config, notebook_path):
    """Generate notebook for custom file format."""
    # Add your custom format logic
    pass

# Add new table properties
def add_custom_table_properties(self, table_properties):
    """Add custom table properties to generated notebooks."""
    table_properties.update({
        "custom.property": "value"
    })
```

### 2. Resource Generator (`resources/unified_pipeline_generator.py`)

**Purpose**: Generates Databricks resources (pipelines, jobs) from configuration.

**Key Classes**:
- `UnifiedPipelineGenerator`: Main resource generation logic
- `Pipeline`: DLT pipeline definitions
- `Job`: Scheduled job definitions with pipeline tasks

**Key Methods**:
- `generate_resources()`: Main entry point
- `create_unified_pipeline()`: DLT pipeline creation
- `create_scheduled_job()`: Scheduled job creation with cron schedules

**Extension Examples**:

#### Adding New Resource Types
```python
def create_custom_resource(self, pipeline_group: str, group_rows: List[dict]) -> CustomResource:
    """Create a custom resource type."""
    return CustomResource(
        name=f"custom_{pipeline_group}",
        configuration=self._build_custom_config(group_rows),
        # Add your custom resource properties
    )
```

#### Adding New Cluster Configurations
```python
def _get_cluster_config(self, cluster_size: str, cluster_config: str) -> dict:
    """Generate cluster configuration with new sizes."""
    if cluster_size == "xlarge":
        cluster_settings.update({
            "node_type_id": "Standard_D16s_v5",
            "autoscale": {
                "min_workers": 4,
                "max_workers": 16
            }
        })
    return cluster_settings
```

### 3. Configuration Management

**TSV Configuration Schema**:
```python
# Add new configuration columns
def validate_custom_column(self, df: pd.DataFrame) -> List[str]:
    errors = []
    if 'custom_field' in df.columns:
        # Validate custom field logic
        pass
    return errors

# Add new pipeline configuration options
def parse_custom_pipeline_config(self, config_json: str) -> dict:
    """Parse custom pipeline configuration options."""
    try:
        config = json.loads(config_json)
        # Add custom parsing logic
        return config
    except json.JSONDecodeError:
        return {}
```

## 🚀 Development Workflow

### 1. Local Development

```bash
# Set up virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Test notebook generation
python resources/notebook_generator.py

# Test resource generation
python -c "from resources.unified_pipeline_generator import UnifiedPipelineGenerator; print('Import successful')"
```

### 2. Testing Changes

```bash
# Test notebook generation
python resources/notebook_generator.py

# Validate generated notebooks
python -c "import ast; ast.parse(open('src/notebooks/generated/unified_customer_pipeline.py').read()); print('Syntax valid')"

# Test bundle deployment (dry run)
databricks bundle validate --profile dev
```

### 3. Adding New Features

#### New File Format Support
1. Update `notebook_generator.py` with format-specific logic
2. Add format validation in configuration parser
3. Update example configurations
4. Test with sample data

#### New Pipeline Configuration Options
1. Add new JSON fields to pipeline_config
2. Update resource generator to handle new options
3. Add validation logic
4. Update documentation

## 🔧 Configuration Extensions

### Custom Pipeline Properties

```python
# Add custom pipeline properties
def add_custom_pipeline_properties(self, unified_config: dict, group_rows: List[dict]):
    """Add custom properties to pipeline configuration."""
    for row in group_rows:
        if row.get('operation_type') == 'silver':
            custom_config = json.loads(row.get('custom_config', '{}'))
            for key, value in custom_config.items():
                unified_config[f"custom.{key}"] = str(value)
```

### Custom Job Scheduling

```python
# Add custom scheduling logic
def create_custom_scheduled_job(self, pipeline_group: str, pipeline: Pipeline, schedule_config: dict) -> Job:
    """Create job with custom scheduling logic."""
    if schedule_config.get('type') == 'custom':
        # Implement custom scheduling logic
        schedule = self._create_custom_schedule(schedule_config)
    else:
        schedule = CronSchedule(quartz_cron_expression=schedule_config['cron'])
    
    return Job(
        name=f"custom_{pipeline_group}_job",
        tasks=[Task(task_key="custom_task", pipeline_task=PipelineTask(pipeline_id=pipeline.name))],
        schedule=schedule
    )
```

## 🧪 Testing

### Unit Tests

```python
# Test notebook generation
def test_notebook_generation():
    generator = NotebookGenerator()
    generator.generate_all_notebooks()
    
    # Verify notebooks were created
    assert os.path.exists("src/notebooks/generated/unified_customer_pipeline.py")
    assert os.path.exists("src/notebooks/generated/unified_product_pipeline.py")

# Test resource generation
def test_resource_generation():
    bundle = MockBundle()
    generator = UnifiedPipelineGenerator(bundle)
    pipelines, jobs = generator.generate_resources()
    
    assert len(pipelines) == 4  # customer, order, order_items, product
    assert len(jobs) == 4
```

### Integration Tests

```bash
# Test complete workflow
python resources/notebook_generator.py
python -c "from resources.unified_pipeline_generator import UnifiedPipelineGenerator; print('✅ All imports successful')"
databricks bundle validate --profile dev
```

## 📚 Best Practices

### Code Organization
- Keep notebook generation logic separate from resource generation
- Use consistent naming conventions for pipeline groups
- Implement proper error handling and logging
- Add comprehensive validation for configuration files

### Performance Considerations
- Generate notebooks once, not on every deployment
- Use resource references for pipeline dependencies
- Implement proper cleanup for temporary resources
- Cache configuration parsing results

### Security
- Validate all configuration inputs
- Sanitize file paths and table names
- Use proper authentication for Databricks connections
- Implement least-privilege access controls

## 🔍 Debugging

### Common Issues

1. **Import Errors**: Check virtual environment and dependencies
2. **Configuration Errors**: Validate TSV format and JSON syntax
3. **Deployment Errors**: Check Databricks profile and permissions
4. **Scheduling Issues**: Verify Quartz cron syntax

### Debug Commands

```bash
# Check configuration parsing
python -c "import pandas as pd; df = pd.read_csv('config/unified_pipeline_config.tsv', sep='\t'); print(df.head())"

# Validate JSON configuration
python -c "import json; json.loads('{\"test\": \"value\"}'); print('JSON valid')"

# Test resource generation step by step
python -c "from resources.unified_pipeline_generator import UnifiedPipelineGenerator; print('Generator imported successfully')"
```

## 📖 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables Developer Guide](https://docs.databricks.com/data-engineering/delta-live-tables/)
- [Auto Loader Configuration Options](https://docs.databricks.com/ingestion/auto-loader/options.html)
- [Unity Catalog Development](https://docs.databricks.com/data-governance/unity-catalog/)

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Code Standards
- Follow PEP 8 Python style guidelines
- Add comprehensive docstrings for all functions
- Include error handling and validation
- Write tests for new functionality
- Update documentation for new features

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
