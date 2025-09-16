"""
Configuration parser utility for handling TSV configuration files.
"""

import pandas as pd
import json
from typing import Dict, List, Any, Optional
from pathlib import Path


class ConfigParser:
    """Parser for TSV configuration files used by the autoloader framework."""
    
    def __init__(self, config_file: str = "config/ingestion_config.tsv"):
        self.config_file = Path(config_file)
        
    def load_config(self) -> pd.DataFrame:
        """Load configuration from TSV file."""
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file {self.config_file} not found")
        
        return pd.read_csv(self.config_file, sep='\t')
    
    def validate_config(self, df: pd.DataFrame) -> List[str]:
        """Validate configuration data and return list of errors."""
        errors = []
        
        required_columns = [
            'operation_type', 'pipeline_group', 'source_type', 'source_path', 'target_table', 'file_format',
            'trigger_type', 'schedule', 'pipeline_config', 'cluster_size', 'cluster_config', 'notifications', 'custom_expr', 'parameters'
        ]
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return errors  # Return early if required columns are missing
        
        # Validate source types
        valid_source_types = ['file', 'table', 'notebook']
        invalid_source_types = df[~df['source_type'].isin(valid_source_types)]['source_type'].unique()
        if len(invalid_source_types) > 0:
            errors.append(f"Invalid source types: {invalid_source_types}. Valid types: {valid_source_types}")
        
        # Validate file formats (allow empty for manual operations)
        valid_formats = ['json', 'parquet', 'csv', 'avro', 'orc', '']
        # For manual operations, file_format can be empty
        manual_mask = df['operation_type'] == 'manual'
        non_manual_df = df[~manual_mask]
        invalid_formats = non_manual_df[~non_manual_df['file_format'].isin(valid_formats)]['file_format'].unique()
        if len(invalid_formats) > 0:
            errors.append(f"Invalid file formats: {invalid_formats}. Valid formats: {valid_formats}")
        
        # Validate trigger types
        valid_triggers = ['file', 'time']
        invalid_triggers = df[~df['trigger_type'].isin(valid_triggers)]['trigger_type'].unique()
        if len(invalid_triggers) > 0:
            errors.append(f"Invalid trigger types: {invalid_triggers}. Valid types: {valid_triggers}")
        
        # Validate operation types
        valid_operation_types = ['bronze', 'silver', 'gold', 'manual']
        invalid_operation_types = df[~df['operation_type'].isin(valid_operation_types)]['operation_type'].unique()
        if len(invalid_operation_types) > 0:
            errors.append(f"Invalid operation types: {invalid_operation_types}. Valid types: {valid_operation_types}")
        
        # Validate cluster sizes
        valid_cluster_sizes = ['small', 'medium', 'large', 'serverless']
        invalid_cluster_sizes = df[~df['cluster_size'].isin(valid_cluster_sizes)]['cluster_size'].unique()
        if len(invalid_cluster_sizes) > 0:
            errors.append(f"Invalid cluster sizes: {invalid_cluster_sizes}. Valid sizes: {valid_cluster_sizes}")
        
        # Validate paths and autoloader options
        for idx, row in df.iterrows():
            source_type = row['source_type']
            source_path = row['source_path']
            cluster_size = row.get('cluster_size', 'medium')
            cluster_config = row.get('cluster_config', '{}')
            
            # Validate source path based on type
            if source_type == 'file':
                # File sources should have valid file paths (Unity Catalog volumes, S3, ADLS, etc.)
                if not (source_path.startswith('/Volumes/') or 
                       source_path.startswith('s3://') or 
                       source_path.startswith('abfss://') or
                       source_path.startswith('src/')):
                    errors.append(f"Row {idx}: Invalid file path format: {source_path}. Should start with /Volumes/, s3://, abfss://, or src/")
            elif source_type == 'table':
                # Table sources should reference existing tables
                if not ('.' in source_path and not source_path.startswith('/')):
                    errors.append(f"Row {idx}: Invalid table reference format: {source_path}. Should be in format 'catalog.schema.table'")
            elif source_type == 'notebook':
                # Notebook sources should reference notebook files
                if not source_path.startswith('src/'):
                    errors.append(f"Row {idx}: Invalid notebook path format: {source_path}. Should start with src/")
            
            # Validate JSON in pipeline_config
            if pd.notna(row.get('pipeline_config', '')) and row['pipeline_config']:
                try:
                    options = json.loads(row['pipeline_config'])
                    
                    # Validate schema and checkpoint locations if present
                    if 'schema_location' in options:
                        schema_loc = options['schema_location']
                        if schema_loc:
                            if not (schema_loc.startswith('/Volumes/') or 
                                   schema_loc.startswith('s3://') or 
                                   schema_loc.startswith('abfss://')):
                                errors.append(f"Row {idx}: Invalid schema location format: {schema_loc}")
                    
                    if 'checkpoint_location' in options:
                        checkpoint_loc = options['checkpoint_location']
                        if checkpoint_loc:
                            if not (checkpoint_loc.startswith('/Volumes/') or 
                                   checkpoint_loc.startswith('s3://') or 
                                   checkpoint_loc.startswith('abfss://')):
                                errors.append(f"Row {idx}: Invalid checkpoint location format: {checkpoint_loc}")
                            
                except json.JSONDecodeError:
                    errors.append(f"Row {idx}: Invalid JSON in pipeline_config: {row['pipeline_config']}")
            
            # Validate cluster_config JSON
            if pd.notna(cluster_config) and cluster_config and cluster_config.strip() != '':
                try:
                    config = json.loads(cluster_config)
                    
                    # Validate serverless-specific configurations
                    if cluster_size == 'serverless':
                        if 'warehouse_id' not in config:
                            errors.append(f"Row {idx}: serverless cluster requires warehouse_id in cluster_config")
                        elif not config['warehouse_id'] or config['warehouse_id'] == 'your_warehouse_id':
                            errors.append(f"Row {idx}: serverless cluster requires valid warehouse_id, not placeholder")
                            
                except json.JSONDecodeError:
                    errors.append(f"Row {idx}: Invalid JSON in cluster_config: {cluster_config}")
            
            # Validate notifications JSON
            notifications = row.get('notifications', '{}')
            if pd.notna(notifications) and notifications and notifications.strip() != '':
                try:
                    notification_config = json.loads(notifications)
                    
                    # Validate required fields
                    if 'recipients' not in notification_config:
                        errors.append(f"Row {idx}: notifications must contain 'recipients' field")
                    elif not isinstance(notification_config['recipients'], list) or len(notification_config['recipients']) == 0:
                        errors.append(f"Row {idx}: notifications.recipients must be a non-empty list")
                    
                    # Validate on_success and on_failure are boolean
                    for field in ['on_success', 'on_failure']:
                        if field in notification_config and not isinstance(notification_config[field], bool):
                            errors.append(f"Row {idx}: {field} must be a boolean value")
                    
                    # Validate email addresses format (basic validation)
                    for email in notification_config.get('recipients', []):
                        if not isinstance(email, str) or '@' not in email or '.' not in email:
                            errors.append(f"Row {idx}: Invalid email format in recipients: {email}")
                            
                except json.JSONDecodeError:
                    errors.append(f"Row {idx}: Invalid JSON in notifications: {notifications}")
            
            # Validate parameters JSON (only for manual operations)
            parameters = row.get('parameters', '{}')
            operation_type = row.get('operation_type', '')
            
            if operation_type == 'manual' and pd.notna(parameters) and parameters and parameters.strip() != '':
                try:
                    parameters_config = json.loads(parameters)
                    
                    # Validate that parameters is a dictionary
                    if not isinstance(parameters_config, dict):
                        errors.append(f"Row {idx}: parameters must be a JSON object (dictionary)")
                    
                    # Validate parameter values are basic types (string, number, boolean)
                    for param_name, param_value in parameters_config.items():
                        if not isinstance(param_value, (str, int, float, bool)):
                            errors.append(f"Row {idx}: Parameter '{param_name}' must be a string, number, or boolean")
                        
                        # Validate parameter names (basic validation)
                        if not isinstance(param_name, str) or not param_name.strip():
                            errors.append(f"Row {idx}: Parameter names must be non-empty strings")
                            
                except json.JSONDecodeError:
                    errors.append(f"Row {idx}: Invalid JSON in parameters: {parameters}")
            elif operation_type != 'manual' and pd.notna(parameters) and parameters and parameters.strip() != '':
                # Check if it's an empty JSON object
                try:
                    params_dict = json.loads(parameters)
                    if isinstance(params_dict, dict) and len(params_dict) == 0:
                        # Empty JSON object is fine for non-manual operations
                        pass
                    else:
                        # Non-empty parameters should only be used with manual operations
                        errors.append(f"Row {idx}: parameters column should only be used with manual operation type")
                except json.JSONDecodeError:
                    # Invalid JSON - this will be caught by the manual operation validation above
                    pass
        
        return errors
    
    def get_job_configs(self) -> List[Dict[str, Any]]:
        """Get validated job configurations."""
        df = self.load_config()
        errors = self.validate_config(df)
        
        if errors:
            raise ValueError(f"Configuration validation failed:\n" + "\n".join(errors))
        
        configs = []
        for _, row in df.iterrows():
            config = {
                'pipeline_type': row['pipeline_type'],
                'pipeline_group': row['pipeline_group'],
                'source_type': row['source_type'],
                'source_path': row['source_path'],
                'target_table': row['target_table'],
                'file_format': row['file_format'],
                'trigger_type': row['trigger_type'],
                'schedule': row['schedule'],
                'autoloader_options': json.loads(row['autoloader_options']) if pd.notna(row.get('autoloader_options', '')) and row['autoloader_options'] else {},
                'cluster_size': row['cluster_size'],
                'cluster_config': json.loads(row['cluster_config']) if pd.notna(row.get('cluster_config', '')) and row['cluster_config'].strip() else {},
                'email_notifications': json.loads(row['email_notifications']) if pd.notna(row.get('email_notifications', '')) and row['email_notifications'].strip() else {},
                'parameters': json.loads(row['parameters']) if pd.notna(row.get('parameters', '')) and row['parameters'].strip() else {}
            }
            configs.append(config)
        
        return configs
    
    def add_job_config(self, config: Dict[str, Any]) -> None:
        """Add a new job configuration to the TSV file."""
        df = self.load_config()
        
        # Validate the new config
        new_row = pd.DataFrame([config])
        errors = self.validate_config(pd.concat([df, new_row], ignore_index=True))
        
        if errors:
            raise ValueError(f"Configuration validation failed:\n" + "\n".join(errors))
        
        # Add the new row
        new_row = pd.DataFrame([{
            'pipeline_type': config.get('pipeline_type', 'dlt'),  # Default to DLT
            'pipeline_group': config.get('pipeline_group', 'default'),  # Default group
            'source_type': config['source_type'],
            'source_path': config['source_path'],
            'target_table': config['target_table'],
            'file_format': config['file_format'],
            'trigger_type': config['trigger_type'],
            'schedule': config['schedule'],
            'autoloader_options': json.dumps(config.get('autoloader_options', {})),
            'cluster_size': config.get('cluster_size', 'medium'),
            'cluster_config': json.dumps(config.get('cluster_config', {})),
            'email_notifications': json.dumps(config.get('email_notifications', {})),
            'parameters': json.dumps(config.get('parameters', {}))
        }])
        
        updated_df = pd.concat([df, new_row], ignore_index=True)
        updated_df.to_csv(self.config_file, sep='\t', index=False)
    
    def remove_job_config(self, target_table: str) -> bool:
        """Remove a job configuration by target table name."""
        df = self.load_config()
        original_length = len(df)
        
        df = df[df['target_table'] != target_table]
        
        if len(df) == original_length:
            return False  # No matching row found
        
        df.to_csv(self.config_file, sep='\t', index=False)
        return True 