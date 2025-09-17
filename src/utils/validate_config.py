#!/usr/bin/env python3
"""
Configuration validation script for the autoloader framework.
Run this before bundle operations to validate your TSV configuration.

Usage:
    python validate_config.py [--config-file CONFIG_FILE] [--profile PROFILE]
    
Examples:
    python validate_config.py
    python validate_config.py --config-file config/unified_pipeline_config.tsv
    python validate_config.py --profile dev
"""

import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_parser import ConfigParser
from utils.schema_converter import validate_schema_definition
import pandas as pd
import json


class ConfigValidator:
    """Enhanced configuration validator with detailed reporting."""
    
    def __init__(self, config_file: str = "config/unified_pipeline_config.tsv"):
        self.config_file = config_file
        self.parser = ConfigParser(config_file)
        
    def validate_all(self) -> tuple[bool, List[str]]:
        """Perform comprehensive validation and return (is_valid, errors)."""
        errors = []
        
        print(f"🔍 Validating configuration: {self.config_file}")
        print("=" * 60)
        
        # Check if file exists
        if not os.path.exists(self.config_file):
            errors.append(f"Configuration file not found: {self.config_file}")
            return False, errors
        
        try:
            # Load and validate with existing parser
            df = self.parser.load_config()
            return self.validate_dataframe(df)
            
        except Exception as e:
            errors.append(f"Failed to load configuration: {str(e)}")
            return False, errors
    
    def validate_dataframe(self, df) -> tuple[bool, List[str]]:
        """Validate a specific DataFrame and return (is_valid, errors)."""
        errors = []
        
        try:
            # Basic validation with existing parser
            validation_errors = self.parser.validate_config(df)
            
            if validation_errors:
                errors.extend(validation_errors)
            
            # Additional validations
            additional_errors = self._validate_additional_rules(df)
            errors.extend(additional_errors)
            
            # Schema validation for JSON fields
            schema_errors = self._validate_json_schemas(df)
            errors.extend(schema_errors)
            
            # Variable resolution validation
            var_errors = self._validate_variable_usage(df)
            errors.extend(var_errors)
            
        except Exception as e:
            errors.append(f"Failed to validate configuration: {str(e)}")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            print("✅ Configuration validation passed!")
            print(f"   - Loaded {len(df)} pipeline configurations")
            print(f"   - All validations passed successfully")
        else:
            print("❌ Configuration validation failed!")
            print(f"   - Found {len(errors)} validation errors")
            print("\nValidation Errors:")
            for i, error in enumerate(errors, 1):
                print(f"   {i}. {error}")
        
        return is_valid, errors
    
    def _validate_additional_rules(self, df: pd.DataFrame) -> List[str]:
        """Additional validation rules beyond the basic parser."""
        errors = []
        
        # Check for duplicate pipeline groups (this is actually expected, so we'll skip this validation)
        # duplicate_groups = df[df.duplicated(['pipeline_group'], keep=False)]['pipeline_group'].unique()
        # if len(duplicate_groups) > 0:
        #     errors.append(f"Duplicate pipeline groups found: {list(duplicate_groups)}")
        
        # Check for duplicate target tables
        duplicate_tables = df[df.duplicated(['target_table'], keep=False)]['target_table'].unique()
        if len(duplicate_tables) > 0:
            errors.append(f"Duplicate target tables found: {list(duplicate_tables)}")
        
        # Validate cron expressions
        for idx, row in df.iterrows():
            schedule = row.get('schedule', '')
            if schedule and schedule.strip():  # Skip empty schedules
                if not self._is_valid_cron(schedule):
                    errors.append(f"Row {idx}: Invalid cron expression: {schedule}")
        
        return errors
    
    def _validate_json_schemas(self, df: pd.DataFrame) -> List[str]:
        """Validate JSON schema definitions in pipeline_config."""
        errors = []
        
        for idx, row in df.iterrows():
            pipeline_config = row.get('pipeline_config', '{}')
            if pd.notna(pipeline_config) and pipeline_config.strip():
                try:
                    config = json.loads(pipeline_config)
                    
                    # Check for schema definition
                    if 'schema' in config and isinstance(config['schema'], dict):
                        schema_errors = validate_schema_definition(config['schema'])
                        if schema_errors:
                            for schema_error in schema_errors:
                                errors.append(f"Row {idx}: Schema validation error: {schema_error}")
                                
                except json.JSONDecodeError:
                    # This is already caught by the main parser
                    pass
        
        return errors
    
    def _validate_variable_usage(self, df: pd.DataFrame) -> List[str]:
        """Validate that variables are used consistently."""
        errors = []
        
        # Check for unresolved variables
        for idx, row in df.iterrows():
            for col in ['source_path', 'target_table', 'pipeline_config', 'notifications', 'parameters']:
                value = str(row.get(col, ''))
                if '${var.' in value and '}' in value:
                    # Check if it's a valid variable reference
                    import re
                    var_pattern = r'\$\{var\.(\w+)\}'
                    matches = re.findall(var_pattern, value)
                    valid_vars = ['catalog_name', 'schema_name', 'volume_name']
                    
                    for var_name in matches:
                        if var_name not in valid_vars:
                            errors.append(f"Row {idx}: Unknown variable '${{var.{var_name}}}' in {col}. Valid variables: {valid_vars}")
        
        return errors
    
    def _is_valid_cron(self, cron_expr: str) -> bool:
        """Validate Quartz cron expression format (6-field format with optional spaces)."""
        import re
        
        # Normalize the expression - replace multiple spaces with single space and strip
        normalized = re.sub(r'\s+', ' ', cron_expr.strip())
        
        # Split by single space
        parts = normalized.split(' ')
        
        # Must have exactly 6 parts
        if len(parts) != 6:
            return False
        
        # Define patterns for each field (Quartz cron format)
        patterns = [
            r'^(\*|([0-5]?\d)(,([0-5]?\d))*|([0-5]?\d)-([0-5]?\d)|(\*|([0-5]?\d)(,([0-5]?\d))*)/([0-5]?\d))$',  # seconds (0-59)
            r'^(\*|([0-5]?\d)(,([0-5]?\d))*|([0-5]?\d)-([0-5]?\d)|(\*|([0-5]?\d)(,([0-5]?\d))*)/([0-5]?\d))$',  # minutes (0-59)
            r'^(\*|([01]?\d|2[0-3])(,([01]?\d|2[0-3]))*|([01]?\d|2[0-3])-([01]?\d|2[0-3])|(\*|([01]?\d|2[0-3])(,([01]?\d|2[0-3]))*)/([01]?\d|2[0-3]))$',  # hours (0-23)
            r'^(\*|([0-2]?\d|3[01])(,([0-2]?\d|3[01]))*|([0-2]?\d|3[01])-([0-2]?\d|3[01])|(\*|([0-2]?\d|3[01])(,([0-2]?\d|3[01]))*)/([0-2]?\d|3[01])|\?)$',  # day of month (1-31) or ?
            r'^(\*|([0-9]|1[0-2])(,([0-9]|1[0-2]))*|([0-9]|1[0-2])-([0-9]|1[0-2])|(\*|([0-9]|1[0-2])(,([0-9]|1[0-2]))*)/([0-9]|1[0-2])|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$',  # month (1-12) or names
            r'^(\*|([0-6])(,([0-6]))*|([0-6])-([0-6])|(\*|([0-6])(,([0-6]))*)/([0-6])|\?|SUN|MON|TUE|WED|THU|FRI|SAT)$'  # day of week (0-6) or names or ?
        ]
        
        # Validate each part
        for i, part in enumerate(parts):
            # Check if part matches the pattern
            if not re.match(patterns[i], part, re.IGNORECASE):
                return False
        
        return True
    
    def print_summary(self, df: pd.DataFrame):
        """Print a summary of the configuration."""
        print("\n📊 Configuration Summary:")
        print("-" * 30)
        
        # Count by operation type
        op_counts = df['operation_type'].value_counts()
        print("Operation Types:")
        for op_type, count in op_counts.items():
            print(f"  - {op_type}: {count}")
        
        # Count by source type
        source_counts = df['source_type'].value_counts()
        print("\nSource Types:")
        for source_type, count in source_counts.items():
            print(f"  - {source_type}: {count}")
        
        # Count by pipeline group
        group_counts = df['pipeline_group'].value_counts()
        print("\nPipeline Groups:")
        for group, count in group_counts.items():
            print(f"  - {group}: {count}")
        
        # File formats used
        formats = df['file_format'].value_counts()
        print("\nFile Formats:")
        for fmt, count in formats.items():
            if fmt:  # Skip empty formats
                print(f"  - {fmt}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Validate autoloader framework configuration")
    parser.add_argument("--config-file", default="config/unified_pipeline_config.tsv",
                       help="Path to the configuration TSV file")
    parser.add_argument("--profile", help="Databricks profile (for future use)")
    parser.add_argument("--summary", action="store_true", 
                       help="Show configuration summary")
    parser.add_argument("--quiet", action="store_true",
                       help="Only show errors, no success messages")
    
    args = parser.parse_args()
    
    # Validate configuration file exists
    if not os.path.exists(args.config_file):
        print(f"❌ Error: Configuration file not found: {args.config_file}")
        sys.exit(1)
    
    # Create validator and run validation
    validator = ConfigValidator(args.config_file)
    is_valid, errors = validator.validate_all()
    
    # Show summary if requested
    if args.summary and is_valid:
        try:
            df = validator.parser.load_config()
            validator.print_summary(df)
        except Exception as e:
            print(f"Warning: Could not generate summary: {e}")
    
    # Exit with appropriate code
    if is_valid:
        if not args.quiet:
            print("\n🎉 Configuration is ready for bundle operations!")
        sys.exit(0)
    else:
        print(f"\n💡 Fix the errors above and run validation again.")
        print(f"   Command: python validate_config.py --config-file {args.config_file}")
        sys.exit(1)


if __name__ == "__main__":
    main()
