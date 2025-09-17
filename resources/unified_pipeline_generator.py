import pandas as pd
import json
import os
from typing import Dict, Any, List
from databricks.bundles.core import Bundle, Resources, Variable, variables
from databricks.bundles.jobs import Job, Task, PipelineTask, NotebookTask, CronSchedule, JobEmailNotifications
from databricks.bundles.pipelines import Pipeline, PipelineLibrary, NotebookLibrary


@variables
class Variables:
    catalog_name: Variable[str]
    schema_name: Variable[str]
    volume_name: Variable[str]


class UnifiedPipelineGenerator:
    """Generates unified DLT pipelines from a single configuration table."""

    def __init__(self, bundle: Bundle):
        self.bundle = bundle
        self.config_file = "config/unified_pipeline_config.tsv"

    def load_config(self) -> pd.DataFrame:
        """Load configuration from unified TSV file and resolve variables."""
        import sys
        
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file {self.config_file} not found")

        df = pd.read_csv(self.config_file, sep='\t')
        print(f"Loaded {len(df)} pipeline configurations")
        
        # Resolve variables in the configuration
        df = self._resolve_variables_in_config(df)
        
        # Pre-hook validation: Validate configuration after variable resolution
        self._validate_config_pre_hook(df)
        
        return df

    def _resolve_variables_in_config(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resolve bundle variables in the configuration DataFrame."""
        print("Resolving bundle variables in configuration...")
        
        # Get variable values from bundle
        try:
            # Access variables through bundle context as dictionary
            catalog_name = self.bundle.variables["catalog_name"]
            schema_name = self.bundle.variables["schema_name"]
            volume_name = self.bundle.variables["volume_name"]
            print(f"  Using catalog: {catalog_name}")
            print(f"  Using schema: {schema_name}")
            print(f"  Using volume: {volume_name}")
        except Exception as e:
            raise ValueError(f"Failed to access bundle variables: {e}. Make sure variables are defined in databricks.yml")
        
        # Create a copy of the dataframe to avoid modifying the original
        df_resolved = df.copy()
        
        # Define variable mapping
        variable_map = {
            '${var.catalog_name}': catalog_name,
            '${var.schema_name}': schema_name,
            '${var.volume_name}': volume_name
        }
        
        # Replace variables in all string columns, but be careful with NaN values
        for column in df_resolved.columns:
            if df_resolved[column].dtype == 'object':  # String columns
                for old, new in variable_map.items():
                    # Only replace if the value is not NaN
                    mask = df_resolved[column].notna()
                    df_resolved.loc[mask, column] = df_resolved.loc[mask, column].astype(str).str.replace(old, new)
        
        print("  Variable resolution completed")
        return df_resolved

    def _validate_config_pre_hook(self, df: pd.DataFrame) -> None:
        """Pre-hook validation to catch configuration errors early."""
        import sys
        
        # Import the enhanced validator for comprehensive validation
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from utils.validate_config import ConfigValidator
        
        # Create a temporary validator instance for validation
        temp_validator = ConfigValidator(self.config_file)
        
        # Run comprehensive validation on the provided DataFrame
        is_valid, errors = temp_validator.validate_dataframe(df)
        
        if errors:
            # Use stderr for error reporting to ensure it shows up
            error_msg = "[ERROR] Configuration validation failed!\n"
            error_msg += "Pre-hook validation errors:\n"
            for i, error in enumerate(errors, 1):
                error_msg += f"  {i}. {error}\n"
            error_msg += "\n[INFO] Fix these errors before running bundle operations.\n"
            error_msg += "   You can run: python src/utils/validate_config.py\n"
            error_msg += "   Or run: ./scripts/validate.sh\n"
            error_msg += "\n" + "="*60 + "\n"
            error_msg += "DETAILED ERROR REPORT:\n"
            error_msg += "="*60 + "\n"
            for i, error in enumerate(errors, 1):
                error_msg += f"ERROR {i}: {error}\n"
            error_msg += "="*60 + "\n"
            
            # Print to stderr to ensure it shows up
            print(error_msg, file=sys.stderr)
            
            # Also include errors in the exception message
            error_list = "\n".join([f"  {i}. {error}" for i, error in enumerate(errors, 1)])
            raise ValueError(f"Configuration validation failed with {len(errors)} errors:\n{error_list}\n\nRun 'python src/utils/validate_config.py' for detailed validation.")
        
        print("[SUCCESS] Pre-hook validation passed!", file=sys.stderr)

    def create_unified_pipeline(self, pipeline_group: str, group_rows: List[dict]) -> Pipeline:
        """Create a unified pipeline for a pipeline group using static generated notebooks"""
        
        print(f"Creating unified pipeline for group: {pipeline_group}")
        print(f"   Operations in group: {len(group_rows)}")
        
        # Find the appropriate generated notebook for this pipeline group
        notebook_path = f"src/notebooks/generated/unified_{pipeline_group}.py"
        
        # Create the pipeline library
        pipeline_library = PipelineLibrary(
            notebook=NotebookLibrary(
                path=notebook_path
            )
        )
        
        # Get the first row to extract common configuration
        first_row = group_rows[0]
        
        # Extract common configuration
        cluster_size = first_row.get("cluster_size", "medium")
        cluster_config = first_row.get("cluster_config", "")
        email_notifications = first_row.get("email_notifications", "{}")
        
        # Get the fastest schedule from all operations in the group
        unified_schedule = self._get_unified_schedule_for_group(pipeline_group, [pd.Series(row) for row in group_rows])
        
        # Parse email notifications
        email_config = {}
        if email_notifications and email_notifications != "{}":
            try:
                email_config = json.loads(email_notifications)
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Create unified configuration
        unified_config = {
            "pipeline_group": pipeline_group,
            "pipelines.enableDPMForExistingPipeline": "true",
            "pipelines.setMigrationHints": "true",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true",
            "pipelines.trigger.interval": "10 minutes"  # Default fallback
        }
        
        # Add email notifications if specified
        if email_config:
            unified_config["email_notifications"] = json.dumps(email_config)
        
        # Extract and merge pipeline-specific configuration from TSV (only bronze and silver)
        for row in group_rows:
            if row.get('operation_type') in ['silver'] and row.get('pipeline_config'):
                try:
                    pipeline_specific_config = json.loads(row['pipeline_config'])
                    operation_type = row.get('operation_type')
                    print(f"   Extracted {operation_type} pipeline config: {pipeline_specific_config}")
                    
                    # Merge SCD2 and other pipeline-specific settings
                    for key, value in pipeline_specific_config.items():
                        if key not in ['pipeline_group']:  # Avoid overwriting core settings
                            # Convert all values to strings for Databricks compatibility
                            if isinstance(value, (list, dict)):
                                unified_config[key] = str(value)
                            else:
                                unified_config[key] = str(value)
                            print(f"      + Added {operation_type} config: {key} = {unified_config[key]}")
                    
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"      Warning: Error parsing {operation_type} pipeline config: {e}")
                    continue
        
        print(f"   Full unified config: {unified_config}")
        
        # Force all pipelines to serverless for quick testing
        is_serverless = True
        cluster_config_dict = None
        
        print(f"   Applied serverless config: Databricks will handle compute automatically")
        
        # Determine the correct schema for this pipeline (should be silver schema)
        target_schema = "adls_bronze"  # Default fallback
        for row in group_rows:
            if row.get('operation_type') == 'silver':
                target_table = row.get('target_table', '')
                if target_table and '.' in target_table:
                    # Extract schema from target_table (e.g., vbdemos.adls_silver.orders_scd2)
                    parts = target_table.split('.')
                    if len(parts) >= 2:
                        target_schema = parts[1]  # Get the schema part
                        print(f"   Using target schema: {target_schema} (from {target_table})")
                        break
        
        # Extract catalog from target table names (now resolved from variables)
        target_tables = [row.get('target_table', '') for row in group_rows if row.get('target_table')]
        
        if target_tables and '.' in target_tables[0]:
            catalog = target_tables[0].split('.')[0]
            print(f"   Using catalog: {catalog} (from resolved target table)")
        else:
            # Fallback to bundle variable
            catalog = self.bundle.variables["catalog_name"]
            print(f"   Using catalog: {catalog} (from bundle variable)")
        
        # Create the unified pipeline with a resource name for referencing
        pipeline = Pipeline(
            name=f"unified_{pipeline_group}",
            libraries=[pipeline_library],
            configuration=unified_config,
            catalog=catalog,  # Use catalog from configuration
            schema=target_schema,  # Use the correct schema (silver, not bronze)
            tags={
                "deployment_type": "unified_framework",
                "pipeline_group": pipeline_group,
                "cluster_size": "serverless",  # Always serverless for testing
                "framework": "unified-autoloader-pydab"
            },
            edition="ADVANCED",
            development=False,
            continuous=False,  # Set to False for triggered pipelines with schedules
            photon=False,
            serverless=is_serverless,
            clusters=None  # No clusters needed for serverless
        )
        
        # Set the resource name for referencing in jobs
        pipeline.resource_name = f"unified_{pipeline_group}_pipeline"
        
        print(f"   Created pipeline '{pipeline.name}' with configuration:")
        print(f"      - Trigger interval: {unified_config.get('pipelines.trigger.interval', 'Not set')}")
        print(f"      - Continuous mode: {pipeline.continuous}")
        print(f"      - Serverless: {pipeline.serverless}")
        
        return pipeline

    def create_manual_job(self, pipeline_group: str, group_rows: List[dict]) -> Job:
        """Create a manual job with notebook tasks based on TSV configuration."""
        
        print(f"   Creating manual job for group: {pipeline_group}")
        
        # Get the Quartz cron schedule directly from the TSV config
        cron_schedule = self._get_quartz_cron_for_group(pipeline_group, group_rows)
        
        # Get notification configuration from the first manual operation
        notification_config = self._get_notification_config_for_group(pipeline_group, group_rows)
        
        # Get all manual operations and sort them by order if specified
        manual_operations = [row for row in group_rows if row.get('operation_type') == 'manual']
        
        # Sort by order if specified in pipeline_config, otherwise maintain TSV order
        for op in manual_operations:
            try:
                pipeline_config = json.loads(op.get('pipeline_config', '{}'))
                op['_order'] = pipeline_config.get('order', 0)
                op['_depends_on'] = pipeline_config.get('depends_on', None)
            except (json.JSONDecodeError, TypeError):
                op['_order'] = 0
                op['_depends_on'] = None
        
        manual_operations.sort(key=lambda x: x['_order'])
        
        # Create tasks list for manual operations
        tasks = []
        task_key_map = {}  # Map order -> task_key for dependency resolution
        
        for i, manual_op in enumerate(manual_operations):
            notebook_path = manual_op.get('source_path', '')
            if not notebook_path:
                print(f"      Warning: Skipping manual operation {i}: no notebook path specified")
                continue
            
            # Create notebook task
            task_key = f"manual_notebook_{pipeline_group}_{i}"
            
            # Start with framework parameters
            base_parameters = {
                "pipeline_group": pipeline_group,
                "operation_type": "manual",
                "operation_index": i,
                "total_operations": len(manual_operations)
            }
            
            # Add user-defined parameters if they exist
            user_parameters = manual_op.get('parameters', '{}')
            if user_parameters and pd.notna(user_parameters):
                try:
                    user_params = json.loads(user_parameters)
                    base_parameters.update(user_params)
                    print(f"      Added user parameters: {list(user_params.keys())}")
                except json.JSONDecodeError as e:
                    print(f"      Warning: Invalid JSON in parameters for {notebook_path}: {e}")
            
            task = Task(
                task_key=task_key,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=base_parameters
                ),
                run_if="ALL_SUCCESS"
            )
            
            # Handle dependencies based on depends_on configuration
            depends_on_order = manual_op.get('_depends_on')
            if depends_on_order is not None:
                # Find the task that this operation depends on
                dependent_task_key = task_key_map.get(depends_on_order)
                if dependent_task_key:
                    task.depends_on = [{"task_key": dependent_task_key}]
                    print(f"      Added manual notebook task: {notebook_path} (depends on order {depends_on_order}: {dependent_task_key})")
                else:
                    print(f"      Warning: Could not find dependency for order {depends_on_order}, task will run without dependency")
                    print(f"      Added manual notebook task: {notebook_path} (no dependency)")
            else:
                # No explicit dependency, check if this is the first task of its order
                current_order = manual_op.get('_order', 0)
                if current_order == 1 or not any(op.get('_order', 0) < current_order for op in manual_operations):
                    print(f"      Added manual notebook task: {notebook_path} (first task of order {current_order})")
                else:
                    # Find the last task of the previous order
                    previous_order = current_order - 1
                    previous_task_key = task_key_map.get(previous_order)
                    if previous_task_key:
                        task.depends_on = [{"task_key": previous_task_key}]
                        print(f"      Added manual notebook task: {notebook_path} (depends on previous order {previous_order}: {previous_task_key})")
                    else:
                        print(f"      Added manual notebook task: {notebook_path} (no dependency found)")
            
            tasks.append(task)
            
            # Map the order to task key for future dependencies
            current_order = manual_op.get('_order', 0)
            task_key_map[current_order] = task_key
        
        if not tasks:
            print(f"      Error: No valid manual operations found for {pipeline_group}")
            return None
        
        # Create the job with all tasks
        job = Job(
            name=f"manual_{pipeline_group}_job",
            tasks=tasks,
            schedule=CronSchedule(
                quartz_cron_expression=cron_schedule,
                timezone_id="UTC"
            ),
            max_concurrent_runs=1,
            tags={
                "deployment_type": "manual_framework",
                "pipeline_group": pipeline_group,
                "framework": "unified-autoloader-pydab",
                "scheduling": "cron_based"
            }
        )
        
        # Apply notification configuration if specified
        if notification_config:
            job.email_notifications = notification_config
            print(f"      Applied notifications: {notification_config}")
        
        print(f"      Applied cron schedule: {cron_schedule}")
        print(f"      Job will run {len(tasks)} manual notebook tasks")
        
        # Set the resource name for the job
        job.resource_name = f"manual_{pipeline_group}_job"
        
        return job

    def create_scheduled_job(self, pipeline_group: str, pipeline: Pipeline, group_rows: List[dict]) -> Job:
        """Create a scheduled job that runs the pipeline based on TSV cron configuration."""
        
        print(f"   Creating scheduled job for pipeline: {pipeline.name}")
        
        # Get the Quartz cron schedule directly from the TSV config
        cron_schedule = self._get_quartz_cron_for_group(pipeline_group, group_rows)
        
        # Get notification configuration from the silver operation
        notification_config = self._get_notification_config_for_group(pipeline_group, group_rows)
        
        # Create tasks list starting with the pipeline task
        tasks = [
            Task(
                task_key=f"pipeline_task_{pipeline_group}",
                pipeline_task=PipelineTask(
                    pipeline_id=f"${{resources.pipelines.unified_{pipeline_group}_pipeline.id}}"
                ),
                run_if="ALL_SUCCESS"
            )
        ]
        
        # Add gold operation notebook tasks if they exist
        gold_operations = [row for row in group_rows if row.get('operation_type') == 'gold']
        for i, gold_op in enumerate(gold_operations):
            notebook_path = gold_op.get('source_path', '')
            if notebook_path:
                # Create a notebook task for the gold operation with explicit dependency
                gold_task = Task(
                    task_key=f"gold_notebook_{pipeline_group}_{i}",
                    notebook_task=NotebookTask(
                        notebook_path=notebook_path,
                        base_parameters={
                            "pipeline_group": pipeline_group,
                            "operation_type": "gold"
                        }
                    ),
                    depends_on=[{"task_key": f"pipeline_task_{pipeline_group}"}],  # Explicit dependency on pipeline task
                    run_if="ALL_SUCCESS"  # Run after pipeline task succeeds
                )
                tasks.append(gold_task)
                print(f"      Added gold notebook task: {notebook_path} (depends on pipeline_task_{pipeline_group})")
        
        # Create the job with all tasks
        job = Job(
            name=f"unified_{pipeline_group}_job",
            tasks=tasks,
            schedule=CronSchedule(
                quartz_cron_expression=cron_schedule,
                timezone_id="UTC"
            ),
            max_concurrent_runs=1,
            tags={
                "deployment_type": "unified_framework",
                "pipeline_group": pipeline_group,
                "framework": "unified-autoloader-pydab",
                "scheduling": "cron_based"
            }
        )
        
        # Apply notification configuration if specified
        if notification_config:
            job.email_notifications = notification_config
            print(f"      Applied notifications: {notification_config}")
        
        print(f"      Applied cron schedule: {cron_schedule}")
        print(f"      Job will run pipeline: {pipeline.name}")
        
        # Set the resource name for the job
        job.resource_name = f"unified_{pipeline_group}_job"
        
        return job

    def _get_quartz_cron_for_group(self, pipeline_group: str, group_rows: List[dict]) -> str:
        """Get the Quartz cron schedule directly from the TSV config for a pipeline group."""
        
        # Look for the silver operation to get the schedule (for DLT pipelines)
        for row in group_rows:
            if row.get('operation_type') == 'silver':
                schedule = row.get('schedule', '')
                if schedule and pd.notna(schedule):
                    print(f"      Found Quartz cron schedule: '{schedule}'")
                    return schedule
        
        # Look for manual operations to get the schedule
        for row in group_rows:
            if row.get('operation_type') == 'manual':
                schedule = row.get('schedule', '')
                if schedule and pd.notna(schedule):
                    print(f"      Found Quartz cron schedule: '{schedule}'")
                    return schedule
        
        # Fallback to default schedule if none found
        default_schedule = "0 0 6 * * ?"  # Daily at 6 AM in Quartz syntax
        print(f"      Warning: No schedule found, using default: '{default_schedule}'")
        return default_schedule

    def _get_notification_config_for_group(self, pipeline_group: str, group_rows: List[dict]) -> JobEmailNotifications:
        """Get notification configuration from the TSV config for a pipeline group."""
        
        # Look for the silver operation to get the notification config (for DLT pipelines)
        for row in group_rows:
            if row.get('operation_type') == 'silver':
                notifications = row.get('notifications', '')
                if notifications and pd.notna(notifications):
                    try:
                        notification_data = json.loads(notifications)
                        print(f"      Found notification config: {notification_data}")
                        
                        # Create JobEmailNotifications object
                        recipients = notification_data.get('recipients', [])
                        email_notifications = JobEmailNotifications(
                            on_success=recipients if notification_data.get('on_success', False) else [],
                            on_failure=recipients if notification_data.get('on_failure', True) else []
                        )
                        
                        return email_notifications
                        
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"      Warning: Error parsing notification config: {e}")
                        break
        
        # Look for manual operations to get the notification config
        for row in group_rows:
            if row.get('operation_type') == 'manual':
                notifications = row.get('notifications', '')
                if notifications and pd.notna(notifications):
                    try:
                        notification_data = json.loads(notifications)
                        print(f"      Found notification config: {notification_data}")
                        
                        # Create JobEmailNotifications object
                        recipients = notification_data.get('recipients', [])
                        email_notifications = JobEmailNotifications(
                            on_success=recipients if notification_data.get('on_success', False) else [],
                            on_failure=recipients if notification_data.get('on_failure', True) else []
                        )
                        
                        return email_notifications
                        
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"      Warning: Error parsing notification config: {e}")
                        break
        
        # Return None if no valid notification config found
        print(f"      No notification config found, using defaults")
        return None

    # Removed _convert_to_quartz_cron method - now using Quartz cron directly in TSV config

    def _get_unified_schedule_for_group(self, pipeline_group: str, group_rows: List[pd.Series]) -> str:
        """Determine the unified schedule for a pipeline group based on all operations."""
        print(f"      Determining unified schedule for {pipeline_group}")
        
        # Extract schedules from all operations in the group
        schedules = []
        for row in group_rows:
            schedule = row.get('schedule', '')
            if schedule and pd.notna(schedule):
                schedules.append(schedule)
                print(f"        Found schedule: '{schedule}'")
        
        if not schedules:
            print(f"        No schedules found, using default: 10 minutes")
            return "10 minutes"
        
        # Use the fastest schedule (most frequent) to ensure all operations can complete
        intervals = [self._parse_cron_to_interval(schedule) for schedule in schedules]
        fastest_interval = min(intervals, key=lambda x: self._interval_to_minutes(x))
        
        print(f"        Using fastest schedule: {fastest_interval}")
        return fastest_interval
    
    def _interval_to_minutes(self, interval: str) -> int:
        """Convert interval string to minutes for comparison."""
        if not interval:
            return 10  # Default
        
        interval_lower = interval.lower()
        if "minute" in interval_lower:
            try:
                return int(interval.split()[0])
            except:
                return 10
        elif "hour" in interval_lower:
            try:
                return int(interval.split()[0]) * 60
            except:
                return 60
        elif "day" in interval_lower:
            try:
                return int(interval.split()[0]) * 1440
            except:
                return 1440
        else:
            return 10  # Default

    def _get_cluster_config(self, cluster_size: str, cluster_config: str) -> dict:
        """Generate cluster configuration for DLT pipelines based on size."""
        base_config = {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 3,
                "mode": "ENHANCED"
            }
        }
        
        # Apply size-specific configurations
        if cluster_size == "small":
            base_config["node_type_id"] = "Standard_D2s_v5"
            base_config["autoscale"]["max_workers"] = 2
        elif cluster_size == "medium":
            base_config["node_type_id"] = "Standard_D4s_v5"
            base_config["autoscale"]["max_workers"] = 3
        elif cluster_size == "large":
            base_config["node_type_id"] = "Standard_D8s_v5"
            base_config["autoscale"]["max_workers"] = 5
        else:
            base_config["node_type_id"] = "Standard_D4s_v5"
        
        return base_config

    def _parse_cron_to_interval(self, cron_expression: str) -> str:
        """Parse cron expression to get trigger interval for DLT pipelines."""
        if not cron_expression:
            return "10 minutes"
        
        # Clean the cron expression
        cron_expr = cron_expression.strip()
        print(f"        Parsing cron expression: '{cron_expr}'")
        
        # Common cron patterns and their corresponding intervals
        cron_patterns = {
            # Minute-based intervals
            "0 */5 * * *": "5 minutes",
            "0 */10 * * *": "10 minutes",
            "0 */15 * * *": "15 minutes",
            "0 */20 * * *": "20 minutes",
            "0 */25 * * *": "25 minutes",
            "0 */30 * * *": "30 minutes",
            
            # Hour-based intervals
            "0 * * * *": "1 hour",
            "0 */2 * * *": "2 hours",
            "0 */3 * * *": "3 hours",
            "0 */4 * * *": "4 hours",
            "0 */6 * * *": "6 hours",
            "0 */8 * * *": "8 hours",
            "0 */12 * * *": "12 hours",
            
            # Daily at specific times
            "0 0 * * *": "1 day",
            "0 6 * * *": "1 day",  # Daily at 6 AM
            "0 9 * * *": "1 day",  # Daily at 9 AM
            "0 12 * * *": "1 day", # Daily at 12 PM
            "0 18 * * *": "1 day", # Daily at 6 PM
            "0 23 * * *": "1 day", # Daily at 11 PM
            
            # Weekly on specific days
            "0 0 * * 0": "1 week",  # Weekly on Sunday
            "0 0 * * 1": "1 week",  # Weekly on Monday
            "0 0 * * 2": "1 week",  # Weekly on Tuesday
            "0 0 * * 3": "1 week",  # Weekly on Wednesday
            "0 0 * * 4": "1 week",  # Weekly on Thursday
            "0 0 * * 5": "1 week",  # Weekly on Friday
            "0 0 * * 6": "1 week",  # Weekly on Saturday
            
            # Weekly on specific days at specific times
            "0 9 * * 1": "1 week",  # Weekly on Monday at 9 AM
            "0 9 * * 2": "1 week",  # Weekly on Tuesday at 9 AM
            "0 9 * * 3": "1 week",  # Weekly on Wednesday at 9 AM
            "0 9 * * 4": "1 week",  # Weekly on Thursday at 9 AM
            "0 9 * * 5": "1 week",  # Weekly on Friday at 9 AM
            
            # Monthly on specific dates
            "0 0 1 * *": "1 month",   # Monthly on 1st
            "0 0 15 * *": "1 month",  # Monthly on 15th
            "0 0 28 * *": "1 month",  # Monthly on 28th
            "0 0 30 * *": "1 month",  # Monthly on 30th
            
            # Monthly on specific dates at specific times
            "0 12 1 * *": "1 month",  # Monthly on 1st at 12 PM
            "0 12 15 * *": "1 month", # Monthly on 15th at 12 PM
            "0 12 28 * *": "1 month", # Monthly on 28th at 12 PM
            "0 12 30 * *": "1 month"  # Monthly on 30th at 12 PM
        }
        
        # Try exact match first
        if cron_expr in cron_patterns:
            result = cron_patterns[cron_expr]
            print(f"        Exact match: '{cron_expr}' → '{result}'")
            return result
        
        # Try to parse more complex cron expressions
        try:
            # Parse the cron expression to extract the minute interval
            parts = cron_expr.split()
            if len(parts) >= 2:
                minute_part = parts[1]
                if minute_part.startswith("*/"):
                    interval = minute_part[2:]
                    if interval.isdigit():
                        interval_val = int(interval)
                        if interval_val <= 60:
                            result = f"{interval_val} minutes"
                            print(f"        Parsed interval: '{cron_expr}' → '{result}'")
                            return result
        except Exception as e:
            print(f"        Warning: Error parsing cron expression '{cron_expr}': {e}")
        
        # Fallback to default
        result = "10 minutes"
        print(f"        Warning: No match found for '{cron_expr}', using default: '{result}'")
        return result

    def generate_resources(self) -> tuple[List[Pipeline], List[Job]]:
        """Generate unified DLT pipelines and scheduled jobs from configuration."""
        print("Generating unified pipeline resources...")
        
        # Load configuration
        df = self.load_config()
        
        # Group by pipeline_group
        pipeline_groups = df.groupby('pipeline_group')
        
        pipelines = []
        jobs = []
        
        print(f"\nFound {len(pipeline_groups)} pipeline groups:")
        
        # First pass: Create all pipelines (only for non-manual groups)
        pipeline_map = {}  # Map pipeline_group -> pipeline object
        manual_groups = []  # List of manual pipeline groups
        
        for group_name, group_df in pipeline_groups:
            print(f"  [INFO] {group_name}: {len(group_df)} operations")
            
            # Check if this is a manual-only group
            group_operations = group_df.to_dict('records')
            operation_types = [op.get('operation_type') for op in group_operations]
            
            if all(op_type == 'manual' for op_type in operation_types):
                # This is a manual-only group - no pipeline needed
                print(f"  {group_name}: Manual-only group (no DLT pipeline)")
                manual_groups.append(group_name)
                continue
            
            try:
                pipeline = self.create_unified_pipeline(group_name, group_operations)
                pipelines.append(pipeline)
                pipeline_map[group_name] = pipeline
                print(f"  Created unified pipeline: {pipeline.name}")
                
            except Exception as e:
                print(f"  Error creating pipeline for {group_name}: {e}")
                continue
        
        # Second pass: Create jobs for DLT pipelines
        print(f"\nCreating scheduled jobs for {len(pipeline_map)} DLT pipelines...")
        for group_name, pipeline in pipeline_map.items():
            try:
                # Create scheduled job for this pipeline
                job = self.create_scheduled_job(group_name, pipeline, df[df['pipeline_group'] == group_name].to_dict('records'))
                jobs.append(job)
                print(f"  Created scheduled job: {job.name}")
                
            except Exception as e:
                print(f"  Error creating job for {group_name}: {e}")
                continue
        
        # Third pass: Create manual jobs
        print(f"\nCreating manual jobs for {len(manual_groups)} manual groups...")
        for group_name in manual_groups:
            try:
                # Create manual job for this group
                group_rows = df[df['pipeline_group'] == group_name].to_dict('records')
                job = self.create_manual_job(group_name, group_rows)
                if job:
                    jobs.append(job)
                    print(f"  Created manual job: {job.name}")
                else:
                    print(f"  Warning: No valid manual job created for {group_name}")
                
            except Exception as e:
                print(f"  Error creating manual job for {group_name}: {e}")
                continue
        
        print(f"\nGenerated {len(pipelines)} unified pipelines and {len(jobs)} scheduled jobs")
        return pipelines, jobs

    def generate_notebooks(self):
        """Generate notebooks using bundle variables."""
        import sys
        import os
        from pathlib import Path
        
        # Add the notebook generator to the path
        sys.path.insert(0, str(Path(__file__).parent))
        from notebook_generator import generate_pipeline_group_notebook, resolve_variables_in_config
        import pandas as pd
        
        print("[INFO] Generating notebooks with bundle variables...", file=sys.stderr)
        
        # Load configuration
        df = self.load_config()
        
        # Get bundle variables
        bundle_variables = {
            "catalog_name": self.bundle.variables["catalog_name"],
            "schema_name": self.bundle.variables["schema_name"],
            "volume_name": self.bundle.variables["volume_name"]
        }
        
        print(f"[INFO] Using bundle variables: {bundle_variables}", file=sys.stderr)
        
        # Group by pipeline_group
        pipeline_groups = df.groupby('pipeline_group')
        
        # Create output directory
        output_dir = "src/notebooks/generated"
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate notebooks for each pipeline group
        for group_name, group_df in pipeline_groups:
            print(f"  Generating notebook for pipeline group: {group_name}", file=sys.stderr)
            
            # Check if this is a manual-only group - skip DLT notebook generation
            operation_types = group_df['operation_type'].unique()
            if all(op_type == 'manual' for op_type in operation_types):
                print(f"    Skipping DLT notebook generation for manual-only group: {group_name}", file=sys.stderr)
                continue
            
            # Convert group_df to operations_config format
            operations_config = {}
            for _, row in group_df.iterrows():
                operation_type = row['operation_type']
                if operation_type == 'bronze':
                    op_name = f"bronze_{row['target_table'].split('.')[-1].replace('_new', '')}"
                    
                    # Parse the pipeline_config JSON for bronze operations
                    try:
                        pipeline_config = json.loads(row['pipeline_config'])
                    except (json.JSONDecodeError, TypeError):
                        pipeline_config = {}
                    
                    # Start with basic config and add all autoloader options from pipeline_config
                    operations_config[op_name] = {
                        'source_path': row['source_path'],
                        'target_table': row['target_table'],
                        'file_format': row['file_format']
                    }
                    
                    # Add custom_expr if present
                    if pd.notna(row.get('custom_expr', '')) and row['custom_expr']:
                        operations_config[op_name]['custom_expr'] = row['custom_expr']
                    
                    # Add include_file_metadata if present
                    if pd.notna(row.get('include_file_metadata', '')) and row['include_file_metadata']:
                        operations_config[op_name]['include_file_metadata'] = row['include_file_metadata']
                    
                    # Add all autoloader options from the pipeline_config
                    for key, value in pipeline_config.items():
                        if key.startswith("cloudFiles.") or key in ["header", "inferSchema", "multiline"] or key == "schema":
                            operations_config[op_name][key] = value
                elif operation_type == 'silver':
                    op_name = f"silver_{row['target_table'].split('.')[-1].replace('_scd2', '')}"
                    
                    # Parse the pipeline_config JSON for silver operations
                    try:
                        pipeline_config = json.loads(row['pipeline_config'])
                    except (json.JSONDecodeError, TypeError):
                        pipeline_config = {}
                    
                    # Start with basic config and add all DLT options from pipeline_config
                    operations_config[op_name] = {
                        'source_path': row['source_path'],  # This should be the bronze table
                        'target_table': row['target_table']
                    }
                    
                    # Add custom_expr if present
                    if pd.notna(row.get('custom_expr', '')) and row['custom_expr']:
                        operations_config[op_name]['custom_expr'] = row['custom_expr']
                    
                    # Add all DLT options from the pipeline_config
                    for key, value in pipeline_config.items():
                        operations_config[op_name][key] = value
                elif operation_type == 'gold':
                    op_name = f"gold_{row['target_table'].split('.')[-1] if row['target_table'] else 'analytics'}"
                    
                    # For gold operations, source_path contains the notebook path
                    notebook_path = row['source_path']
                    
                    # Parse the pipeline_config JSON for additional gold operation settings
                    try:
                        pipeline_config = json.loads(row['pipeline_config'])
                    except (json.JSONDecodeError, TypeError):
                        pass
                    
                    operations_config[op_name] = {
                        'source_path': notebook_path,  # This contains the notebook path
                        'target_table': row['target_table'],
                        'pipeline_config': row['pipeline_config']  # Keep the full config for gold operations
                    }
            
            # Generate notebooks for this group using the notebook generator
            try:
                generate_pipeline_group_notebook(group_name, operations_config, output_dir, bundle_variables)
                print(f"    Generated notebook: unified_{group_name}.py", file=sys.stderr)
            except Exception as e:
                print(f"    Error generating notebook for {group_name}: {e}", file=sys.stderr)
                continue
        
        print(f"[SUCCESS] Notebook generation completed. Output directory: {output_dir}", file=sys.stderr)


def load_resources(bundle: Bundle) -> Resources:
    """Load unified pipeline resources from configuration."""
    from databricks.bundles.core import Resources
    
    print("Loading unified pipeline resources...")
    
    # Create new resources object
    resources = Resources()
    
    # Generate unified pipeline resources
    generator = UnifiedPipelineGenerator(bundle)
    
    # Generate notebooks first using bundle variables
    generator.generate_notebooks()
    
    # Then generate pipelines and jobs
    pipelines, jobs = generator.generate_resources()
    
    print(f"\nAdding resources to bundle:")
    
    # Add pipelines to resources using their resource names
    for pipeline in pipelines:
        resource_name = getattr(pipeline, 'resource_name', pipeline.name)
        print(f"  + Pipeline: {pipeline.name} (resource: {resource_name})")
        resources.add_pipeline(resource_name, pipeline)
    
    # Add jobs to resources using their resource names
    for job in jobs:
        resource_name = getattr(job, 'resource_name', job.name)
        print(f"  + Job: {job.name} (resource: {resource_name})")
        resources.add_job(resource_name, job)
    
    print(f"\nTotal resources loaded: {len(pipelines)} pipelines + {len(jobs)} jobs = {len(pipelines) + len(jobs)} total")
    
    return resources
