import pandas as pd
import json
import os
from typing import Dict, Any, List
from databricks.bundles.core import Bundle, Resources, Variable, variables
from databricks.bundles.jobs import Job, Task, PipelineTask, CronSchedule, JobEmailNotifications
from databricks.bundles.pipelines import Pipeline, PipelineLibrary, NotebookLibrary


@variables
class Variables:
    warehouse_id: Variable[str]
    catalog: Variable[str]
    schema: Variable[str]


class UnifiedPipelineGenerator:
    """Generates unified DLT pipelines from a single configuration table."""

    def __init__(self, bundle: Bundle):
        self.bundle = bundle
        self.config_file = "config/unified_pipeline_config.tsv"

    def load_config(self) -> pd.DataFrame:
        """Load configuration from unified TSV file."""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file {self.config_file} not found")

        df = pd.read_csv(self.config_file, sep='\t')
        print(f"📋 Loaded {len(df)} pipeline configurations")
        return df

    def create_unified_pipeline(self, pipeline_group: str, group_rows: List[dict]) -> Pipeline:
        """Create a unified pipeline for a pipeline group using static generated notebooks"""
        
        print(f"🔧 Creating unified pipeline for group: {pipeline_group}")
        print(f"   📊 Operations in group: {len(group_rows)}")
        
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
        
        # Extract and merge pipeline-specific configuration from TSV
        for row in group_rows:
            if row.get('operation_type') == 'silver' and row.get('pipeline_config'):
                try:
                    pipeline_specific_config = json.loads(row['pipeline_config'])
                    print(f"   🔧 Extracted pipeline config: {pipeline_specific_config}")
                    
                    # Merge SCD2 and other pipeline-specific settings
                    for key, value in pipeline_specific_config.items():
                        if key not in ['pipeline_group']:  # Avoid overwriting core settings
                            # Convert all values to strings for Databricks compatibility
                            if isinstance(value, (list, dict)):
                                unified_config[key] = json.dumps(value)
                            else:
                                unified_config[key] = str(value)
                            print(f"      + Added config: {key} = {unified_config[key]}")
                    
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"      ⚠️  Error parsing pipeline config: {e}")
                    continue
        
        print(f"   📋 Full unified config: {unified_config}")
        
        # Force all pipelines to serverless for quick testing
        is_serverless = True
        cluster_config_dict = None
        
        print(f"   🚀 Applied serverless config: Databricks will handle compute automatically")
        
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
                        print(f"   🎯 Using target schema: {target_schema} (from {target_table})")
                        break
        
        # Create the unified pipeline with a resource name for referencing
        pipeline = Pipeline(
            name=f"unified_{pipeline_group}",
            libraries=[pipeline_library],
            configuration=unified_config,
            catalog="vbdemos",  # Default catalog
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
        
        print(f"   🎯 Created pipeline '{pipeline.name}' with configuration:")
        print(f"      - Trigger interval: {unified_config.get('pipelines.trigger.interval', 'Not set')}")
        print(f"      - Continuous mode: {pipeline.continuous}")
        print(f"      - Serverless: {pipeline.serverless}")
        
        return pipeline

    def create_scheduled_job(self, pipeline_group: str, pipeline: Pipeline, group_rows: List[dict]) -> Job:
        """Create a scheduled job that runs the pipeline based on TSV cron configuration."""
        
        print(f"   🔧 Creating scheduled job for pipeline: {pipeline.name}")
        
        # Get the Quartz cron schedule directly from the TSV config
        cron_schedule = self._get_quartz_cron_for_group(pipeline_group, group_rows)
        
        # Get notification configuration from the silver operation
        notification_config = self._get_notification_config_for_group(pipeline_group, group_rows)
        
        # Create the job with pipeline task using resource reference
        # This will resolve to the actual pipeline ID during deployment
        job = Job(
            name=f"unified_{pipeline_group}_job",
            tasks=[
                Task(
                    task_key=f"pipeline_task_{pipeline_group}",
                    pipeline_task=PipelineTask(
                        pipeline_id=f"${{resources.pipelines.unified_{pipeline_group}_pipeline.id}}"
                    ),
                    run_if="ALL_SUCCESS"
                )
            ],
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
            print(f"      📧 Applied notifications: {notification_config}")
        
        print(f"      ⏰ Applied cron schedule: {cron_schedule}")
        print(f"      🎯 Job will run pipeline: {pipeline.name}")
        
        # Set the resource name for the job
        job.resource_name = f"unified_{pipeline_group}_job"
        
        return job

    def _get_quartz_cron_for_group(self, pipeline_group: str, group_rows: List[dict]) -> str:
        """Get the Quartz cron schedule directly from the TSV config for a pipeline group."""
        
        # Look for the silver operation to get the schedule
        for row in group_rows:
            if row.get('operation_type') == 'silver':
                schedule = row.get('schedule', '')
                if schedule and pd.notna(schedule):
                    print(f"      📅 Found Quartz cron schedule: '{schedule}'")
                    return schedule
        
        # Fallback to default schedule if none found
        default_schedule = "0 0 6 * * ?"  # Daily at 6 AM in Quartz syntax
        print(f"      ⚠️  No schedule found, using default: '{default_schedule}'")
        return default_schedule

    def _get_notification_config_for_group(self, pipeline_group: str, group_rows: List[dict]) -> JobEmailNotifications:
        """Get notification configuration from the TSV config for a pipeline group."""
        
        # Look for the silver operation to get the notification config
        for row in group_rows:
            if row.get('operation_type') == 'silver':
                notifications = row.get('notifications', '')
                if notifications and pd.notna(notifications):
                    try:
                        notification_data = json.loads(notifications)
                        print(f"      📧 Found notification config: {notification_data}")
                        
                        # Create JobEmailNotifications object
                        recipients = notification_data.get('recipients', [])
                        email_notifications = JobEmailNotifications(
                            on_success=recipients if notification_data.get('on_success', False) else [],
                            on_failure=recipients if notification_data.get('on_failure', True) else []
                        )
                        
                        return email_notifications
                        
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"      ⚠️  Error parsing notification config: {e}")
                        break
        
        # Return None if no valid notification config found
        print(f"      📧 No notification config found, using defaults")
        return None

    # Removed _convert_to_quartz_cron method - now using Quartz cron directly in TSV config

    def _get_unified_schedule_for_group(self, pipeline_group: str, group_rows: List[pd.Series]) -> str:
        """Determine the unified schedule for a pipeline group based on all operations."""
        print(f"      🔄 Determining unified schedule for {pipeline_group}")
        
        # Extract schedules from all operations in the group
        schedules = []
        for row in group_rows:
            schedule = row.get('schedule', '')
            if schedule and pd.notna(schedule):
                schedules.append(schedule)
                print(f"        📅 Found schedule: '{schedule}'")
        
        if not schedules:
            print(f"        📅 No schedules found, using default: 10 minutes")
            return "10 minutes"
        
        # Use the fastest schedule (most frequent) to ensure all operations can complete
        intervals = [self._parse_cron_to_interval(schedule) for schedule in schedules]
        fastest_interval = min(intervals, key=lambda x: self._interval_to_minutes(x))
        
        print(f"        📅 Using fastest schedule: {fastest_interval}")
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
        print(f"        🔍 Parsing cron expression: '{cron_expr}'")
        
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
            print(f"        ⏰ Exact match: '{cron_expr}' → '{result}'")
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
                            print(f"        ⏰ Parsed interval: '{cron_expr}' → '{result}'")
                            return result
        except Exception as e:
            print(f"        ⚠️  Error parsing cron expression '{cron_expr}': {e}")
        
        # Fallback to default
        result = "10 minutes"
        print(f"        ⚠️  No match found for '{cron_expr}', using default: '{result}'")
        return result

    def generate_resources(self) -> tuple[List[Pipeline], List[Job]]:
        """Generate unified DLT pipelines and scheduled jobs from configuration."""
        print("🚀 Generating unified pipeline resources...")
        
        # Load configuration
        df = self.load_config()
        
        # Group by pipeline_group
        pipeline_groups = df.groupby('pipeline_group')
        
        pipelines = []
        jobs = []
        
        print(f"\n📊 Found {len(pipeline_groups)} pipeline groups:")
        
        # First pass: Create all pipelines
        pipeline_map = {}  # Map pipeline_group -> pipeline object
        for group_name, group_df in pipeline_groups:
            print(f"  📁 {group_name}: {len(group_df)} operations")
            
            try:
                pipeline = self.create_unified_pipeline(group_name, group_df.to_dict('records'))
                pipelines.append(pipeline)
                pipeline_map[group_name] = pipeline
                print(f"  ✅ Created unified pipeline: {pipeline.name}")
                
            except Exception as e:
                print(f"  ❌ Error creating pipeline for {group_name}: {e}")
                continue
        
        # Second pass: Create jobs that reference the actual pipelines
        print(f"\n🔧 Creating scheduled jobs for {len(pipeline_map)} pipelines...")
        for group_name, pipeline in pipeline_map.items():
            try:
                # Create scheduled job for this pipeline
                job = self.create_scheduled_job(group_name, pipeline, df[df['pipeline_group'] == group_name].to_dict('records'))
                jobs.append(job)
                print(f"  ✅ Created scheduled job: {job.name}")
                
            except Exception as e:
                print(f"  ❌ Error creating job for {group_name}: {e}")
                continue
        
        print(f"\n✅ Generated {len(pipelines)} unified pipelines and {len(jobs)} scheduled jobs")
        return pipelines, jobs


def load_resources(bundle: Bundle) -> Resources:
    """Load unified pipeline resources from configuration."""
    from databricks.bundles.core import Resources
    
    print("🚀 Loading unified pipeline resources...")
    
    # Create new resources object
    resources = Resources()
    
    # Generate unified pipeline resources
    generator = UnifiedPipelineGenerator(bundle)
    pipelines, jobs = generator.generate_resources()
    
    print(f"\n📦 Adding resources to bundle:")
    
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
    
    print(f"\n🎯 Total resources loaded: {len(pipelines)} pipelines + {len(jobs)} jobs = {len(pipelines) + len(jobs)} total")
    
    return resources
