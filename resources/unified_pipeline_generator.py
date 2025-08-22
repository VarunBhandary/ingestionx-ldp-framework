import pandas as pd
import json
import os
from typing import Dict, Any, List
from databricks.bundles.core import Bundle, Resources, Variable, variables
from databricks.bundles.jobs import Job, Task, NotebookTask
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
        
        # Add unified scheduling
        if unified_schedule:
            # Ensure the trigger interval is properly formatted for Databricks
            trigger_interval = f'"{unified_schedule}"'  # Wrap in quotes as per Databricks requirements
            unified_config["pipelines.trigger.interval"] = trigger_interval
            print(f"   ⏰ Applied unified schedule: {trigger_interval}")
        
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
        
        # Create the unified pipeline
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
        
        print(f"   🎯 Created pipeline '{pipeline.name}' with configuration:")
        print(f"      - Trigger interval: {unified_config.get('pipelines.trigger.interval', 'Not set')}")
        print(f"      - Continuous mode: {pipeline.continuous}")
        print(f"      - Serverless: {pipeline.serverless}")
        
        return pipeline

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
        """Generate unified DLT pipelines from configuration."""
        print("🚀 Generating unified pipeline resources...")
        
        # Load configuration
        df = self.load_config()
        
        # Group by pipeline_group
        pipeline_groups = df.groupby('pipeline_group')
        
        pipelines = []
        jobs = []
        
        print(f"\n📊 Found {len(pipeline_groups)} pipeline groups:")
        for group_name, group_df in pipeline_groups:
            print(f"  📁 {group_name}: {len(group_df)} operations")
            
            # Create unified pipeline for this group
            try:
                pipeline = self.create_unified_pipeline(group_name, group_df.to_dict('records'))
                pipelines.append(pipeline)
                print(f"  ✅ Created unified pipeline: {pipeline.name}")
            except Exception as e:
                print(f"  ❌ Error creating pipeline for {group_name}: {e}")
                continue
        
        print(f"\n✅ Generated {len(pipelines)} unified pipelines")
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
    
    # Add pipelines to resources
    for pipeline in pipelines:
        print(f"  + Pipeline: {pipeline.name}")
        resources.add_pipeline(pipeline.name, pipeline)
    
    # Add jobs to resources
    for job in jobs:
        print(f"  + Job: {job.name}")
        resources.add_job(job.name, job)
    
    print(f"\n🎯 Total resources loaded: {len(pipelines)} pipelines + {len(jobs)} jobs = {len(pipelines) + len(jobs)} total")
    
    return resources
