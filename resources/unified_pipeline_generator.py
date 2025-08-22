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

    def create_unified_pipeline(self, pipeline_group: str, group_rows: List[pd.Series]) -> Pipeline:
        """Create a single unified DLT pipeline that handles multiple operations within a group."""
        
        print(f"🔧 Creating unified pipeline for group: {pipeline_group}")
        print(f"   📊 Operations in group: {len(group_rows)}")
        
        # Get common configuration from the first row
        first_row = group_rows[0]
        cluster_size = first_row.get('cluster_size', 'medium')
        cluster_config = first_row.get('cluster_config', '')
        
        # Parse email notifications from the first row
        email_notifications = {}
        if first_row.get('email_notifications') and pd.notna(first_row['email_notifications']):
            try:
                email_notifications = json.loads(first_row['email_notifications'])
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in email_notifications for {pipeline_group}")
        
        # Build unified configuration with all operations
        unified_config = {
            "pipeline_group": pipeline_group,
            "pipelines.enableDPMForExistingPipeline": "true",
            "pipelines.setMigrationHints": "true",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true"
        }
        
        # Add email notifications if specified
        if email_notifications:
            unified_config["email_notifications"] = json.dumps(email_notifications)
        
        # Determine unified scheduling strategy for the pipeline group
        unified_schedule = self._get_unified_schedule_for_group(pipeline_group, group_rows)
        if unified_schedule:
            unified_config["pipelines.trigger.interval"] = unified_schedule
            print(f"   ⏰ Applied unified schedule: {unified_schedule}")
        
        # Build operations configuration
        operations_config = {}
        for row in group_rows:
            pipeline_type = row['pipeline_type']
            operation_name = f"{pipeline_type}_{pipeline_group.split('_')[0]}"  # e.g., bronze_customer, silver_customer
            
            if pipeline_type == 'bronze':
                # Bronze operation configuration
                operations_config[operation_name] = {
                    "source_path": row.get('source_path', ''),
                    "target_table": row.get('target_table', ''),
                    "file_format": row.get('file_format', 'csv'),
                    "schema_location": "",
                    "checkpoint_location": ""
                }
                
                # Parse pipeline_config for bronze
                if row.get('pipeline_config') and pd.notna(row['pipeline_config']):
                    try:
                        bronze_config = json.loads(row['pipeline_config'])
                        operations_config[operation_name].update(bronze_config)
                    except json.JSONDecodeError:
                        print(f"Warning: Invalid JSON in pipeline_config for bronze operation {operation_name}")
                
            elif pipeline_type == 'silver':
                # Silver operation configuration
                operations_config[operation_name] = {
                    "bronze_table": row.get('source_path', ''),  # source_path contains bronze table for silver
                    "target_table": row.get('target_table', ''),
                    "keys": [],
                    "track_history_except_column_list": [],
                    "stored_as_scd_type": "2",
                    "sequence_by": "_ingestion_timestamp"
                }
                
                # Parse pipeline_config for silver
                if row.get('pipeline_config') and pd.notna(row['pipeline_config']):
                    try:
                        silver_config = json.loads(row['pipeline_config'])
                        operations_config[operation_name].update(silver_config)
                    except json.JSONDecodeError:
                        print(f"Warning: Invalid JSON in pipeline_config for silver operation {operation_name}")
        
        # Add operations to unified config
        unified_config["operations_config"] = json.dumps(operations_config)
        print(f"   🔧 Added {len(operations_config)} operations to unified pipeline")
        
        # Handle serverless vs traditional cluster configuration
        cluster_config_dict = None
        is_serverless = cluster_size == 'serverless'
        
        if is_serverless:
            print(f"   🚀 Applied serverless config: Databricks will handle compute automatically")
        else:
            cluster_config_dict = self._get_cluster_config(cluster_size, cluster_config)
            print(f"   🔧 Applied cluster config: {cluster_size} - {cluster_config_dict}")
        
        # Create the unified pipeline
        pipeline = Pipeline(
            name=f"unified_{pipeline_group}",
            libraries=[pipeline_library],
            configuration=unified_config,
            catalog="vbdemos",  # Default catalog
            schema="adls_bronze",  # Default schema for unified pipeline
            tags={
                "deployment_type": "unified_framework",
                "pipeline_group": pipeline_group,
                "cluster_size": cluster_size,
                "framework": "unified-autoloader-pydab"
            },
            edition="ADVANCED",
            development=False,
            continuous=False,  # Use triggered mode for unified pipelines
            photon=False,
            serverless=is_serverless,
            clusters=[cluster_config_dict] if cluster_config_dict else None
        )
        
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
        
        # Common cron patterns and their corresponding intervals
        cron_patterns = {
            "0 */5 * * *": "5 minutes",
            "0 */10 * * *": "10 minutes",
            "0 */15 * * *": "15 minutes",
            "0 */20 * * *": "20 minutes",
            "0 */25 * * *": "25 minutes",
            "0 */30 * * *": "30 minutes",
            "0 * * * *": "1 hour",
            "0 */2 * * *": "2 hours",
            "0 */3 * * *": "3 hours",
            "0 */4 * * *": "4 hours",
            "0 */6 * * *": "6 hours",
            "0 */8 * * *": "8 hours",
            "0 */12 * * *": "12 hours",
            "0 0 * * *": "1 day",
            "0 0 * * 0": "1 week",
            "0 0 1 * *": "1 month"
        }
        
        return cron_patterns.get(cron_expression.strip(), "10 minutes")

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
        for group_name, group_df in pipeline_groups.items():
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
