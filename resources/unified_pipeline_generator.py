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
        """Create a unified DLT pipeline that handles both bronze and silver operations."""
        
        print(f"🔧 Creating unified pipeline for group: {pipeline_group}")
        print(f"   📊 Rows in group: {len(group_rows)}")
        
        # Get the first row for common configuration
        first_row = group_rows[0]
        pipeline_type = first_row['pipeline_type']
        cluster_size = first_row.get('cluster_size', 'medium')
        cluster_config = first_row.get('cluster_config', '')
        
        # Parse email notifications
        email_notifications = {}
        if first_row.get('email_notifications') and pd.notna(first_row['email_notifications']):
            try:
                email_notifications = json.loads(first_row['email_notifications'])
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in email_notifications for {pipeline_group}")
        
        # Create pipeline library
        pipeline_library = PipelineLibrary(
            notebook=NotebookLibrary(
                path="src/notebooks/unified_pipeline.py"
            )
        )
        
        # Build unified configuration
        unified_config = {
            "pipeline_type": pipeline_type,
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
            print(f"   ⏰ Applied unified schedule for {pipeline_group}: {unified_schedule}")
        
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
            schema="adls_bronze" if pipeline_type == "bronze" else "adls_silver",  # Default schema
            tags={
                "deployment_type": "unified_framework",
                "pipeline_group": pipeline_group,
                "pipeline_type": pipeline_type,
                "cluster_size": cluster_size,
                "framework": "unified-autoloader-pydab"
            },
            edition="ADVANCED",
            development=False,
            continuous=False,  # Use triggered mode for all unified pipelines
            photon=False,
            serverless=is_serverless,
            clusters=[cluster_config_dict] if cluster_config_dict else None
        )
        
        return pipeline

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

    def _get_unified_schedule_for_group(self, pipeline_group: str, group_rows: List[pd.Series]) -> str:
        """Determine the unified schedule for a pipeline group based on bronze and silver operations."""
        print(f"      🔄 Determining unified schedule for {pipeline_group}")
        
        # Extract schedules from the group
        bronze_schedule = None
        silver_schedule = None
        
        for row in group_rows:
            if row['pipeline_type'] == 'bronze':
                bronze_schedule = row.get('schedule', '')
            elif row['pipeline_type'] == 'silver':
                silver_schedule = row.get('schedule', '')
        
        # If both bronze and silver have schedules, use the faster one (more frequent)
        if bronze_schedule and silver_schedule:
            bronze_interval = self._parse_cron_to_interval(bronze_schedule)
            silver_interval = self._parse_cron_to_interval(silver_schedule)
            
            # Parse intervals to minutes for comparison
            bronze_minutes = self._interval_to_minutes(bronze_interval)
            silver_minutes = self._interval_to_minutes(silver_interval)
            
            if bronze_minutes <= silver_minutes:
                print(f"        📅 Using bronze schedule: {bronze_schedule} -> {bronze_interval}")
                return bronze_interval
            else:
                print(f"        📅 Using silver schedule: {silver_schedule} -> {silver_interval}")
                return silver_interval
        
        # If only one has a schedule, use that one
        elif bronze_schedule:
            interval = self._parse_cron_to_interval(bronze_schedule)
            print(f"        📅 Using bronze schedule: {bronze_schedule} -> {interval}")
            return interval
        elif silver_schedule:
            interval = self._parse_cron_to_interval(silver_schedule)
            print(f"        📅 Using silver schedule: {silver_schedule} -> {interval}")
            return interval
        
        # Default fallback
        print(f"        📅 Using default schedule: 10 minutes")
        return "10 minutes"
    
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
        for group_name, group_df in pipeline_groups:
            print(f"  📁 {group_name}: {len(group_df)} configurations")
            
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
