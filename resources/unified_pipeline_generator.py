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

    def create_unified_pipeline(self, pipeline_group: str, row: pd.Series) -> Pipeline:
        """Create a single unified DLT pipeline that handles both bronze and silver operations."""
        
        print(f"🔧 Creating unified pipeline for group: {pipeline_group}")
        
        # Get configuration
        cluster_size = row.get('cluster_size', 'medium')
        cluster_config = row.get('cluster_config', '')
        schedule = row.get('schedule', '0 */10 * * *')
        
        # Parse email notifications
        email_notifications = {}
        if row.get('email_notifications') and pd.notna(row['email_notifications']):
            try:
                email_notifications = json.loads(row['email_notifications'])
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in email_notifications for {pipeline_group}")
        
        # Parse pipeline configuration
        pipeline_config = {}
        if row.get('pipeline_config') and pd.notna(row['pipeline_config']):
            try:
                pipeline_config = json.loads(row['pipeline_config'])
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in pipeline_config for {pipeline_group}")
        
        # Create pipeline library
        pipeline_library = PipelineLibrary(
            notebook=NotebookLibrary(
                path="src/notebooks/unified_pipeline.py"
            )
        )
        
        # Build unified configuration
        unified_config = {
            "pipeline_group": pipeline_group,
            "pipeline_config": json.dumps(pipeline_config),
            "pipelines.enableDPMForExistingPipeline": "true",
            "pipelines.setMigrationHints": "true",
            "pipelines.autoOptimize.optimizeWrite": "true",
            "pipelines.autoOptimize.autoCompact": "true"
        }
        
        # Add email notifications if specified
        if email_notifications:
            unified_config["email_notifications"] = json.dumps(email_notifications)
        
        # Add schedule configuration
        interval = self._parse_cron_to_interval(schedule)
        if interval:
            unified_config["pipelines.trigger.interval"] = interval
            print(f"   ⏰ Applied schedule: {schedule} -> {interval}")
        
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
        
        pipelines = []
        jobs = []
        
        print(f"\n📊 Processing {len(df)} pipeline groups:")
        for idx, row in df.iterrows():
            pipeline_group = row['pipeline_group']
            print(f"  📁 {pipeline_group}")
            
            # Create unified pipeline for this group
            try:
                pipeline = self.create_unified_pipeline(pipeline_group, row)
                pipelines.append(pipeline)
                print(f"  ✅ Created unified pipeline: {pipeline.name}")
            except Exception as e:
                print(f"  ❌ Error creating pipeline for {pipeline_group}: {e}")
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
