"""
Job mutators for adding common configurations to autoloader jobs.

This module provides job mutators that automatically enhance Databricks jobs with
production-ready configurations including cluster sizing, notifications, and monitoring.

Cluster Size Options:
- add_small_cluster_config: Standard_D2s_v5 (2 vCPUs, 8 GB RAM) - Development/Testing
- add_medium_cluster_config: Standard_D4s_v5 (4 vCPUs, 16 GB RAM) - Production (Default)
- add_large_cluster_config: Standard_D8s_v5 (8 vCPUs, 32 GB RAM) - High-Performance
- add_serverless_cluster_config: Uses Databricks SQL warehouses for serverless compute

Configuration Integration:
Users can specify cluster preferences in the ingestion_config.tsv file:
- cluster_size: "small", "medium", "large", or "serverless"
- cluster_config: JSON string with additional cluster settings

Example cluster_config options:
{
    "warehouse_id": "your_warehouse_id",  # Required for serverless
    "spark_conf": {"spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m"},
    "driver_node_type_id": "Standard_D4s_v5",
    "instance_pool_id": "your_instance_pool_id"
}

Usage Examples:
    # Use default medium cluster
    @job_mutator
    def my_job_mutator(bundle: Bundle, job: Job) -> Job:
        job = add_default_cluster_config(bundle, job)
        return job
    
    # Use specific cluster size with custom config
    @job_mutator  
    def my_custom_job_mutator(bundle: Bundle, job: Job) -> Job:
        custom_config = {"spark_conf": {"spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m"}}
        job = add_large_cluster_config(bundle, job, custom_config)
        return job
    
    # Use serverless cluster
    @job_mutator
    def my_serverless_job_mutator(bundle: Bundle, job: Job) -> Job:
        job = add_serverless_cluster_config(bundle, job, "warehouse_123")
        return job
"""

from dataclasses import replace
from databricks.bundles.core import Bundle, job_mutator
from databricks.bundles.jobs import Job, JobEmailNotifications, JobCluster


@job_mutator
def add_email_notifications(bundle: Bundle, job: Job) -> Job:
    """Add email notifications to jobs that don't have them."""
    if job.email_notifications:
        return job

    email_notifications = JobEmailNotifications.from_dict(
        {
            "on_failure": ["${workspace.current_user.userName}"],
            "on_success": ["${workspace.current_user.userName}"]
        }
    )

    return replace(job, email_notifications=email_notifications)


@job_mutator
def add_default_cluster_config(bundle: Bundle, job: Job, cluster_size: str = "medium", cluster_config: dict = None) -> Job:
    """Add default cluster configuration to jobs with size presets and configurable options.
    
    Args:
        bundle: The Databricks bundle
        job: The job to configure
        cluster_size: Cluster size preset - "small", "medium", "large", or "serverless"
        cluster_config: Additional cluster configuration options from config file
    """
    if job.job_clusters:
        return job  # Job already has cluster config

    # Define cluster size presets with optimal Azure VM types
    cluster_presets = {
        "small": {
            "node_type_id": "Standard_D2s_v5",  # 2 vCPUs, 8 GB RAM - cost-effective dev/test
            "num_workers": 1,
            "spark_version": "16.4.x-scala2.12"
        },
        "medium": {
            "node_type_id": "Standard_D4s_v5",  # 4 vCPUs, 16 GB RAM - balanced production
            "num_workers": 2,
            "spark_version": "16.4.x-scala2.12"
        },
        "large": {
            "node_type_id": "Standard_D8s_v5",  # 8 vCPUs, 32 GB RAM - high-performance
            "num_workers": 3,
            "spark_version": "16.4.x-scala2.12"
        },
        "serverless": {
            # Serverless uses SQL warehouses - no cluster configuration needed
        }
    }
    
    # Validate cluster size
    if cluster_size not in cluster_presets:
        cluster_size = "medium"  # Default to medium if invalid size specified
    
    preset = cluster_presets[cluster_size]
    
    # Handle serverless clusters
    if cluster_size == "serverless":
        if not cluster_config or 'warehouse_id' not in cluster_config:
            raise ValueError("serverless cluster requires warehouse_id in cluster_config")
        
        # For serverless, we don't create a job cluster - we'll use the warehouse
        # The job will need to be configured to use the warehouse instead
        return job
    
    # Create default cluster configuration with essential Spark settings
    spark_conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }

    # Build cluster configuration with defaults and overrides
    cluster_settings = {
        "spark_version": preset["spark_version"],
        "node_type_id": preset["node_type_id"],
        "num_workers": preset["num_workers"],
        "spark_conf": spark_conf
    }
    
    # Override with user-specified cluster config
    if cluster_config:
        # Safe overrides that users can customize
        safe_overrides = [
            'spark_conf', 'driver_node_type_id', 'driver_instance_pool_id', 'instance_pool_id'
        ]
        
        for key, value in cluster_config.items():
            if key in safe_overrides:
                if key == 'spark_conf' and isinstance(value, dict):
                    # Merge user spark_conf with defaults
                    cluster_settings['spark_conf'].update(value)
                else:
                    cluster_settings[key] = value

    job_cluster = JobCluster(
        job_cluster_key=f"autoloader_cluster_{cluster_size}",
        new_cluster=cluster_settings
    )

    return replace(job, job_clusters=[job_cluster])


@job_mutator
def add_small_cluster_config(bundle: Bundle, job: Job, cluster_config: dict = None) -> Job:
    """Add small cluster configuration for development and testing."""
    return add_default_cluster_config(bundle, job, "small", cluster_config)


@job_mutator
def add_medium_cluster_config(bundle: Bundle, job: Job, cluster_config: dict = None) -> Job:
    """Add medium cluster configuration for production workloads."""
    return add_default_cluster_config(bundle, job, "medium", cluster_config)


@job_mutator
def add_large_cluster_config(bundle: Bundle, job: Job, cluster_config: dict = None) -> Job:
    """Add large cluster configuration for high-performance workloads."""
    return add_default_cluster_config(bundle, job, "large", cluster_config)


@job_mutator
def add_serverless_cluster_config(bundle: Bundle, job: Job, warehouse_id: str = None, cluster_config: dict = None) -> Job:
    """Add serverless cluster configuration using Databricks SQL warehouses.
    
    Args:
        bundle: The Databricks bundle
        job: The job to configure
        warehouse_id: The warehouse ID to use for serverless compute
        cluster_config: Additional configuration options from config file
    """
    if job.job_clusters:
        return job  # Job already has cluster config
    
    # Get warehouse_id from cluster_config if not provided directly
    if not warehouse_id and cluster_config:
        warehouse_id = cluster_config.get('warehouse_id')
    
    if not warehouse_id:
        raise ValueError("warehouse_id is required for serverless clusters")
    
    # For serverless, we don't create job clusters - the job will use SQL warehouses
    # We just tag the job to indicate it's serverless and which warehouse to use
    # The actual warehouse configuration happens at the job level, not cluster level
    
    # Add serverless configuration to job tags for reference and monitoring
    if job.tags:
        job.tags.update({
            "deployment_type": "ingestion_framework",  # Ensure this tag is always present
            "compute_type": "serverless",
            "warehouse_id": warehouse_id
        })
    else:
        job.tags = {
            "deployment_type": "ingestion_framework",  # Ensure this tag is always present
            "compute_type": "serverless", 
            "warehouse_id": warehouse_id
        }
    
    return job


@job_mutator
def add_timeout_config(bundle: Bundle, job: Job) -> Job:
    """Add timeout configuration to jobs."""
    if hasattr(job, 'timeout_seconds') and job.timeout_seconds:
        return job  # Job already has timeout config

    # Set default timeout to 1 hour
    return replace(job, timeout_seconds=3600)


@job_mutator
def add_retry_config(bundle: Bundle, job: Job) -> Job:
    """Add retry configuration to jobs."""
    if hasattr(job, 'max_retries') and job.max_retries:
        return job  # Job already has retry config

    # Set default retry configuration
    return replace(job, max_retries=3, retry_on_timeout=True)


@job_mutator
def add_notification_config(bundle: Bundle, job: Job) -> Job:
    """Add comprehensive notification configuration to jobs."""
    if job.email_notifications:
        return job  # Job already has notifications

    # Create comprehensive notification configuration
    email_notifications = JobEmailNotifications.from_dict(
        {
            "on_failure": ["${workspace.current_user.userName}"],
            "on_success": ["${workspace.current_user.userName}"],
            "on_start": ["${workspace.current_user.userName}"]
        }
    )

    return replace(job, email_notifications=email_notifications)


@job_mutator
def add_monitoring_config(bundle: Bundle, job: Job) -> Job:
    """Add monitoring and logging configuration to jobs."""
    # Add tags for monitoring if not present
    monitoring_tags = {
        "deployment_type": "ingestion_framework",  # Ensure this tag is always present
        "framework": "autoloader-pydab",
        "version": "1.0.0",
        "type": "autoloader"
    }
    
    if job.tags:
        # Preserve existing tags and add/update monitoring tags
        job.tags.update(monitoring_tags)
    else:
        job.tags = monitoring_tags

    return job 