from databricks.bundles.core import (
    Bundle,
    Resources,
)


def load_resources(bundle: Bundle) -> Resources:
    """
    'load_resources' function is referenced in databricks.yml and is responsible for loading
    bundle resources defined in Python code. This function is called by Databricks CLI during
    bundle deployment. After deployment, this function is not used.
    """

    # Import and use our unified pipeline generator
    from .unified_pipeline_generator import load_resources as load_unified_pipeline_resources
    return load_unified_pipeline_resources(bundle) 