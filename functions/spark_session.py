
import os
import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def create_spark_session() -> SparkSession:
    """
    Creates a SparkSession object using the Databricks configuration.

    Returns:
        SparkSession: The created SparkSession object.

    Raises:
        None

    Notes:
        - By checking if the DATABRICKS_RUNTIME_VERSION is included 
        in the environment variables, it can be identified if the 
        command is run locally or within the Databricks environment.
        If it is run in the databricks environment the general 
        session can be used and if run locally, a remote spark session
        has to be created.

    """

    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        # Create a spark session within Databricks
        spark_session = SparkSession.builder.getOrCreate()
    else:
        with open(r'./settings.yaml') as file:
            settings = yaml.load(file, Loader=yaml.FullLoader)
        databricks_settings = settings['databricks']
        # Build the remote SparkSession using Databricks settings
        spark_session = SparkSession.builder.appName('remote-spark-session').remote(
            f"{databricks_settings['workspace_name']}:443/;token={databricks_settings['access_token']};x-databricks-cluster-id={databricks_settings['cluster_id']};user_id=123123"
        ).getOrCreate()

    # Dynamic Overwrite mode makes sure that other parts of a partition that are not processed are not overwritten as well.
    spark_session.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark_session
