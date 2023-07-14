from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
import os

from functions.tables import get_table_definition

env = 'develop'

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
        import yaml
        with open(r'./settings.yaml') as file:
            settings = yaml.load(file, Loader=yaml.FullLoader)
        databricks_settings = settings['databricks']
        # Build the remote SparkSession using Databricks settings
        spark_session = SparkSession.builder.remote(
            f"{databricks_settings['workspace_name']}:443/;token={databricks_settings['access_token']};x-databricks-cluster-id={databricks_settings['cluster_id']};user_id=123123"
        ).getOrCreate()

    return spark_session


def read_table(read_session: SparkSession, table_name: str, partition: str = '') -> DataFrame:
    """
    Reads a table with the specified table name and optional partition.

    Args:
        read_session (SparkSession): The SparkSession object for reading the table.
        table_name (str): The name of the table to be read.
        partition (str, optional): The partition value (default: '').

    Returns:
        DataFrame: The DataFrame representing the table data.

    Raises:
        None

    """

    table_definition = get_table_definition(table_name)

    if partition != '':
        table_partition = table_definition['partition_by']
        partition_path = f'{table_partition}={partition}'
    else:
        partition_path = partition

    table_location = build_table_path(table_definition['container'], table_definition['location'], partition_path)

    df = read_session.read.format(table_definition['type']).schema(table_definition['columns']).option('header', True).load(table_location)

    if partition != '':
        df = df.withColumn(table_partition, lit(partition))

    return df


def write_table(spark_session: SparkSession, data_frame: DataFrame, table_name: str, partition: str = ''):
    """
    Writes the DataFrame to a table with the specified table name and optional partition.

    Args:
        spark_session (SparkSession): The SparkSession object for writing the table.
        data_frame (DataFrame): The DataFrame to be written to the table.
        table_name (str): The name of the table.
        partition (str, optional): The partition value (default: '').

    Returns:
        None

    Raises:
        ValueError: If the table format validation fails.

    """

    table_definition = get_table_definition(table_name)

    table_check = validate_table_format(spark_session, data_frame, table_name)

    if table_check:
        table_location = build_table_path(table_definition['container'], table_definition['location'], None)
        if partition:
            data_frame.write.partitionBy(partition).mode('overwrite').parquet(table_location)
        else:
            data_frame.coalesce(1).write.mode('overwrite').parquet(table_location)
    else:
        raise ValueError("Table format validation failed.")


def build_table_path(container: str, location: str, partition: str) -> str:
    """
    Builds the path for a table based on the container, location, and optional partition.

    Args:
        container (str): The name of the storage container.
        location (str): The location of the table within the container.
        partition (str): The optional partition value.

    Returns:
        str: The built table path.

    Raises:
        None

    """

    if partition:
        # Return the table path with the specified partition
        return f'abfss://{container}@storagetilt{env}.dfs.core.windows.net/{location}/{partition}'
    else:
        # Return the table path without a partition
        return f'abfss://{container}@storagetilt{env}.dfs.core.windows.net/{location}'


def validate_table_format(spark_session: SparkSession, data_frame: DataFrame, table_name: str) -> bool:
    """
    Validates the format of a DataFrame against the specified table definition.

    Args:
        spark_session (SparkSession): The SparkSession object for interacting with Spark.
        data_frame (DataFrame): The DataFrame to be validated.
        table_name (str): The name of the table to use for validation.

    Returns:
        bool: True if the DataFrame format is valid, False otherwise.

    Raises:
        ValueError: If the initial structure does not match or the head of the table does not match,
                    or if the data quality rules on a column are violated.

    """

    table_definition = get_table_definition(table_name)

    # Create an empty DataFrame with the table definition columns
    if table_definition['partition_by']:
        check_df = spark_session.createDataFrame([], table_definition['columns'].add(table_definition['partition_by'], StringType(), False))
    else:
        check_df = spark_session.createDataFrame([], table_definition['columns'])

    try:
        # Union the empty DataFrame with the provided DataFrame
        check_df = check_df.union(data_frame)
        check_df.head()
    except:
        # An exception occurred, indicating a format mismatch
        raise ValueError("The initial structure can not be joined.")

    # Compare the first row of the original DataFrame with the check DataFrame
    if not data_frame.head().asDict() == check_df.head().asDict():
        # The format of the DataFrame does not match the table definition
        raise ValueError("The head of the table does not match.")

    # Perform additional quality checks on specific columns
    for validate_column, regex_string in table_definition['quality_checks']:
        if data_frame.filter(col(validate_column).rlike(regex_string)).count() > 0:
            # At least one row does not pass the quality check
            raise ValueError(f"The data quality rules on column '{validate_column}' are violated.")

    # All checks passed, the format is valid
    return True

