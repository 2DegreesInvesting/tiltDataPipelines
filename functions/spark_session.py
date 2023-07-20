from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, count, countDistinct
from pyspark.sql.types import StringType
import os

from functions.tables import get_table_definition
from functions.data_quality import dq_format

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

    if table_definition['type'] == 'csv':
        df = read_session.read.format(table_definition['type']).schema(table_definition['columns']).option('header', True).option("quote", '"').option("multiline", 'True').load(table_location)
    else:
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
    validate_data_quality(spark_session, data_frame, table_name)

    # All checks passed, the format is valid
    return True


def validate_data_quality(spark_session: SparkSession, data_frame: DataFrame, table_name: str) -> bool:
    """
    Validates the data quality of a DataFrame against the specified table definition.

    Args:
        spark_session (SparkSession): The SparkSession object.
        data_frame (DataFrame): The DataFrame to be validated.
        table_name (str): The name of the table for which the data quality is validated.

    Returns:
        bool: True if the data quality checks pass, False otherwise.

    Raises:
        ValueError: If any data quality checks fail.

    """
    table_definition = get_table_definition(table_name)

    for condition_list in table_definition['quality_checks']:
        if condition_list[0] not in ['unique', 'format']:
            raise ValueError(f'The quality check -{condition_list[0]}- is not implemented')

        # Check uniqueness by comparing the total values versus distinct values in a column
        if condition_list[0] == 'unique':
            # Check if the table is partitioned
            if table_definition['partition_by']:
                if data_frame.groupBy(table_definition['partition_by']).agg(count(col(condition_list[1])).alias('count')).collect() != data_frame.groupBy(table_definition['partition_by']).agg(countDistinct(col(condition_list[1])).alias('count')).collect():
                    raise ValueError(f"Column: {condition_list[1]} is not unique along gropued columns.")
            else:
                if data_frame.select(col(condition_list[1])).count() != data_frame.select(col(condition_list[1])).distinct().count():
                    raise ValueError(f"Column: {condition_list[1]} is not unique.")

        # Check if the formatting aligns with a specified pattern
        elif condition_list[0] == 'format':
            if data_frame.filter(dq_format(col(condition_list[1]), condition_list[2])).count() > 0:
                raise ValueError(f'Column: {condition_list[1]} does not align with the specified format {condition_list[2]}')

    return True
