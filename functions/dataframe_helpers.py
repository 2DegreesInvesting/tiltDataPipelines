import re
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame


def create_map_column(dataframe: DataFrame, dataframe_name: str) -> DataFrame:
    """
    Creates a new column in the dataframe with a map containing the dataframe name as the key and the 'tiltRecordID' column as the value.

    Args:
        spark_session (SparkSession): The Spark session.
        dataframe (DataFrame): The input dataframe.
        dataframe_name (str): The name of the dataframe.

    Returns:
        DataFrame: The dataframe with the new map column.
    """
    dataframe = dataframe.withColumn(
        f'map_{dataframe_name}', F.create_map(
            F.lit(dataframe_name), F.col('tiltRecordID'))
    )
    return dataframe


def create_sha_values(data_frame: DataFrame, col_list: list) -> DataFrame:
    """
    Creates SHA values for the specified columns in the DataFrame.

    Args:
        spark_session (SparkSession): The SparkSession object.
        data_frame (DataFrame): The input DataFrame.
        col_list (list): The list of column names to create SHA values for.

    Returns:
        DataFrame: The DataFrame with the 'shaValue' column added, containing the SHA values.

    """
    data_frame = data_frame.withColumn(
        'shaValue', F.sha2(F.concat_ws('|', *col_list), 256))

    return data_frame


def create_table_path(environment: str, schema: dict, partition_name: str = '') -> str:
    """
    Creates a table path based on the given environment, schema, and optional partition name.

    Args:
        environment (str): The environment name.
        schema (dict): The schema dictionary containing container, location, and partition_column.
        partition_name (str, optional): The name of the partition. Defaults to ''.

    Returns:
        str: The table path.

    """
    if schema['container'] == 'landingzone':
        if partition_name:
            return f"abfss://{schema['container']}@storagetilt{environment}.dfs.core.windows.net/{schema['location']}/{schema['partition_column']}={partition_name}"

        return f"abfss://{schema['container']}@storagetilt{environment}.dfs.core.windows.net/{schema['location']}/"
    return ''


def create_table_name(environment: str, container: str, location: str) -> str:
    """
    Creates a table name by combining the environment, container, and location.

    Args:
        environment (str): The environment name.
        container (str): The container name.
        location (str): The location name.

    Returns:
        str: The formatted table name.
    """
    return f"`{environment}`.`{container}`.`{location.replace('.','')}`"


def clean_column_names(data_frame: DataFrame) -> DataFrame:
    """
    Cleans the column names of a DataFrame by removing special characters,
    replacing spaces with underscores, and removing parentheses.

    Args:
        data_frame (DataFrame): The input DataFrame with column names to be cleaned.

    Returns:
        DataFrame: The DataFrame with cleaned column names.
    """
    for col in data_frame.columns:
        new_col_name = re.sub(r"[-\\\/]", ' ', col)
        new_col_name = re.sub(r'[\(\)]', '', new_col_name)
        new_col_name = re.sub(r'\s+', '_', new_col_name)
        data_frame = data_frame.withColumnRenamed(col, new_col_name)
    return data_frame


def create_catalog_table(table_name: str, schema: dict) -> str:
    """
    Creates a SQL string to recreate a table in Delta Lake format.

    This function constructs a SQL string that can be used to create a table in Delta Lake format.
    The table is created with the provided name and schema. If the schema includes a partition column,
    the table is partitioned by that column.

    Parameters
    ----------
    table_name : str
        The name of the table to be created.
    schema : dict
        The schema of the table to be created. The schema should be a dictionary with a 'columns' key
        containing a list of dictionaries, each representing a column. Each column dictionary should
        have 'name', 'type', and 'nullable' keys. The schema can optionally include a 'partition_column'
        key with the name of the column to partition the table by.

    Returns
    -------
    str
        A SQL string that can be used to create the table in Delta Lake format.
    """

    if not schema['columns']:
        raise ValueError("The provided schema can not be empty")

    create_catalog_table_string = ""

    # Build a SQL string to recreate the table in the most up to date format
    create_catalog_table_string = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    for i in schema['columns']:
        col_info = i.jsonValue()
        col_string = f"`{col_info['name']}` {col_info['type']} {'NOT NULL' if not col_info['nullable'] else ''},"
        create_catalog_table_string += col_string

    create_catalog_table_string = create_catalog_table_string[:-1] + ")"
    create_catalog_table_string += " USING DELTA "
    if schema['partition_column']:
        create_catalog_table_string += f"PARTITIONED BY (`{schema['partition_column']}` STRING)"

    return create_catalog_table_string


def create_catalog_schema(environment: str, schema: dict) -> str:
    """
    Creates a catalog schema if it doesn't already exist.

    Args:
        environment (str): The environment in which the schema should be created.
        schema (dict): A dictionary containing the schema details, including the container name.

    Returns:
        str: The SQL string for creating the catalog schema.
    """

    create_catalog_schema_string = f'CREATE SCHEMA IF NOT EXISTS {environment}.{schema["container"]};'

    return create_catalog_schema_string


def create_catalog_table_owner(table_name: str) -> str:
    """
    Creates a SQL string to set the owner of a table.

    Args:
        table_name (str): The name of the table.

    Returns:
        str: The SQL string to set the owner of the table.
    """

    create_catalog_table_owner_string = f"ALTER TABLE {table_name} SET OWNER TO tiltDevelopers"

    return create_catalog_table_owner_string
