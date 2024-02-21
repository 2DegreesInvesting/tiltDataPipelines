import re
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window


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

    create_catalog_schema_string = f'CREATE SCHEMA IF NOT EXISTS {environment}.{schema["container"]}; ALTER SCHEMA {environment}.{schema["container"]} SET OWNER TO tiltDevelopers;'

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


def apply_scd_type_2(new_table: DataFrame, existing_table: DataFrame) -> DataFrame:
    """
    Applies Slowly Changing Dimension (SCD) Type 2 logic to merge new and existing dataframes.

    Args:
        new_table (DataFrame): The new dataframe containing the updated records.
        existing_table (DataFrame): The existing dataframe containing the current records.

    Returns:
        DataFrame: The merged dataframe with updated records based on SCD Type 2 logic.
    """

    # Determine the processing date
    processing_date = F.current_date()
    future_date = F.lit('2099-12-31')
    from_to_list = [F.col('from_date'), F.col('to_date')]

    # Select the columns that contain values that should be compared
    value_columns = [F.col(col_name) for col_name in new_table.columns if col_name not in [
        'tiltRecordID', 'from_date', 'to_date']]

    old_closed_records = existing_table.filter(
        F.col('to_date') != future_date).select(value_columns + from_to_list)
    old_df = existing_table.filter(F.col('to_date') == future_date).select(
        value_columns + from_to_list)

    # Add the SHA representation of the old records and rename to unique name
    old_df = create_sha_values(old_df, value_columns)
    old_df = old_df.withColumnRenamed('shaValue', 'shaValueOld')

    # Add the SHA representation of the incoming records and rename to unique name
    new_data_frame = create_sha_values(
        new_table, value_columns)
    new_data_frame = new_data_frame.withColumn(
        'from_date', processing_date).withColumn('to_date', F.to_date(future_date))
    new_data_frame = new_data_frame.withColumnRenamed(
        'shaValue', 'shaValueNew')

    # Join the SHA values of both tables together
    combined_df = new_data_frame.select(F.col('shaValueNew')).join(old_df.select(
        'shaValueOld'), on=old_df.shaValueOld == new_data_frame.shaValueNew, how='full')

    # Set the base of all records to the already expired/ closed records
    all_records = old_closed_records
    # Records that did not change are taken from the existing set of data
    identical_records = combined_df.filter(
        (F.col('shaValueOld').isNotNull()) & (F.col('shaValueNew').isNotNull()))
    if identical_records.count() > 0:
        identical_records = combined_df.filter((F.col('shaValueOld').isNotNull()) & (
            F.col('shaValueNew').isNotNull())).join(old_df, on='shaValueOld', how='inner')
        identical_records = identical_records.select(
            value_columns + from_to_list)
        all_records = all_records.union(identical_records)

    # Records that do not exist anymore are taken from the existing set of data
    # Records are closed by filling the to_date column with the current date
    closed_records = combined_df.filter(
        (F.col('shaValueOld').isNotNull()) & (F.col('shaValueNew').isNull()))
    if closed_records.count() > 0:
        closed_records = combined_df.filter((F.col('shaValueOld').isNotNull()) & (
            F.col('shaValueNew').isNull())).join(old_df, on='shaValueOld', how='inner')
        closed_records = closed_records.select(
            value_columns + from_to_list)
        closed_records = closed_records.withColumn(
            'to_date', processing_date)
        all_records = all_records.union(closed_records)

    # Records that are new are taken from the new set of data
    new_records = combined_df.filter(
        (F.col('shaValueOld').isNull()) & (F.col('shaValueNew').isNotNull()))
    if new_records.count() > 0:
        new_records = combined_df.filter((F.col('shaValueOld').isNull()) & (F.col(
            'shaValueNew').isNotNull())).join(new_data_frame, on='shaValueNew', how='inner')
        new_records = new_records.select(value_columns + from_to_list)
        all_records = all_records.union(new_records)

    return all_records


def assign_signalling_id(monitoring_values_df: DataFrame, existing_monitoring_df: DataFrame) -> DataFrame:

    max_issue = existing_monitoring_df.fillna(0, subset='signalling_id') \
        .select(F.max(F.col('signalling_id')).alias('max_signalling_id')).collect()[0]['max_signalling_id']
    if not max_issue:
        max_issue = 0
    existing_monitoring_df = existing_monitoring_df.select([F.col(c).alias(c+'_old') for c in existing_monitoring_df.columns])\
        .select(['signalling_id_old', 'column_name_old', 'check_name_old', 'table_name_old', 'check_id_old']).distinct()
    w = Window().partitionBy('table_name').orderBy(F.col('check_id'))
    join_conditions = [monitoring_values_df.table_name == existing_monitoring_df.table_name_old,
                       monitoring_values_df.column_name == existing_monitoring_df.column_name_old,
                       monitoring_values_df.check_name == existing_monitoring_df.check_name_old,
                       monitoring_values_df.check_id == existing_monitoring_df.check_id_old]
    monitoring_values_intermediate = monitoring_values_df.join(
        existing_monitoring_df, on=join_conditions, how='left')

    existing_signalling_id = monitoring_values_intermediate.where(
        F.col('signalling_id_old').isNotNull())
    non_existing_signalling_id = monitoring_values_intermediate.where(
        F.col('signalling_id_old').isNull())
    non_existing_signalling_id = non_existing_signalling_id.withColumn(
        'signalling_id', F.row_number().over(w)+F.lit(max_issue))

    monitoring_values_intermediate = existing_signalling_id.union(
        non_existing_signalling_id)
    monitoring_values_intermediate = monitoring_values_intermediate.withColumn(
        'signalling_id', F.coalesce(F.col('signalling_id_old'), F.col('signalling_id')))
    monitoring_values_df = monitoring_values_intermediate.select(
        ['signalling_id', 'check_id', 'column_name', 'check_name', 'total_count', 'valid_count', 'table_name'])

    return monitoring_values_df
