from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import os
import pyspark

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


def read_table(read_session: SparkSession, table_name: str, partition: str = '', history: str = 'recent') -> DataFrame:
    """
    Read data from a specified table.

    This function reads data from a table specified by its name and an optional partition. It provides flexibility
    for reading different types of tables and handling historical data.

    Args:
        read_session (SparkSession): The SparkSession object for reading the table.
        table_name (str): The name of the table to be read.
        partition (str, optional): The partition value (default: '').
        history (str, optional): Specify 'recent' to read the most recent data, or 'complete' to read all data history.

    Returns:
        DataFrame: A DataFrame representing the data in the specified location based on either the full or most resent history.

    Raises:
        ValueError: If 'history' argument is not 'recent' or 'complete'.

    Note:
        - The function dynamically determines the file format and schema based on the table definition.
        - If the specified table does not exist yet, it returns an empty DataFrame.
        - If 'history' is set to 'recent' and a 'to_date' column exists, it filters data for the most recent records.

    Example:
        # Read the most recent data from a table named 'my_table'
        recent_data = read_table(spark, 'my_table', history='recent')

        # Read all historical data from a partitioned table named 'products_activities_transformed' for a specific partition 'CutOff'
        all_data = read_table(spark, 'products_activities_transformed', partition='CutOff', history='complete')
    """

    if history not in ['recent','complete']:
        raise ValueError(f"Value {history} is not in valid arguments [recent,complete] for history argument")

    table_definition = get_table_definition(table_name)

    if partition != '':
        table_partition = table_definition['partition_by']
        partition_path = f'{table_partition}={partition}'
    else:
        partition_path = partition

    table_location = build_table_path(table_definition['container'], table_definition['location'], partition_path)

    try:
        if table_definition['type'] == 'csv':
            df = read_session.read.format('csv').schema(table_definition['columns']).option('header', True).option("quote", '"').option("multiline", 'True').load(table_location)
        if table_definition['type'] in ['ecoInvent','tiltData']:
            df = read_session.read.format('csv').schema(table_definition['columns']).option('header', True).option("quote", '~').option('delimiter',';').option("multiline", 'True').load(table_location)
        else:
            df = read_session.read.format(table_definition['type']).schema(table_definition['columns']).option('header', True).load(table_location)
        # Force to load first record of the data to check if it throws an error
        df.head()
    # Try to catch the specific exception where the table to be read does not exist
    except Exception as e:
        # If the table does not exist yet, return an empty data frame
        if "Path does not exist:" in str(e):
            df = read_session.createDataFrame([], table_definition['columns'])
        # If we encounter any other error, raise as the error
        else:
            raise(e)

    if partition != '':
        df = df.withColumn(table_partition, F.lit(partition))

    if history == 'recent' and 'to_date' in df.columns:
        df = df.filter(F.col('to_date')=='2099-12-31')

    df = df.replace('NA', None)
    
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

    # Compare the newly created records with the existing tables
    data_frame = compare_tables(spark_session, data_frame, table_name)

    # Add the SHA value to create a unique ID within tilt
    data_frame = add_record_id(spark_session, data_frame, partition)

    table_check = validate_table_format(spark_session, data_frame, table_name)

    if table_check:
        table_location = build_table_path(table_definition['container'], table_definition['location'], None)
        if partition:
            if table_definition['type'] == 'csv':
                data_frame.write.partitionBy(partition).mode('overwrite').csv(table_location)
            else:
                data_frame.write.partitionBy(partition).mode('overwrite').parquet(table_location)
            
        else:
            if table_definition['type'] == 'csv':
                data_frame.coalesce(1).write.mode('overwrite').csv(table_location)
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
    if not data_frame.orderBy(F.col('tiltRecordID')).head().asDict() == check_df.orderBy(F.col('tiltRecordID')).head().asDict():
        # The format of the DataFrame does not match the table definition
        raise ValueError("The head of the table does not match.")

    # Perform additional quality checks on specific columns
    validated = validate_data_quality(spark_session, data_frame, table_name)

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

    writing_data_frame = data_frame.filter(F.col('to_date')=='2099-12-31')

    for condition_list in table_definition['quality_checks']:
        if condition_list[0] not in ['unique', 'format','in list']:
            raise ValueError(f'The quality check -{condition_list[0]}- is not implemented')

        # Check uniqueness by comparing the total values versus distinct values in a column
        if condition_list[0] == 'unique':
            # Get the list of columns that have to be unique
            unique_columns = condition_list[1]
            # Check if the table is partitioned
            if table_definition['partition_by']:
                # If the table is partitioned, the unique columns have to be unique within one partition
                # Compare the total count of values agains the distinct count of values per partition
                if writing_data_frame.groupBy(table_definition['partition_by']).agg(F.count(*[F.col(column) for column in unique_columns]).alias('count')).collect() != writing_data_frame.groupBy(table_definition['partition_by']).agg(F.countDistinct(*[F.col(column) for column in unique_columns]).alias('count')).collect():
                    # If the values of count and distinct count are not identical, the columns are not unique.
                    raise ValueError(f"Column: {unique_columns} is not unique along grouped columns.")
            else:
                # Compare the total count of values in the tables against the distinct count
                if writing_data_frame.select(*[F.col(column) for column in unique_columns]).count() != writing_data_frame.select(*[F.col(column) for column in unique_columns]).distinct().count():
                    # If the values of count and distinct count are not identical, the columns are not unique.
                    raise ValueError(f"Column: {unique_columns} is not unique.")

        # Check if the formatting aligns with a specified pattern
        elif condition_list[0] == 'format':
            if writing_data_frame.filter(~F.col(condition_list[1]).rlike(condition_list[2])).count() > 0:
                raise ValueError(f'Column: {condition_list[1]} does not align with the specified format {condition_list[2]}')

        elif condition_list[0] == 'in list':
            list_column = condition_list[1][0]
            if writing_data_frame.filter(~F.col(list_column).isin(condition_list[2])).count() > 0:
                raise ValueError(f'Columns: {list_column} have values that are not contained in the following values {";".join(condition_list[2])}')

    return True

def add_record_id(spark_session: SparkSession, data_frame: DataFrame, partition: str = '') -> DataFrame:
    """
    Computes SHA-256 hash values for each row in the DataFrame and adds the hash as a new column 'tiltRecordID'.

    This function takes a SparkSession object and a DataFrame as input, and it computes SHA-256 hash values
    for each row in the DataFrame based on the values in all columns. The computed hash is then appended
    as a new column called 'tiltRecordID' to the DataFrame. The order of columns in the DataFrame will
    affect the generated hash value.

    Args:
        spark_session (SparkSession): The SparkSession instance.
        data_frame (DataFrame): The input DataFrame containing the data to be processed.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with an additional 'tiltRecordID' column, where each row's
        value represents the SHA-256 hash of the respective row's contents.
    """
    # Select all columns that are needed for the creation of a record ID
    sha_columns = [F.col(col_name) for col_name in data_frame.columns if col_name not in ['tiltRecordID','to_date']]

    # Create the SHA256 record ID by concatenating all relevant columns
    data_frame = create_sha_values(spark_session, data_frame, sha_columns)
    data_frame = data_frame.withColumnRenamed('shaValue','tiltRecordID')

    # Reorder the columns, to make sure the partition column is the most right column in the data frame
    if partition:
        col_order = [x for x in data_frame.columns if x not in ['tiltRecordID', partition]] + ['tiltRecordID',partition]
    else:
        col_order = [x for x in data_frame.columns if x not in ['tiltRecordID']] + ['tiltRecordID']
    
    data_frame = data_frame.select(col_order)

    return data_frame

def create_sha_values(spark_session: SparkSession, data_frame: DataFrame, col_list: list) -> DataFrame:

    data_frame = data_frame.withColumn('shaValue',F.sha2(F.concat_ws('|',*col_list),256))

    return data_frame

def compare_tables(spark_session: SparkSession, data_frame: DataFrame, table_name: str) -> DataFrame:
    """
    Compare an incoming DataFrame with an existing table, identifying new, identical, and closed records.

    Parameters:
    - spark_session (SparkSession): The SparkSession used for Spark operations.
    - data_frame (DataFrame): The incoming DataFrame to be compared.
    - table_name (str): The name of the existing table to compare against.

    Returns:
    - DataFrame: A DataFrame containing records that are new, identical, or closed compared to the existing table.

    This function compares an incoming DataFrame with an existing table and identifies the following types of records:
    - New records: Records in the incoming DataFrame that do not exist in the existing table.
    - Identical records: Records with unchanged values present in both the incoming and existing table.
    - Closed records: Records that exist in the existing table but are no longer present in the incoming DataFrame.

    The comparison is based on SHA values of selected columns, and the function returns a DataFrame containing all relevant records.

    Note: The function assumes that the incoming DataFrame and existing table have a common set of columns for comparison.
    """
    # Determine the processing date
    processing_date = F.current_date()
    future_date = F.lit('2099-12-31')
    from_to_list = [F.col('from_date'),F.col('to_date')]
    
    # Select the columns that contain values that should be compared
    value_columns = [F.col(col_name) for col_name in data_frame.columns if col_name not in ['tiltRecordID','from_date','to_date']]

    # Read the already existing table
    old_df = read_table(spark_session,table_name)
    old_closed_records = old_df.filter(F.col('to_date')!=future_date).select(value_columns + from_to_list)
    old_df = old_df.filter(F.col('to_date')==future_date).select(value_columns + from_to_list)

    # Add the SHA representation of the old records and rename to unique name
    old_df = create_sha_values(spark_session, old_df, value_columns)
    old_df = old_df.withColumnRenamed('shaValue','shaValueOld')

    # Add the SHA representation of the incoming records and rename to unique name
    new_data_frame = create_sha_values(spark_session, data_frame, value_columns)
    new_data_frame = new_data_frame.withColumn('from_date',processing_date).withColumn('to_date',F.to_date(future_date))
    new_data_frame = new_data_frame.withColumnRenamed('shaValue','shaValueNew')

    # Join the SHA values of both tables together
    combined_df = new_data_frame.select(F.col('shaValueNew')).join(old_df.select('shaValueOld'),on=old_df.shaValueOld==new_data_frame.shaValueNew,how='full')
    
    # Set the base of all records to the already expired/ closed records
    all_records = old_closed_records
    # Records that did not change are taken from the existing set of data
    identical_records = combined_df.filter((F.col('shaValueOld').isNotNull())&(F.col('shaValueNew').isNotNull()))
    if identical_records.count() > 0:
        identical_records = combined_df.filter((F.col('shaValueOld').isNotNull())&(F.col('shaValueNew').isNotNull())).join(old_df,on='shaValueOld',how='inner')
        identical_records = identical_records.select(value_columns + from_to_list)
        all_records = all_records.union(identical_records)

    # Records that do not exist anymore are taken from the existing set of data
    # Records are closed by filling the to_date column with the current date
    closed_records = combined_df.filter((F.col('shaValueOld').isNotNull())&(F.col('shaValueNew').isNull()))
    if closed_records.count() > 0:
        closed_records = combined_df.filter((F.col('shaValueOld').isNotNull())&(F.col('shaValueNew').isNull())).join(old_df,on='shaValueOld',how='inner')
        closed_records = closed_records.select(value_columns + from_to_list)
        closed_records = closed_records.withColumn('to_date',processing_date)
        all_records = all_records.union(closed_records)

    # Records that are new are taken from the new set of data
    new_records = combined_df.filter((F.col('shaValueOld').isNull())&(F.col('shaValueNew').isNotNull()))
    if new_records.count() > 0:
        new_records = combined_df.filter((F.col('shaValueOld').isNull())&(F.col('shaValueNew').isNotNull())).join(new_data_frame,on='shaValueNew',how='inner')
        new_records = new_records.select(value_columns + from_to_list)
        all_records = all_records.union(new_records)

    return all_records
