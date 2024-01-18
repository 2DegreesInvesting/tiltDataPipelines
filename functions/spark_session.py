from functions.signalling_functions import calculate_signalling_issues
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType
import os

from functions.tables import get_table_definition
from functions.signalling_rules import signalling_checks_dictionary


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

    # Dynamic Overwrite mode makes sure that other parts of a partition that are not processed are not overwritten as well.
    spark_session.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark_session


def read_table(read_session: SparkSession, table_name: str, partition_name: str = '', history: str = 'recent') -> DataFrame:
    """
    Read data from a specified table.

    This function reads data from a table specified by its name and an optional partition. It provides flexibility
    for reading different types of tables and handling historical data.

    Args:
        read_session (SparkSession): The SparkSession object for reading the table.
        table_name (str): The name of the table to be read.
        partition_name (str, optional): The partition value (default: '').
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

    if history not in ['recent', 'complete']:
        raise ValueError(
            f"Value {history} is not in valid arguments [recent,complete] for history argument")

    table_definition = get_table_definition(table_name)

    if partition_name != '':
        partition_column = table_definition['partition_column']
        partition_path = f'{partition_column}={partition_name}'
    else:
        partition_path = partition_name

    table_location = build_table_path(
        table_definition['container'], table_definition['location'], partition_path)

    try:
        if table_definition['type'] == 'csv':
            df = read_session.read.format('csv').schema(table_definition['columns']).option(
                'header', True).option("quote", '"').option("multiline", 'True').load(table_location)
        if table_definition['type'] in ['ecoInvent', 'tiltData']:
            df = read_session.read.format('csv').schema(table_definition['columns']).option('header', True).option(
                "quote", '~').option('delimiter', ';').option("multiline", 'True').load(table_location)
        else:
            df = read_session.read.format(table_definition['type']).schema(
                table_definition['columns']).option('header', True).load(table_location)
        # Force to load first record of the data to check if it throws an error
        df.head()
    # Try to catch the specific exception where the table to be read does not exist
    except Exception as e:
        # If the table does not exist yet, return an empty data frame
        if "Path does not exist:" in str(e):
            df = read_session.createDataFrame([], table_definition['columns'])
        # If we encounter any other error, raise as the error
        else:
            raise (e)

    if partition_name != '':
        df = df.withColumn(partition_column, F.lit(partition_name))

    if history == 'recent' and 'to_date' in df.columns:
        df = df.filter(F.col('to_date') == '2099-12-31')

    # Replace empty values with None/null
    replacement_dict = {'NA': None, 'nan': None}
    df = df.replace(replacement_dict, subset=df.columns)

    return df


def write_table(spark_session: SparkSession, data_frame: DataFrame, table_name: str, partition_name: str = ''):
    """
    Writes the DataFrame to a table with the specified table name and optional partition.

    Args:
        spark_session (SparkSession): The SparkSession object for writing the table.
        data_frame (DataFrame): The DataFrame to be written to the table.
        table_name (str): The name of the table.
        partition_name (str, optional): The partition_name value (default: '').

    Returns:
        None

    Raises:
        ValueError: If the table format validation fails.

    """

    table_definition = get_table_definition(table_name)

    # Compare the newly created records with the existing tables
    data_frame = compare_tables(
        spark_session, data_frame, table_name, partition_name)

    # Add the SHA value to create a unique ID within tilt
    partition_column = table_definition['partition_column']
    data_frame = add_record_id(spark_session, data_frame, partition_column)

    table_check = validate_table_format(spark_session, data_frame, table_name)

    if table_check:
        table_location = build_table_path(
            table_definition['container'], table_definition['location'], None)
        if partition_column:
            if table_definition['type'] == 'csv':
                data_frame.write.partitionBy(partition_column).mode(
                    'overwrite').csv(table_location)
            else:
                data_frame.write.partitionBy(partition_column).mode(
                    'overwrite').parquet(table_location)

        else:
            if table_definition['type'] == 'csv':
                data_frame.coalesce(1).write.mode(
                    'overwrite').csv(table_location)
            else:
                data_frame.coalesce(1).write.mode(
                    'overwrite').parquet(table_location)
    else:
        raise ValueError("Table format validation failed.")

    create_catalog_tables(spark_session, table_name)

    if table_name != 'monitoring_values':
        check_signalling_issues(spark_session, table_name)


def build_table_path(container: str, location: str, partition_column_and_name: str) -> str:
    """
    Builds the path for a table based on the container, location, and optional partition.

    Args:
        container (str): The name of the storage container.
        location (str): The location of the table within the container.
        partition_column_and_name (str): The optional string that points to the location of the specified partition.

    Returns:
        str: The built table path.

    Raises:
        None

    """
    if partition_column_and_name:
        # Return the table path with the specified partition
        return f'abfss://{container}@storagetilt{env}.dfs.core.windows.net/{location}/{partition_column_and_name}'
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
    if table_definition['partition_column']:
        check_df = spark_session.createDataFrame([], table_definition['columns'].add(
            table_definition['partition_column'], StringType(), False))
    else:
        check_df = spark_session.createDataFrame(
            [], table_definition['columns'])

    try:
        # Union the empty DataFrame with the provided DataFrame
        check_df = check_df.union(data_frame)
        check_df.head()
    except Exception as e:
        # An exception occurred, indicating a format mismatchs
        raise ValueError(
            "The initial structure can not be joined, because:" + str(e))

    # Compare the first row of the original DataFrame with the check DataFrame
    if not data_frame.orderBy(F.col('tiltRecordID')).head().asDict() == check_df.orderBy(F.col('tiltRecordID')).head().asDict():
        # The format of the DataFrame does not match the table definition
        raise ValueError("The head of the table does not match.")

    # Check if all of the rows are unique in the table
    if data_frame.count() != data_frame.distinct().count():
        # The format of the DataFrame does not match the table definition
        raise ValueError("Not all rows in the table are unqiue")

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

    writing_data_frame = data_frame.filter(F.col('to_date') == '2099-12-31')

    for condition_list in table_definition['quality_checks']:
        if condition_list[0] not in ['unique', 'format', 'in list']:
            raise ValueError(
                f'The quality check -{condition_list[0]}- is not implemented')

        # Check uniqueness by comparing the total values versus distinct values in a column
        if condition_list[0] == 'unique':
            # Get the list of columns that have to be unique
            unique_columns = condition_list[1]
            # Check if the table is partitioned
            if table_definition['partition_column']:
                # If the table is partitioned, the unique columns have to be unique within one partition
                # Compare the total count of values agains the distinct count of values per partition
                if writing_data_frame.groupBy(table_definition['partition_column']).agg(F.count(*[F.col(column) for column in unique_columns]).alias('count')).collect() != writing_data_frame.groupBy(table_definition['partition_column']).agg(F.countDistinct(*[F.col(column) for column in unique_columns]).alias('count')).collect():
                    # If the values of count and distinct count are not identical, the columns are not unique.
                    raise ValueError(
                        f"Column: {unique_columns} is not unique along grouped columns.")
            else:
                # Compare the total count of values in the tables against the distinct count
                if writing_data_frame.select(*[F.col(column) for column in unique_columns]).count() != writing_data_frame.select(*[F.col(column) for column in unique_columns]).distinct().count():
                    # If the values of count and distinct count are not identical, the columns are not unique.
                    raise ValueError(
                        f"Column: {unique_columns} is not unique.")

        # Check if the formatting aligns with a specified pattern
        elif condition_list[0] == 'format':
            if writing_data_frame.filter(~F.col(condition_list[1]).rlike(condition_list[2])).count() > 0:
                raise ValueError(
                    f'Column: {condition_list[1]} does not align with the specified format {condition_list[2]}')

        elif condition_list[0] == 'in list':
            list_column = condition_list[1][0]
            if writing_data_frame.filter(~F.col(list_column).isin(condition_list[2])).count() > 0:
                raise ValueError(
                    f'Columns: {list_column} have values that are not contained in the following values {";".join(condition_list[2])}')

    return True


def add_record_id(spark_session: SparkSession, data_frame: DataFrame, partition_column: str = '') -> DataFrame:
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
    sha_columns = [F.col(col_name) for col_name in data_frame.columns if col_name not in [
        'tiltRecordID', 'to_date']]

    # Create the SHA256 record ID by concatenating all relevant columns
    data_frame = create_sha_values(spark_session, data_frame, sha_columns)
    data_frame = data_frame.withColumnRenamed('shaValue', 'tiltRecordID')

    # Reorder the columns, to make sure the partition column is the most right column in the data frame
    if partition_column:
        col_order = [x for x in data_frame.columns if x not in [
            'tiltRecordID', partition_column]] + ['tiltRecordID', partition_column]
    else:
        col_order = [x for x in data_frame.columns if x not in [
            'tiltRecordID']] + ['tiltRecordID']

    data_frame = data_frame.select(col_order)

    return data_frame


def compare_tables(spark_session: SparkSession, data_frame: DataFrame, table_name: str, partition_name: str) -> DataFrame:
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
    from_to_list = [F.col('from_date'), F.col('to_date')]

    # Select the columns that contain values that should be compared
    value_columns = [F.col(col_name) for col_name in data_frame.columns if col_name not in [
        'tiltRecordID', 'from_date', 'to_date']]

    # Read the already existing table
    old_df = read_table(spark_session, table_name, partition_name, 'complete')
    old_closed_records = old_df.filter(
        F.col('to_date') != future_date).select(value_columns + from_to_list)
    old_df = old_df.filter(F.col('to_date') == future_date).select(
        value_columns + from_to_list)

    # Add the SHA representation of the old records and rename to unique name
    old_df = create_sha_values(spark_session, old_df, value_columns)
    old_df = old_df.withColumnRenamed('shaValue', 'shaValueOld')

    # Add the SHA representation of the incoming records and rename to unique name
    new_data_frame = create_sha_values(
        spark_session, data_frame, value_columns)
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
        closed_records = closed_records.select(value_columns + from_to_list)
        closed_records = closed_records.withColumn('to_date', processing_date)
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


def check_signalling_issues(spark_session: SparkSession, table_name: str) -> None:
    """
    Perform signalling checks on a specified table and update the monitoring values table.

    This function performs signalling checks on a given table by executing various data quality checks as defined in
    the 'signalling_checks_dictionary'. The results of these checks are recorded in the 'monitoring_values' table.

    Args:
        spark_session (SparkSession): The SparkSession to use for reading and writing data.
        table_name (str): The name of the table to perform signalling checks on.

    Returns:
        None

    Note:
        - Signalling checks are defined in the 'signalling_checks_dictionary'.
        - The function reads the specified table and calculates filled values using the 'calculate_filled_values' function.
        - It then iterates through the signalling checks, records the results, and updates the 'monitoring_values' table.
        - The specific checks performed depend on the definitions in the 'signalling_checks_dictionary'.
    """

    dataframe = read_table(spark_session, table_name)
    dummy_signalling_df = read_table(spark_session, 'dummy_quality_check')

    signalling_checks = {}
    # Check if there are additional data quality monitoring checks to be executed
    if table_name in signalling_checks_dictionary.keys():
        signalling_checks = signalling_checks_dictionary[table_name]

    # Generate the monitoring values table to be written
    monitoring_values_df = calculate_signalling_issues(
        spark_session, dataframe, signalling_checks, dummy_signalling_df)
    monitoring_values_df = monitoring_values_df.withColumn(
        'table_name', F.lit(table_name))

    existing_monitoring_df = read_table(
        spark_session, 'monitoring_values', table_name)
    max_issue = existing_monitoring_df.fillna(0, subset='signalling_id') \
        .select(F.max(F.col('signalling_id')).alias('max_signalling_id')).collect()[0]['max_signalling_id']
    if not max_issue:
        max_issue = 0
    existing_monitoring_df = existing_monitoring_df.select([F.col(c).alias(c+'_old') for c in existing_monitoring_df.columns])\
        .select(['signalling_id_old', 'column_name_old', 'check_name_old', 'table_name_old', 'check_id_old'])
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

    # Write the table to the location
    write_table(spark_session, monitoring_values_df,
                'monitoring_values', table_name)


def create_catalog_tables(spark_session: SparkSession, table_name: str) -> bool:
    """
    Create or replace an external Hive table in the specified Hive container using the given SparkSession.

    This function generates and executes SQL statements to create or replace an external Hive table. It first drops
    the table if it already exists and then creates the table based on the provided table definition. The table
    definition is obtained using the 'get_table_definition' function.

    Args:
        spark_session (SparkSession): The SparkSession to use for executing SQL statements.
        table_name (str): The name of the table to create or replace.

    Returns:
        bool: True if the table creation was successful, False otherwise.

    Note:
        - The table is created as an external table.
        - The table definition is obtained using the 'get_table_definition' function.
        - The table is created with the specified columns, data types, and partitioning (if applicable).
        - If the table already exists, it is dropped and recreated.
    """

    table_definition = get_table_definition(table_name)

    # Drop the table definition in the unity catalog
    delete_string = f"DROP TABLE IF EXISTS `{table_definition['container']}`.`default`.`{table_definition['location'].replace('.','')}`"

    # Build a SQL string to recreate the table in the most up to date format
    create_string = f"CREATE EXTERNAL TABLE IF NOT EXISTS `{table_definition['container']}`.`default`.`{table_definition['location'].replace('.','')}` ("

    for i in table_definition['columns']:
        col_info = i.jsonValue()
        col_string = f"`{col_info['name']}` {col_info['type']} {'NOT NULL' if not col_info['nullable'] else ''},"
        create_string += col_string

    table_path = build_table_path(
        table_definition['container'], table_definition['location'], None)
    create_string = create_string[:-1] + ")"
    create_string += f" USING {table_definition['type']} LOCATION '{table_path}'"
    if table_definition['partition_column']:
        create_string += f" PARTITIONED BY (`{table_definition['partition_column']}` STRING)"
    set_owner_string = f"ALTER TABLE `{table_definition['container']}`.`default`.`{table_definition['location'].replace('.','')}` SET OWNER TO tiltDevelopers"

    # Try and delete the already existing definition of the table
    try:
        spark_session.sql(delete_string)
    # Try to catch the specific exception where the users is not the owner of the table and can thus not delete the table
    except Exception as e:
        # If the user is not the owner, set the user to be the owner and then delete the table
        if "User is not an owner of Table" in str(e):
            import yaml
            with open(r'./settings.yaml') as file:
                settings = yaml.load(file, Loader=yaml.FullLoader)
            databricks_settings = settings['databricks']
            spark_session.sql(set_owner_string)
            spark_session.sql(delete_string)
        # If we encounter any other error, raise as the error
        else:
            raise (e)
    spark_session.sql(create_string)
    spark_session.sql(set_owner_string)

    return True
