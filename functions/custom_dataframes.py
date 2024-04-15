import re
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf

from functions.dataframe_helpers import create_sha_values, create_catalog_schema, create_catalog_table, create_catalog_table_owner, apply_scd_type_2, assign_signalling_id
from functions.signalling_functions import calculate_signalling_issues
from functions.signalling_rules import signalling_checks_dictionary
from functions.tables import get_table_definition
from functions.data_readers import DataReader


class CustomDF(DataReader):
    """
    A custom DataFrame class that wraps a PySpark DataFrame for additional functionality, like data tracing, quality checks and slowly changing dimensions.

    This class provides a convenient way to work with PySpark DataFrames. It includes methods for reading data from a table, writing data to a table, and performing various transformations on the data.

    Attributes:
        _name (str): The name of the table.
        _spark_session (SparkSession): The SparkSession object.
        _schema (dict): The schema of the table, retrieved from the get_table_definition function.
        _partition_name (str): The name of the partition column in the table.
        _history (str): The history setting for the table, either 'complete' or 'recent'.
        _env (str): The environment setting, default is 'develop'.
        _path (str): The path to the table.
        _df (DataFrame): The PySpark DataFrame containing the table data.

    Methods:
        table_path(): Builds the path for a table based on the container, location, and optional partition.
        read_table(): Reads data from a table based on the schema type and applies various transformations.
        write_table(): Writes data from the DataFrame to a table.
    """

    def __init__(self, table_name: str, spark_session: SparkSession, initial_df: DataFrame = None,  partition_name: str = '', history: str = 'recent'):
        self._name = table_name
        self._spark_session = spark_session
        self._schema = get_table_definition(self._name)
        self._partition_name = partition_name
        self._history = history
        self._env = 'develop'
        DataReader.__init__(self, self._spark_session, self._env,
                            self._schema, self._partition_name, self._history)
        if initial_df:
            self._df = initial_df
        else:
            self._df = self.read_source()

    def rename_columns(self, rename_dict):
        for name in rename_dict:
            if name in self._df.columns:
                pass
            else:
                raise ValueError(
                    f"Value {name} does not exist in the specified schema")
        self._df = self._df.withColumnsRenamed(rename_dict)

    def compare_tables(self):
        """
        Compare an incoming DataFrame with an existing table, identifying new, identical, and closed records.

        This function compares an incoming DataFrame with an existing table and identifies the following types of records:
        - New records: Records in the incoming DataFrame that do not exist in the existing table.
        - Identical records: Records with unchanged values present in both the incoming and existing table.
        - Closed records: Records that exist in the existing table but are no longer present in the incoming DataFrame.

        The comparison is based on SHA values of selected columns, and the function returns a DataFrame containing all relevant records.

        Note: The function assumes that the incoming DataFrame and existing table have a common set of columns for comparison.

        Returns:
            DataFrame: A DataFrame containing records that are new, identical, or closed compared to the existing table.
        """

        # Read the already existing table
        old_df = CustomDF(self._name, self._spark_session,
                          None, self._partition_name, 'complete')

        all_records = apply_scd_type_2(self._df, old_df.data)

        return all_records

    def validate_table_format(self):
        """
        Validates the format of a DataFrame against the specified table definition.

        This function checks if the DataFrame's structure matches the table definition, if the first row of the DataFrame matches the first row of the table, and if all rows in the DataFrame are unique. If any of these checks fail, a ValueError is raised.

        Args:
            None. The method uses the instance's SparkSession, DataFrame, and table name.

        Returns:
            bool: True if the DataFrame format is valid, False otherwise.

        Raises:
            ValueError: If the initial structure does not match or the head of the table does not match,
                        or if the data quality rules on a column are violated.
        """
        # Create an empty DataFrame with the table definition columns
        if self._schema['partition_column']:
            check_df = self._spark_session.createDataFrame(
                [], self._schema['columns'])
            check_df = check_df.withColumn(
                self._schema['partition_column'], F.lit(self._partition_name))
        else:
            check_df = self._spark_session.createDataFrame(
                [], self._schema['columns'])

        try:
            # Union the empty DataFrame with the provided DataFrame
            check_df = check_df.union(self._df)
            check_df.head()
        except Exception as e:
            # An exception occurred, indicating a format mismatch
            raise ValueError(
                "The initial structure can not be joined, because: " + str(e)) from e

        # Compare the first row of the original DataFrame with the check DataFrame
        if not self._df.orderBy(F.col('tiltRecordID')).head().asDict() == check_df.orderBy(F.col('tiltRecordID')).head().asDict():
            # The format of the DataFrame does not match the table definition
            raise ValueError("The head of the table does not match.")

        # Check if all of the rows are unique in the table

        if self._df.count() != self._df.distinct().count():
            # The format of the DataFrame does not match the table definition
            raise ValueError("Not all rows in the table are unqiue")

        # Perform additional quality checks on specific columns
        # validated = validate_data_quality(spark_session, data_frame, table_name)
        return True

    def add_record_id(self) -> DataFrame:
        """
        Computes SHA-256 hash values for each row in the DataFrame and adds the hash as a new column 'tiltRecordID'.

        This method computes SHA-256 hash values for each row in the DataFrame based on the values in all columns,
        excluding 'tiltRecordID' and 'to_date'. The computed hash is then appended as a new column called 'tiltRecordID'
        to the DataFrame. The order of columns in the DataFrame will affect the generated hash value.

        If a partition column is specified in the schema, the method ensures that it is the rightmost column in the DataFrame.

        Returns:
            pyspark.sql.DataFrame: A new DataFrame with an additional 'tiltRecordID' column, where each row's
            value represents the SHA-256 hash of the respective row's contents.
        """
        # Select all columns that are needed for the creation of a record ID
        sha_columns = [F.col(col_name) for col_name in self._df.columns if col_name not in [
            'tiltRecordID', 'to_date'] and 'map_' not in col_name]

        # Create the SHA256 record ID by concatenating all relevant columns
        data_frame = create_sha_values(self._df, sha_columns)
        data_frame = data_frame.withColumnRenamed('shaValue', 'tiltRecordID')

        # Reorder the columns, to make sure the partition column is the most right column in the data frame
        if self._partition_name:
            col_order = [x for x in data_frame.columns if x not in [
                'tiltRecordID', self._schema['partition_column']]] + ['tiltRecordID', self._schema['partition_column']]
        else:
            col_order = [x for x in data_frame.columns if x not in [
                'tiltRecordID']] + ['tiltRecordID']

        data_frame = data_frame.select(col_order)

        return data_frame

    def create_catalog_table(self) -> bool:
        """
        Creates a catalog table and schema in the specified environment using the provided table name.
        Additionally, it also sets the owner of the table, so that every developer has the rights to change the structure of the table.

        Returns:
            bool: True if the catalog table is successfully created, False otherwise.
        """

        create_schema_string, set_owner_schema_string = create_catalog_schema(
            self._env, self._schema)
        create_string = create_catalog_table(self._table_name, self._schema)
        set_owner_string = create_catalog_table_owner(self._table_name)

        self._spark_session.sql(create_schema_string)
        self._spark_session.sql(set_owner_schema_string)
        self._spark_session.sql(create_string)
        self._spark_session.sql(set_owner_string)

    def check_signalling_issues(self):
        """
        Perform signalling checks on a specified table and update the monitoring values table.

        This function performs signalling checks on a given table by executing various data quality checks as defined in
        the 'signalling_checks_dictionary'. The results of these checks are recorded in the 'monitoring_values' table.

        Returns:
            None. The method updates the 'monitoring_values' table in-place.

        Note:
            - Signalling checks are defined in the 'signalling_checks_dictionary'.
            - The function reads the specified table and calculates filled values using the 'calculate_filled_values' function.
            - It then iterates through the signalling checks, records the results, and updates the 'monitoring_values' table.
            - The specific checks performed depend on the definitions in the 'signalling_checks_dictionary'.
        """

        signalling_checks = {}
        # Check if there are additional data quality monitoring checks to be executed
        if self._name in signalling_checks_dictionary.keys():
            signalling_checks = signalling_checks_dictionary[self._name]

        # Generate the monitoring values table to be written
        monitoring_values_df = calculate_signalling_issues(
            self._df, signalling_checks, self._spark_session)
        monitoring_values_df = monitoring_values_df.withColumn(
            'table_name', F.lit(self._name))

        existing_monitoring_df = CustomDF(
            'monitoring_values', self._spark_session, partition_name=self._name, history='complete')

        monitoring_values_df = assign_signalling_id(
            monitoring_values_df, existing_monitoring_df.data)

        # Write the table to the location
        complete_monitoring_partition_df = CustomDF(
            'monitoring_values', self._spark_session, partition_name=self._name, initial_df=monitoring_values_df)
        complete_monitoring_partition_df.write_table()

    def write_table(self):
        """
        Writes the DataFrame to a table with the specified table name and optional partition.

        This method writes the DataFrame to a table in the Hive catalog. If a partition is specified, the DataFrame is written to that partition of the table. If the table does not exist, it is created.

        Returns:
            None. The method writes the DataFrame to a table in-place.

        Raises:
            ValueError: If the DataFrame is empty or if the table name is not specified.

        Note:
            - The DataFrame is written in the Delta format.
            - If a partition is specified, the DataFrame is written to that partition of the table.
            - If the table does not exist, it is created.
        """

        self.create_catalog_table()
        print('check 1')
        # Compare the newly created records with the existing tables
        self._df = self.compare_tables()
        print('check 2')

        # Add the SHA value to create a unique ID within tilt
        self._df = self.add_record_id()
        print('check 3', time.time())
        if self._schema['container'] not in ['landingzone', 'monitoring']:
            self.dump_map_column()
        print('check 4', time.time())

        table_check = self.validate_table_format()
        print('check 5', time.time())
        if table_check:
            if self._schema['partition_column']:
                self._df.write.partitionBy(self._schema['partition_column']).mode(
                    'overwrite').format('delta').saveAsTable(self._table_name)
            else:
                self._df.write.mode(
                    'overwrite').format('delta').saveAsTable(self._table_name)
        else:
            raise ValueError("Table format validation failed.")
        print('check 6', time.time())
        if self._name != 'monitoring_values':
            self.check_signalling_issues()
        print('check 7', time.time())

    def dump_map_column(self):

        table_name = f"{self._env}. monitoring.record_tracing"

        trace_schema = get_table_definition("record_trace")

        create_schema_string, set_owner_schema_string = create_catalog_schema(
            self._env, trace_schema)
        create_string = create_catalog_table(
            table_name, trace_schema)
        set_owner_string = create_catalog_table_owner(
            table_name)

        self._spark_session.sql(create_schema_string)
        self._spark_session.sql(set_owner_schema_string)
        self._spark_session.sql(create_string)
        self._spark_session.sql(set_owner_string)

        map_col = [
            col for col in self._df.columns if col.startswith('map_')][0]

        dump_df = self._df.where(F.col('to_date') == '2099-12-31')
        dump_df = dump_df.withColumn('target_table_name', F.lit(
            self._name)).withColumnRenamed('tiltRecordID', 'target_tiltRecordID')
        dump_df = dump_df.select('*', F.explode(map_col).alias('source_table_name', 'source_tiltRecordID_list')
                                 ).select('*', F.explode(F.col('source_tiltRecordID_list')).alias('source_tiltRecordID'))
        dump_df = dump_df.select(F.col('source_tiltRecordID'), F.col('source_table_name'), F.col(
            'target_tiltRecordID'), F.col('target_table_name'))

        df = self._spark_session.read.format(
            'delta').table(table_name)
        df.filter(F.col('target_table_name') == self._name)

        dump_df.write.partitionBy('target_table_name').mode(
            'overwrite').format('delta').saveAsTable(table_name)

        self._df = self._df.drop(F.col(map_col))

    def convert_data_types(self, column_list: list, data_type):
        """
        Converts the data type of specified columns in the DataFrame.

        Args:
            column_list (list): A list of column names in the DataFrame for which the data type needs to be converted.
            data_type (DataType): The target data type to which the column data should be converted.

        Returns:
            None. The method updates the DataFrame in-place.
        """
        for column in column_list:
            self._df = self._df.withColumn(
                column, F.col(column).cast(data_type))

    def custom_join(self, custom_other: 'CustomDF', custom_on: str = None, custom_how: str = None):
        """
        Joins the current CustomDF instance with another CustomDF instance based on the provided conditions.

        Args:
            other (CustomDF): The other CustomDF instance to join with.
            custom_on (str, optional): The column to join on. Defaults to None.
            custom_how (str, optional): Type of join (inner, outer, left, right). Defaults to "inner".

        Raises:
            Exception: If the `custom_on` column does not exist in both dataframes.

        Returns:
            CustomDF: A new CustomDF instance that is the result of the join.
        """
        copy_df = custom_other.data
        self._df = self._df.join(copy_df, custom_on, custom_how)
        self._df = self._df.withColumn(
            f'map_{self._name}', F.map_concat(
                F.col(f'map_{self._name}'), F.col(f'map_{custom_other.name}'))
        )
        self._df = self._df.drop(F.col(f'map_{custom_other.name}'))

        return CustomDF(self._name, self._spark_session, self._df, self._partition_name, self._history)

    def custom_select(self, columns: list):
        """
        Selects the specified columns from the DataFrame.

        Args:
            columns (list): A list of column names to select from the DataFrame.

        Returns:
            CustomDF: A new CustomDF instance with the selected columns.
        """
        self._df = self._df.select(
            *columns, F.col(f"map_{self._schema['location']}_{self._schema['container']}"))
        return CustomDF(self._name, self._spark_session, self._df, self._partition_name, self._history)

    def custom_distinct(self):
        """
        Returns the distinct rows from the DataFrame.

        Returns:
            CustomDF: A new CustomDF instance with distinct rows.
        """
        cols = [F.col(col)
                for col in self._df.columns if not col.startswith('map_')]

        map_col = [
            col for col in self._df.columns if col.startswith('map_')][0]

        df = self._df.select(*cols, F.explode(
            F.col(map_col)).alias('exploded_table', 'exploded_list'))\
            .select(*cols, F.col('exploded_table'), F.explode(F.col('exploded_list')).alias('exploded'))

        self._df = df\
            .groupBy(cols + ['exploded_table'])\
            .agg(F.collect_set(F.col('exploded')).alias('exploded_fold'))\
            .groupBy(cols)\
            .agg(F.collect_list(F.col('exploded_table')).alias('table_list'), F.collect_list(F.col('exploded_fold')).alias('fold_list'))\
            .withColumn(map_col, F.map_from_arrays(F.col('table_list'), F.col('fold_list')))\
            .select(*cols, F.col(map_col))

        return CustomDF(self._name, self._spark_session, self._df, self._partition_name, self._history)

    def custom_drop(self, columns: list):
        """
        Drops the specified columns from the DataFrame.

        Args:
            columns (list): A list of column names to drop from the DataFrame.

        Returns:
            CustomDF: A new CustomDF instance with the specified columns dropped.
        """
        self._df = self._df.drop(*columns)
        return CustomDF(self._name, self._spark_session, self._df, self._partition_name, self._history)

    @ property
    def data(self):
        """
        Returns the Spark DataFrame associated with the CustomDF instance.

        Returns:
            DataFrame: The Spark DataFrame associated with the CustomDF instance.
        """
        return self._df

    @ data.setter
    def data(self, new_df: DataFrame):
        """
        Sets the DataFrame associated with the CustomDF instance.

        This method allows you to replace the DataFrame associated with the CustomDF instance with a new DataFrame.

        Args:
            new_df (DataFrame): The new DataFrame to associate with the CustomDF instance.

        Returns:
            None. The method replaces the DataFrame in-place.

        Note:
            - The new DataFrame should have the same schema as the original DataFrame.
            - The new DataFrame replaces the original DataFrame in-place.
        """
        self._df = new_df

    @ property
    def name(self):
        """
        Returns the name of the CustomDF instance.

        Returns:
            str: The name of the CustomDF instance.
        """
        return self._name
