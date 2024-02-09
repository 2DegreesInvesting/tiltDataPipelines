import re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from delta.tables import DeltaTable

from functions.dataframe_helpers import create_map_column, create_sha_values
from functions.signalling_functions import calculate_signalling_issues
from functions.signalling_rules import signalling_checks_dictionary
from functions.tables import get_table_definition


class CustomDF:
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
        if self._schema['container'] == 'landingzone' or self._name == 'dummy_quality_check':
            self._path = f"abfss://{self._schema['container']}@storagetilt{self._env}.dfs.core.windows.net/{self._schema['location']}/"
        else:
            self._path = ""
        self._table_name = f"`{self._env}`.`{self._schema['container']}`.`{self._schema['location'].replace('.','')}`"
        if self._partition_name:
            self._partition_path = f"{self._schema['partition_column']}={self._partition_name}"
        else:
            self._partition_path = ''
        if initial_df:
            self._df = initial_df
        else:
            self._df = self.read_table()

    def read_table(self):
        """
        Reads data from a table based on the schema type and applies various transformations.

        This method first validates the history attribute and constructs the partition path based on the partition name attribute. It then builds the table location path and defines a dictionary of read functions for different schema types.

        It attempts to read the data using the appropriate read function based on the schema type. If the table does not exist, it creates an empty dataframe with the same schema. If any other error occurs during reading, it raises the error.

        If a partition name is provided, it adds a column with the partition name to the dataframe. If the history attribute is set to 'recent' and the dataframe has a 'to_date' column, it filters the dataframe to only include rows where 'to_date' is '2099-12-31'.

        It then replaces any 'NA' or 'nan' values in the dataframe with None. Finally, it calls the 'create_map_column' function to create a map column in the dataframe and returns the transformed dataframe.

        Returns:
            DataFrame: The transformed dataframe.
        """

        if self._history not in ['recent', 'complete']:
            raise ValueError(
                f"Value {self._history} is not in valid arguments [recent,complete] for history argument")

        read_functions = {
            'csv': lambda: self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '"').option("multiline", 'True').load(self._path + self._partition_path),
            'ecoInvent': lambda: self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '~').option('delimiter', ';').option("multiline", 'True').load(self._path + self._partition_path),
            'tiltData': lambda: self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '~').option('delimiter', ';').option("multiline", 'True').load(self._path + self._partition_path),
            # DeltaTable.forName(self._spark_session, self._table_name).toDF(),
            'delta': lambda: self._spark_session.read.format('delta').table(self._table_name),
            'parquet': lambda: self._spark_session.read.format(self._schema['type']).schema(self._schema['columns']).option('header', True).load(self._path + self._partition_path)
        }

        try:
            df = read_functions.get(
                self._schema['type'], read_functions['delta'])()
            if self._schema['type'] == 'delta' and self._partition_name != '':
                df = df.where(
                    F.col(self._schema['partition_column']) == self._partition_name)
            df.head()  # Force to load first record of the data to check if it throws an error
        except Exception as e:
            if "Path does not exist:" in str(e) or f"`{self._schema['container']}`.`{self._schema['location']}` is not a Delta table" in str(e):
                # If the table does not exist yet, return an empty data frame
                df = self._spark_session.createDataFrame(
                    [], self._schema['columns'])
            else:
                raise  # If we encounter any other error, raise as the error

        if self._partition_name != '':
            df = df.withColumn(
                self._schema['partition_column'], F.lit(self._partition_name))

        if self._history == 'recent' and 'to_date' in df.columns:
            df = df.filter(F.col('to_date') == '2099-12-31')

        # Replace empty values with None/null
        replacement_dict = {'NA': None, 'nan': None}
        df = df.replace(replacement_dict, subset=df.columns)

        if self._schema['container'] != 'landingzone' and self._name != 'dummy_quality_check':
            df = create_map_column(self._spark_session, df, self._name)

        # This generically renames columns to remove special characters so that they can be written into managed storage
        if self._schema['container'] == 'landingzone':
            for col in df.columns:
                new_col_name = re.sub(r"[-\\\/]", ' ', col)
                new_col_name = re.sub(r'[\(\)]', '', new_col_name)
                new_col_name = re.sub(r'\s+', '_', new_col_name)
                df = df.withColumnRenamed(col, new_col_name)

        return df

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
        # Determine the processing date
        processing_date = F.current_date()
        future_date = F.lit('2099-12-31')
        from_to_list = [F.col('from_date'), F.col('to_date')]

        # Select the columns that contain values that should be compared
        value_columns = [F.col(col_name) for col_name in self._df.columns if col_name not in [
            'tiltRecordID', 'from_date', 'to_date']]
        print(value_columns)
        # Read the already existing table
        old_df = CustomDF(self._name, self._spark_session,
                          None, self._partition_name, 'complete')
        old_closed_records = old_df.data.filter(
            F.col('to_date') != future_date).select(value_columns + from_to_list)
        old_df = old_df.data.filter(F.col('to_date') == future_date).select(
            value_columns + from_to_list)

        # Add the SHA representation of the old records and rename to unique name
        old_df = create_sha_values(self._spark_session, old_df, value_columns)
        old_df = old_df.withColumnRenamed('shaValue', 'shaValueOld')

        # Add the SHA representation of the incoming records and rename to unique name
        new_data_frame = create_sha_values(
            self._spark_session, self._df, value_columns)
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
            'tiltRecordID', 'to_date']]

        # Create the SHA256 record ID by concatenating all relevant columns
        data_frame = create_sha_values(
            self._spark_session, self._df, sha_columns)
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
        Create or replace an external Hive table in the specified Hive container.

        This method generates and executes SQL statements to create or replace an external Hive table. It first drops
        the table if it already exists and then creates the table based on the provided table definition. The table
        definition is obtained using the 'get_table_definition' function.

        The table is created with the specified columns, data types, and partitioning (if applicable). If the table 
        already exists, it is dropped and recreated.

        Returns:
            bool: True if the table creation was successful, False otherwise.

        Note:
            - The table is created as an external table.
            - The table definition is obtained using the 'get_table_definition' function.
            - The table is created with the specified columns, data types, and partitioning (if applicable).
            - If the table already exists, it is dropped and recreated.
        """
        schema_string = f'CREATE SCHEMA IF NOT EXISTS {self._env}.{self._schema["container"]};'

        # Build a SQL string to recreate the table in the most up to date format
        create_string = f"CREATE TABLE IF NOT EXISTS {self._table_name} ("

        for i in self._schema['columns']:
            col_info = i.jsonValue()
            col_string = f"`{col_info['name']}` {col_info['type']} {'NOT NULL' if not col_info['nullable'] else ''},"
            create_string += col_string

        create_string = create_string[:-1] + ")"
        create_string += " USING DELTA "
        if self._schema['partition_column']:
            create_string += f" PARTITIONED BY (`{self._schema['partition_column']}` STRING)"
        set_owner_string = f"ALTER TABLE {self._table_name} SET OWNER TO tiltDevelopers"

        self._spark_session.sql(schema_string)
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

        dummy_signalling_df = CustomDF(
            'dummy_quality_check', self._spark_session)
        signalling_checks = {}
        # Check if there are additional data quality monitoring checks to be executed
        if self._name in signalling_checks_dictionary.keys():
            signalling_checks = signalling_checks_dictionary[self._name]

        # Generate the monitoring values table to be written
        monitoring_values_df = calculate_signalling_issues(
            self._df, signalling_checks, dummy_signalling_df.data)
        monitoring_values_df = monitoring_values_df.withColumn(
            'table_name', F.lit(self._name))

        existing_monitoring_df = CustomDF(
            'monitoring_values', self._spark_session, partition_name=self._name, history='complete')
        max_issue = existing_monitoring_df.data.fillna(0, subset='signalling_id') \
            .select(F.max(F.col('signalling_id')).alias('max_signalling_id')).collect()[0]['max_signalling_id']
        if not max_issue:
            max_issue = 0
        existing_monitoring_df = existing_monitoring_df.data.select([F.col(c).alias(c+'_old') for c in existing_monitoring_df.data.columns])\
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
        # Compare the newly created records with the existing tables

        self.create_catalog_table()

        self._df = self.compare_tables()

        # Add the SHA value to create a unique ID within tilt
        self._df = self.add_record_id()

        table_check = self.validate_table_format()

        if table_check:
            if self._schema['partition_column']:
                self._df.write.partitionBy(self._schema['partition_column']).mode(
                    'overwrite').format('delta').saveAsTable(self._table_name)
            else:
                self._df.write.mode(
                    'overwrite').format('delta').saveAsTable(self._table_name)
        else:
            raise ValueError("Table format validation failed.")

        if self._name != 'monitoring_values':
            self.check_signalling_issues()

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

    @property
    def data(self):
        """
        Returns the Spark DataFrame associated with the CustomDF instance.

        Returns:
            DataFrame: The Spark DataFrame associated with the CustomDF instance.
        """
        return self._df

    @data.setter
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

    @property
    def name(self):
        """
        Returns the name of the CustomDF instance.

        Returns:
            str: The name of the CustomDF instance.
        """
        return self._name
