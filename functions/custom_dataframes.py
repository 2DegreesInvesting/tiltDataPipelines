from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from functions.dataframe_helpers import create_map_column, create_sha_values
from functions.signalling_functions import calculate_signalling_issues
from functions.signalling_rules import signalling_checks_dictionary
from functions.spark_session import build_table_path
from functions.tables import get_table_definition


class CustomDF:
    def __init__(self, table_name: str, spark_session: SparkSession, initial_df: DataFrame = None,  partition_name: str = '', history: str = 'recent'):
        self._name = table_name
        self._spark_session = spark_session
        self._schema = get_table_definition(self._name)
        self._partition_name = partition_name
        self._history = history
        self._env = 'develop'
        self._path = self.table_path()
        if initial_df:
            self._df = initial_df
        else:
            self._df = self.read_table()

    def table_path(self):
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
        if self._partition_name:
            # Return the table path with the specified partition
            return f"abfss://{self._schema['container']}@storagetilt{self._env}.dfs.core.windows.net/{self._schema['location']}/{self._schema['partition_column']}={self._partition_name}"
        else:
            # Return the table path without a partition
            return f"abfss://{self._schema['container']}@storagetilt{self._env}.dfs.core.windows.net/{self._schema['location']}"

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
            'csv': lambda: self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '"').option("multiline", 'True').load(self._path),
            'ecoInvent': lambda: self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '~').option('delimiter', ';').option("multiline", 'True').load(self._path),
            'tiltData': lambda: self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '~').option('delimiter', ';').option("multiline", 'True').load(self._path),
            'default': lambda: self._spark_session.read.format(self._schema['type']).schema(self._schema['columns']).option('header', True).load(self._path)
        }

        try:
            df = read_functions.get(
                self._schema['type'], read_functions['default'])()
            df.head()  # Force to load first record of the data to check if it throws an error
        except Exception as e:
            if "Path does not exist:" in str(e):
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

        return df

    def compare_tables(self):
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
        value_columns = [F.col(col_name) for col_name in self._df.columns if col_name not in [
            'tiltRecordID', 'from_date', 'to_date']]

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
        print(self._name)
        if self._name == 'monitoring_values':
            print(self._df.orderBy(F.col('signalling_id')).show(
                n=20, vertical=True))

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

        # Drop the table definition in the unity catalog
        delete_string = f"DROP TABLE IF EXISTS `{self._schema['container']}`.`default`.`{self._schema['location'].replace('.','')}`"

        # Build a SQL string to recreate the table in the most up to date format
        create_string = f"CREATE EXTERNAL TABLE IF NOT EXISTS `{self._schema['container']}`.`default`.`{self._schema['location'].replace('.','')}` ("

        for i in self._schema['columns']:
            col_info = i.jsonValue()
            col_string = f"`{col_info['name']}` {col_info['type']} {'NOT NULL' if not col_info['nullable'] else ''},"
            create_string += col_string

        table_path = build_table_path(
            self._schema['container'], self._schema['location'], None)
        create_string = create_string[:-1] + ")"
        create_string += f" USING {self._schema['type']} LOCATION '{table_path}'"
        if self._schema['partition_column']:
            create_string += f" PARTITIONED BY (`{self._schema['partition_column']}` STRING)"
        set_owner_string = f"ALTER TABLE `{self._schema['container']}`.`default`.`{self._schema['location'].replace('.','')}` SET OWNER TO tiltDevelopers"

        # Try and delete the already existing definition of the table
        try:
            self._spark_session.sql(delete_string)
        # Try to catch the specific exception where the users is not the owner of the table and can thus not delete the table
        except Exception as e:
            # If the user is not the owner, set the user to be the owner and then delete the table
            if "User is not an owner of Table" in str(e):
                self._spark_session.sql(set_owner_string)
                self._spark_session.sql(delete_string)
            # If we encounter any other error, raise as the error
            else:
                raise (e)
        self._spark_session.sql(create_string)
        self._spark_session.sql(set_owner_string)

    def check_signalling_issues(self):
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

        dummy_signalling_df = CustomDF(
            'dummy_quality_check', self._spark_session)
        signalling_checks = {}
        # Check if there are additional data quality monitoring checks to be executed
        if self._name in signalling_checks_dictionary.keys():
            signalling_checks = signalling_checks_dictionary[self._name]

        # Generate the monitoring values table to be written
        monitoring_values_df = calculate_signalling_issues(
            self._spark_session, self._df, signalling_checks, dummy_signalling_df.data)
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
        # Compare the newly created records with the existing tables

        self._df = self.compare_tables()

        # Add the SHA value to create a unique ID within tilt
        self._df = self.add_record_id()

        table_check = self.validate_table_format()

        if table_check:
            if self._schema['partition_column']:
                if self._schema['type'] == 'csv':
                    self._df.write.partitionBy(self._schema['partition_column']).mode(
                        'overwrite').csv(self._path)
                else:
                    self._df.write.partitionBy(self._schema['partition_column']).mode(
                        'overwrite').parquet(self._path)

            else:
                if self._schema['type'] == 'csv':
                    self._df.coalesce(1).write.mode(
                        'overwrite').csv(self._path)
                else:
                    self._df.coalesce(1).write.mode(
                        'overwrite').parquet(self._path)
        else:
            raise ValueError("Table format validation failed.")

        self.create_catalog_table()

        if self._name != 'monitoring_values':
            self.check_signalling_issues()

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
        self._df = new_df

    @property
    def name(self):
        """
        Returns the name of the CustomDF instance.

        Returns:
            str: The name of the CustomDF instance.
        """
        return self._name
