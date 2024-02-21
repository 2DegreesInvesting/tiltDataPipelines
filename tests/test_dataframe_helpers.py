import pytest
from functions.dataframe_helpers import create_catalog_table, create_catalog_schema, create_catalog_table_owner, clean_column_names, create_sha_values, create_map_column, create_table_path, apply_scd_type_2, assign_signalling_id
import pyspark.sql.types as T
import pyspark.sql.functions as F

from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
from functions.spark_session import create_spark_session
from datetime import date


@pytest.fixture(scope='session')
def spark_session_fixture():
    sparkie = create_spark_session()
    yield sparkie


@pytest.fixture(scope='session')
def spark_schema_fixture():
    schema = T.StructType([
        T.StructField('test_string_column', T.StringType(), False),
        T.StructField('test_integer_column', T.IntegerType(), False),
        T.StructField('test_decimal_column', T.DoubleType(), True),
        T.StructField('test_date_column', T.DateType(), True),
    ]
    )
    return schema


@pytest.fixture(scope='session')
def spark_df_fixture(spark_session_fixture, spark_schema_fixture):
    data = [('test', 3, 4.25, date(2024, 1, 1),), ('test', 4, 4.25,
                                                   date(2024, 1, 1),), ('test', 8, -12.25, date(2025, 1, 1),),]
    df = spark_session_fixture.createDataFrame(
        data, spark_schema_fixture)
    return df


@pytest.fixture(scope='session')
def spark_shaValue_schema_fixture():
    schema = T.StructType([
        T.StructField('test_string_column', T.StringType(), False),
        T.StructField('test_integer_column', T.IntegerType(), False),
        T.StructField('test_decimal_column', T.DoubleType(), True),
        T.StructField('test_date_column', T.DateType(), True),
        T.StructField('shaValue', T.StringType(), True),
    ]
    )
    return schema


@pytest.fixture(scope='session')
def spark_df_shaValue_fixture(spark_session_fixture, spark_shaValue_schema_fixture):
    data = [('test', 3, 4.25, date(2024, 1, 1), 'c08b3845dc7c50a6adcffe4eaac17e48254452b2a02a2d8e5b736388e1a922e2'), ('test', 4, 4.25,
                                                                                                                      date(2024, 1, 1), '493ea4dfd66efdecd87aaf61e8b871cb3799778bbcea79b9c8dbb2845fd57536'), ('test', 8, -12.25, date(2025, 1, 1), '21c45afcd25c751840e738ea0cd09c3627b934130b620e88469798334364c4d9'),]
    df = spark_session_fixture.createDataFrame(
        data, spark_shaValue_schema_fixture)
    return df


@pytest.fixture(scope='session')
def spark_map_schema_fixture():
    schema = T.StructType([
        T.StructField('test_string_column', T.StringType(), False),
        T.StructField('test_integer_column', T.IntegerType(), False),
        T.StructField('test_decimal_column', T.DoubleType(), True),
        T.StructField('test_date_column', T.DateType(), True),
        T.StructField('tiltRecordID', T.StringType(), True),
        T.StructField('map_test_table_raw', T.MapType(
            T.StringType(), T.StringType()), True),
    ]
    )
    return schema


@pytest.fixture(scope='session')
def spark_df_map_fixture(spark_session_fixture, spark_map_schema_fixture):
    data = [('test', 3, 4.25, date(2024, 1, 1), 'c08b3845dc7c50a6adcffe4eaac17e48254452b2a02a2d8e5b736388e1a922e2', {'test_table_raw': 'c08b3845dc7c50a6adcffe4eaac17e48254452b2a02a2d8e5b736388e1a922e2'},), ('test', 4, 4.25, date(2024, 1, 1), '493ea4dfd66efdecd87aaf61e8b871cb3799778bbcea79b9c8dbb2845fd57536', {
        'test_table_raw': '493ea4dfd66efdecd87aaf61e8b871cb3799778bbcea79b9c8dbb2845fd57536'},), ('test', 8, -12.25, date(2025, 1, 1), '21c45afcd25c751840e738ea0cd09c3627b934130b620e88469798334364c4d9', {'test_table_raw': '21c45afcd25c751840e738ea0cd09c3627b934130b620e88469798334364c4d9'},),]
    df = spark_session_fixture.createDataFrame(
        data, spark_map_schema_fixture)
    return df


@pytest.fixture(scope='session')
def spark_unclean_schema_fixture():
    schema = T.StructType([
        T.StructField(r'test string_column', T.StringType(), False),
        T.StructField(r'test\integer (column)', T.IntegerType(), False),
        T.StructField(r'test_decimal-column', T.DoubleType(), True),
        T.StructField(r'test_date/column', T.DateType(), True),
    ]
    )
    return schema


@pytest.fixture(scope='session')
def spark_unclean_df_fixture(spark_session_fixture, spark_schema_fixture):
    data = [('test', 3, 4.25, date(2024, 1, 1),), ('test', 4, 4.25,
                                                   date(2024, 1, 1),), ('test', 8, -12.25, date(2025, 1, 1),),]
    df = spark_session_fixture.createDataFrame(
        data, spark_schema_fixture)
    return df


class Test_valid_setup:
    """Validates a working setup"""

    @staticmethod
    def test_always_true():
        assert True


class Test_create_map_column:
    """
    This class contains unit tests for the create_map_column function.
    """

    @staticmethod
    def test_create_map_column(spark_df_shaValue_fixture, spark_df_map_fixture):
        """
        Test case for the create_map_column function. This function should return a DataFrame with a map column, that contains the name of the table, in this case 'test_table_raw', and the vlaue for the tiltRecordID.

        Args:
            spark_df_shaValue_fixture: Spark DataFrame with 'shaValue' column.
            spark_df_map_fixture: Expected Spark DataFrame with 'map' column.

        Returns:
            None
        """
        df = spark_df_shaValue_fixture.withColumnRenamed(
            'shaValue', 'tiltRecordID')
        map_df = create_map_column(df, 'test_table_raw')

        assertDataFrameEqual(map_df, spark_df_map_fixture)


class Test_create_sha_values:

    @staticmethod
    def test_create_sha_values(spark_df_fixture, spark_df_shaValue_fixture):
        """
        Test case for the create_sha_values function. This function should return a DataFrame with a column that creates a SHA value for the specified columns.

        Args:
            spark_df_fixture (pyspark.sql.DataFrame): The input Spark DataFrame.
            spark_df_shaValue_fixture (pyspark.sql.DataFrame): The expected output Spark DataFrame.

        Returns:
            None
        """
        sha_df = create_sha_values(
            spark_df_fixture, ['test_string_column', 'test_integer_column', 'test_decimal_column', 'test_date_column'])

        assertDataFrameEqual(sha_df, spark_df_shaValue_fixture)


class Test_create_table_path:

    @staticmethod
    def test_create_table_path_landingzone():

        environment = 'develop'
        schema = {
            "columns": T.StructType([
                T.StructField('id', T.IntegerType(), False),
                T.StructField('name', T.StringType(), False),
                T.StructField('age', T.IntegerType(), True),
            ]),
            "partition_column": "",
            "container": 'landingzone',
            "location": 'test_table_landingzone'
            ""
        }
        part_name = ''

        test_string = create_table_path(environment, schema, part_name)
        compare_string = 'abfss://landingzone@storagetiltdevelop.dfs.core.windows.net/test_table_landingzone/'

        assert test_string == compare_string

    @staticmethod
    def test_create_table_path_landingzone_partitioned():

        environment = 'develop'
        schema = {
            "columns": T.StructType([
                T.StructField('id', T.IntegerType(), False),
                T.StructField('name', T.StringType(), False),
                T.StructField('age', T.IntegerType(), True),
            ]),
            "partition_column": "date",
            "container": 'landingzone',
            "location": 'test_table_landingzone'
            ""
        }
        part_name = '2020-01-01'

        test_string = create_table_path(environment, schema, part_name)
        compare_string = 'abfss://landingzone@storagetiltdevelop.dfs.core.windows.net/test_table_landingzone/date=2020-01-01'

        assert test_string == compare_string


class Test_create_catalog_table:
    """Tests for the create_catalog_table function"""

    class TestDataFrameHelpers:
        @staticmethod
        def test_create_catalog_table():
            """
            Test case for the create_catalog_table function. This function should return a string that creates a table in the Delta Lake format, with the id, name and age column. Additionally it is partitioned by the date column.

            Returns:
                None
            """
            # Define the input parameters
            table_name = "my_table"
            schema = {
                "columns": T.StructType([
                    T.StructField('id', T.IntegerType(), False),
                    T.StructField('name', T.StringType(), False),
                    T.StructField('age', T.IntegerType(), True),
                ]),
                "partition_column": "date"
            }

            # Call the function to create the SQL string
            sql_string = create_catalog_table(table_name, schema)

            # Define the expected SQL string
            expected_sql_string = "CREATE TABLE IF NOT EXISTS my_table (`id` integer NOT NULL,`name` string NOT NULL,`age` integer ) USING DELTA PARTITIONED BY (`date` STRING)"

            # Assert that the generated SQL string matches the expected SQL string
            assert sql_string == expected_sql_string

    @staticmethod
    def test_create_catalog_table_no_partition():
        """
        Test case for the create_catalog_table function. This function should return a string that creates a table in the Delta Lake format, with the id, name and age column

        Returns:
            None
        """
        # Define the input parameters
        table_name = "my_table"
        schema = {
            "columns": T.StructType([
                T.StructField('id', T.IntegerType(), False),
                T.StructField('name', T.StringType(), False),
                T.StructField('age', T.IntegerType(), True),
            ]
            ),
            "partition_column": ''
        }

        # Call the function to create the SQL string
        sql_string = create_catalog_table(table_name, schema)

        # Define the expected SQL string
        expected_sql_string = "CREATE TABLE IF NOT EXISTS my_table (`id` integer NOT NULL,`name` string NOT NULL,`age` integer ) USING DELTA "

        # Assert that the generated SQL string matches the expected SQL string
        assert sql_string == expected_sql_string

    @staticmethod
    def test_create_catalog_table_empty_schema():
        """
        Test case for the create_catalog_table function. This function should raise a ValueError when the schema is empty.

        Returns:
            None
        """
        # Define the input parameters
        table_name = "my_table"
        schema = {
            "columns": [],
            "partition_column": None
        }

        # Call the function to create the SQL string
        with pytest.raises(ValueError) as error_info:
            sql_string = create_catalog_table(table_name, schema)

        # Assert that the generated SQL string matches the expected SQL string
        assert str(error_info.value) == "The provided schema can not be empty"


class Test_create_catalog_schema:
    """Tests for the create_catalog_schema function"""

    @staticmethod
    def test_create_catalog_schema():
        """
        Test case for the create_catalog_schema function. This function should return a concatenated string that creates a schema in Databricks.

        Returns:
            None
        """
        # Define the input parameters
        environment = "dev"
        schema = {
            "container": "my_schema"
        }

        # Call the function to create the SQL string
        sql_string = create_catalog_schema(environment, schema)

        # Define the expected SQL string
        expected_sql_string = "CREATE SCHEMA IF NOT EXISTS dev.my_schema; ALTER SCHEMA dev.my_schema SET OWNER TO tiltDevelopers;"

        # Assert that the generated SQL string matches the expected SQL string
        assert sql_string == expected_sql_string


class Test_create_catalog_table_owner:
    """Tests for the create_catalog_table_owner function"""

    @staticmethod
    def test_create_catalog_table_owner():
        """
        Test case for the create_catalog_table_owner function. This function should return a string that sets the owner of the table to the tiltDevelopers group.

        Returns:
            None
        """
        # Define the input parameter
        table_name = "my_table"

        # Call the function to create the SQL string
        sql_string = create_catalog_table_owner(table_name)

        # Define the expected SQL string
        expected_sql_string = "ALTER TABLE my_table SET OWNER TO tiltDevelopers"

        # Assert that the generated SQL string matches the expected SQL string
        assert sql_string == expected_sql_string


class Test_clean_column_names:

    @staticmethod
    def test_cleaning_column_names(spark_unclean_df_fixture, spark_df_fixture):
        """
        Test case for the clean_column_names function. This function should return a DataFrame with the column names cleaned. The column names should have all special characters removed from their name that are not allowed by Databricks.

        Returns:
            None
        """

        test_df = clean_column_names(spark_unclean_df_fixture)

        assertDataFrameEqual(test_df, spark_df_fixture)


class Test_apply_scd_type_2:
    """
    Unit tests for the apply_scd_type_2 function.
    """

    @staticmethod
    def test_table_no_updates(spark_session_fixture):
        """
        Test case for apply_scd_type_2 when there are no updates in the target table.

        Args:
            spark_session_fixture: PySpark fixture for the Spark session.

        Returns:
            None
        """

        initial_schema = T.StructType([
            T.StructField('id', T.StringType(), False),
            T.StructField('name', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False)
        ])
        target_schema = T.StructType([
            T.StructField('id', T.StringType(), False),
            T.StructField('name', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False),
            T.StructField('from_date', T.DateType(), False),
            T.StructField('to_date', T.DateType(), False),
        ])
        new_table = spark_session_fixture.createDataFrame(
            [(1, 'test', 1.0,)], initial_schema)

        existing_table = spark_session_fixture.createDataFrame(
            [(1, 'test', 1.0, date(2024, 1, 1),  date(2099, 12, 31),)], target_schema)

        resulting_table = spark_session_fixture.createDataFrame(
            [(1, 'test', 1.0, date(2024, 1, 1),  date(2099, 12, 31),)], target_schema)

        test_table = apply_scd_type_2(new_table, existing_table)

        assertDataFrameEqual(test_table, resulting_table)

    @staticmethod
    def test_table_single_update(spark_session_fixture):
        """
        Test case for apply_scd_type_2 when there is a single update in the target table.

        Args:
            spark_session_fixture: PySpark fixture for the Spark session.

        Returns:
            None
        """

        processing_date = date.today()
        initial_schema = T.StructType([
            T.StructField('id', T.StringType(), False),
            T.StructField('name', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False)
        ])
        target_schema = T.StructType([
            T.StructField('id', T.StringType(), False),
            T.StructField('name', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False),
            T.StructField('from_date', T.DateType(), False),
            T.StructField('to_date', T.DateType(), False),
        ])

        new_table = spark_session_fixture.createDataFrame(
            [(1, 'test', 2.0,)], initial_schema)

        existing_table = spark_session_fixture.createDataFrame(
            [(1, 'test', 1.0, date(2024, 1, 1),  date(2099, 12, 31),)], target_schema)

        resulting_table = spark_session_fixture.createDataFrame(
            [(1, 'test', 1.0, date(2024, 1, 1),  processing_date,), ('1', 'test', 2.0, processing_date, date(2099, 12, 31),)], target_schema)

        test_table = apply_scd_type_2(new_table, existing_table)

        assertDataFrameEqual(test_table, resulting_table)


class Test_assign_signalling_id:

    @staticmethod
    def test_assgn_single_signalling_id(spark_session_fixture):
        """
        This test case focuses on the case that the compare table has a signalling id specified more than once.
        Before fixing this error, it would duplicate the record in the source table.

        Args:
            spark_session_fixture: Spark session fixture for testing.

        Returns:
            None
        """

        processing_date = date.today()

        monitoring_source_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True),
            T.StructField('table_name', T.StringType(), False),
        ])
        monitoring_compare_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True),
            T.StructField('from_date', T.DateType(), False),
            T.StructField('to_date', T.DateType(), False),
            T.StructField('tiltRecordID', T.StringType(), False),
            T.StructField('table_name', T.StringType(), False),
        ])
        monitoring_target_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True),
            T.StructField('table_name', T.StringType(), False),
        ])

        monitoring_source_table = spark_session_fixture.createDataFrame(
            [(1, 'tilt_1', 'test_column', 'check if values are filled', 60, 60, 'test_table',)], monitoring_source_schema)
        monitoring_source_table = monitoring_source_table.withColumn(
            'signalling_id', F.lit(None))

        monitoring_compare_table = spark_session_fixture.createDataFrame(
            [(1, 'tilt_1', 'test_column', 'check if values are filled', 25, 25, date(2024, 1, 1),  date(2024, 2, 1), 'recordID', 'test_table',),
             (1, 'tilt_1', 'test_column', 'check if values are filled', 50, 50, date(2024, 2, 1),  date(2099, 12, 31), 'recordID', 'test_table',)], monitoring_compare_schema)

        monitoring_result_df = spark_session_fixture.createDataFrame(
            [(1, 'tilt_1', 'test_column', 'check if values are filled', 60, 60, 'test_table',)], monitoring_target_schema)

        test_table = assign_signalling_id(
            monitoring_source_table, monitoring_compare_table)

        assertDataFrameEqual(test_table, monitoring_result_df)


if __name__ == "__main__":
    pytest.main()
