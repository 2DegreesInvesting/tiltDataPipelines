import pytest

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.testing import assertDataFrameEqual

from datetime import date

from functions.spark_session import create_spark_session
from functions.signalling_functions import TransposeDF, check_value_within_list, calculate_filled_values, check_values_in_range, check_values_unique, check_values_format, check_expected_distinct_value_count, column_sums_to_1, calculate_signalling_issues, check_expected_value_count


@pytest.fixture(scope='session')
def spark_session_fixture():
    sparkie = create_spark_session()
    yield sparkie


@pytest.fixture(scope='session')
def spark_general_schema():

    schema = T.StructType([
        T.StructField('group_column', T.StringType(), False),
        T.StructField('description_column', T.StringType(), False),
        T.StructField('sum_column', T.DoubleType(), True),
        T.StructField('integer_column', T.IntegerType(), True),
        T.StructField('from_date', T.DateType(), True),
        T.StructField('to_date', T.DateType(), True),
        T.StructField('tiltRecordID', T.StringType(), True),
    ])

    return schema


@pytest.fixture(scope='session')
def spark_general_df(spark_session_fixture, spark_general_schema):

    df = spark_session_fixture.createDataFrame(
        [('group_value_1', 'This is a valid description', 0.5, 6, date(2024, 1, 1), date(2099, 12, 31), 'example_RecordID_1'),
            ('group_value_1', 'This is a valid description', 0.5, 7, date(
                2024, 1, 1), date(2099, 12, 31), 'example_RecordID_2'),
            ('group_value_2', 'This description is not valid', 0.5, 8,
             date(2024, 1, 1), date(2099, 12, 31), 'example_RecordID_3'),
            ('group_value_2', 'This is a valid description', 0.1, 9, date(
                2024, 1, 1), date(2099, 12, 31), 'example_RecordID_4'),
            ('group_value_2', 'NA', 0.4, 10, date(2024, 1, 1),
             date(2099, 12, 31), 'example_RecordID_5'),
            ('group_value_3', 'What is this desctiption', 0.5, 11, date(
                2024, 1, 1), date(2099, 12, 31), 'example_RecordID_6'),
            ('group_value_3', 'This is a valid description', 0.7, 12,
             date(2024, 1, 1), date(2099, 12, 31), 'example_RecordID_7'),
            ('group_value_3', 'This description is empty', 0, 13, date(
                2024, 1, 1), date(2099, 12, 31), 'example_RecordID_8'),
            ('group_value_4', 'This is a valid description', 1, 14, date(2024, 1, 1), date(2099, 12, 31), 'example_RecordID_9'),], spark_general_schema)

    return df


class Test_valid_setup:
    """Validates a working setup"""

    @staticmethod
    def test_always_true():
        assert True


class Test_Transpose_df:

    @staticmethod
    def test_transpose_dataframe(spark_session_fixture):

        to_be_transposed_schema = T.StructType([
            T.StructField('column_1', T.IntegerType(), False),
            T.StructField('column_2', T.IntegerType(), False),
            T.StructField('column_3', T.IntegerType(), True),
            T.StructField('invalid_count_column', T.StringType(), True),
        ])

        to_be_transposed_df = spark_session_fixture.createDataFrame(
            [(5, 6, 7, 'invalid_count')], to_be_transposed_schema)

        col_list = ['column_1', 'column_2', 'column_3']

        df = TransposeDF(to_be_transposed_df,
                         col_list, 'invalid_count_column')

        transposed_schema = T.StructType([
            T.StructField('invalid_count_column', T.StringType(), True),
            T.StructField('invalid_count', T.StringType(), False),
        ])

        transposed_df = spark_session_fixture.createDataFrame(
            [('column_1', 5), ('column_2', 6), ('column_3', 7)], transposed_schema)

        assertDataFrameEqual(transposed_df, df)


class Test_value_within_list:

    @staticmethod
    def test_value_within_list(spark_general_df):

        value_list = ['group_value_1', 'group_value_2']

        check_dict = {
            'columns': ['group_column'],
            'value_list': value_list
        }

        result_count = check_value_within_list(
            spark_general_df, **check_dict)

        assert result_count == 5


class Test_calculate_filled_values:

    @staticmethod
    def test_calculate_filled_values(spark_session_fixture, spark_general_df):

        df = calculate_filled_values(spark_general_df)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame(
            [(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
             (1, 'tilt_1', 'description_column',
              'Check if values are filled', 9, 8, ),
             (1, 'tilt_1', 'sum_column', 'Check if values are filled', 9, 9, ),
             (1, 'tilt_1', 'integer_column', 'Check if values are filled', 9, 9, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)


class Test_check_values_in_range:

    @staticmethod
    def test_check_values_in_range(spark_general_df):

        check_dict = {
            'columns': ['integer_column'],
            'range_start': 6,
            'range_end': 10
        }

        result_count = check_values_in_range(
            spark_general_df, **check_dict)

        assert result_count == 5


class Test_check_values_unique:

    @staticmethod
    def test_check_values_unique(spark_general_df):

        check_dict = {
            'columns': ['group_column']
        }

        result_count = check_values_unique(
            spark_general_df, **check_dict)

        assert result_count == 4


class Test_check_values_format:

    @staticmethod
    def test_check_values_format(spark_general_df):

        check_dict = {
            'columns': ['description_column'],
            'format': 'valid description'
        }
        result_count = check_values_format(
            spark_general_df, **check_dict)

        assert result_count == 5


class Test_check_expected_distinct_value_count:

    @staticmethod
    def test_check_expected_distinct_value_count(spark_general_df):

        check_dict = {
            'columns': ['group_column'],
            'expected_count': 1,
            'distinct_columns': ['description_column']
        }

        result_count = check_expected_distinct_value_count(
            spark_general_df, **check_dict)

        assert result_count == 3


class Test_columns_sum_to_1:

    @staticmethod
    def test_columns_sum_to_1(spark_general_df):

        check_dict = {
            'columns': ['group_column'],
            'sum_column': 'sum_column'
        }

        valid_count = column_sums_to_1(
            spark_general_df, **check_dict)

        assert valid_count == 6


class Test_expected_value_count:

    @staticmethod
    def test_expected_value_count(spark_general_df):

        check_dict = {
            'columns': ['group_column'],
            'expected_count': 3
        }

        valid_count = check_expected_value_count(
            spark_general_df, **check_dict)

        assert valid_count == 6


class Test_calculate_signalling_issues:

    @staticmethod
    def test_signalling_issues_without_custom_checks(spark_session_fixture, spark_general_df):

        df = calculate_signalling_issues(
            spark_general_df, {}, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame(
            [(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
             (1, 'tilt_1', 'description_column',
              'Check if values are filled', 9, 8, ),
             (1, 'tilt_1', 'sum_column', 'Check if values are filled', 9, 9, ),
             (1, 'tilt_1', 'integer_column', 'Check if values are filled', 9, 9, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_list_check(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'values within list',
            'columns': ['group_column'],
            'value_list': ['group_value_1', 'group_value_2']
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_2', 'group_column', 'values within list of: "group_value_1","group_value_2"', 9, 5, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_range_check(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'values in range',
            'columns': ['integer_column'],
            'range_start': 6,
            'range_end': 10
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_3', 'integer_column', 'values between 6 and 10', 9, 5, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_values_unique(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'values are unique',
            'columns': ['group_column']
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_4', 'group_column', "unique values in column `['group_column']`", 9, 4, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_values_format(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'values have format',
            'columns': ['description_column'],
            'format': 'valid description'
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_5', 'description_column', "values have format valid description", 9, 5, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_values_expected_occurence(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'values occur as expected',
            'columns': ['group_column'],
            'expected_count': 3
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_7', 'group_column', "values occur 3 times", 9, 6, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_values_sum_to_1(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'values sum to 1',
            'columns': ['group_column'],
            'sum_column': 'sum_column'
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_8', 'group_column', 'values in column "sum_column" sum to 1', 9, 6, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)

    @staticmethod
    def test_signalling_issues_with_distinct_values_as_expected(spark_session_fixture, spark_general_df):

        signalling_list = [{
            'check': 'distinct values occur as expected',
            'columns': ['group_column'],
            'distinct_columns': ['description_column'],
            'expected_count': 1
        },]

        df = calculate_signalling_issues(
            spark_general_df, signalling_list, spark_session_fixture)

        resulting_schema = T.StructType([
            T.StructField('signalling_id', T.IntegerType(), False),
            T.StructField('check_id', T.StringType(), False),
            T.StructField('column_name', T.StringType(), True),
            T.StructField('check_name', T.StringType(), True),
            T.StructField('total_count', T.IntegerType(), True),
            T.StructField('valid_count', T.IntegerType(), True)
        ]
        )

        resulting_df = spark_session_fixture.createDataFrame([(1, 'tilt_1', 'group_column', 'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'description_column',
                                                               'Check if values are filled', 9, 8, ),
                                                              (1, 'tilt_1', 'sum_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_1', 'integer_column',
                                                               'Check if values are filled', 9, 9, ),
                                                              (1, 'tilt_9', 'group_column', "1 distinct values occur in column description_column", 9, 3, ),], resulting_schema)
        resulting_df = resulting_df.withColumn('signalling_id', F.lit(None))

        assertDataFrameEqual(df, resulting_df)
