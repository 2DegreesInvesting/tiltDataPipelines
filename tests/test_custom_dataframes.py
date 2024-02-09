import pytest

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql import DataFrame

from datetime import date

from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session


@pytest.fixture(scope='session')
def spark_session_fixture():
    sparkie = create_spark_session()
    yield sparkie


@pytest.fixture(scope='session')
def spark_schema_fixture():
    schema = StructType([
        StructField('test_string1_column', StringType(), False),
        StructField('test_integer_column', IntegerType(), False),
        StructField('test_decimal_column', DoubleType(), True),
        StructField('test_date_column', DateType(), True),
    ]
    )
    return schema


@pytest.fixture(scope='session')
def spark_df_fixture(spark_session_fixture, spark_schema_fixture):
    data = [('test', 3, 4.25, date(2024, 1, 1),), ('test', 4, 4.25,
                                                   date(2024, 1, 1),), ('test_string', 8, -12.25, date(2025, 1, 1),),]
    df = spark_session_fixture.createDataFrame(
        data, spark_schema_fixture)
    return df


@pytest.fixture(scope='session')
def custom_filled_df_fixture(spark_session_fixture, spark_df_fixture):
    df = CustomDF('test_table_raw', spark_session_fixture,
                  initial_df=spark_df_fixture)
    return df


@pytest.fixture(scope='session')
def custom_delta_df_fixture(spark_session_fixture, spark_df_fixture):
    df = CustomDF('test_table_raw', spark_session_fixture)
    return df


@pytest.fixture(scope='session')
def custom_csv_df_fixture(spark_session_fixture, spark_df_fixture):
    df = CustomDF('test_table_landingzone', spark_session_fixture)
    return df


class Test_valid_setup:
    """Validates a working setup"""

    @staticmethod
    def test_always_true():
        assert True


class Test_custom_df:
    """Tests the CustomDF class"""

    @staticmethod
    def test_custom_df_init(custom_filled_df_fixture, spark_df_fixture):
        assert custom_filled_df_fixture._name == 'test_table_raw'
        assert custom_filled_df_fixture._spark_session is not None
        assert custom_filled_df_fixture._partition_name == ''
        assert custom_filled_df_fixture._history == 'recent'
        assert custom_filled_df_fixture._path == ''
        assert custom_filled_df_fixture._table_name == '`develop`.`test`.`test_table`'
        assert custom_filled_df_fixture.data.count() == 3
        assertDataFrameEqual(custom_filled_df_fixture.data, spark_df_fixture)
        assertSchemaEqual(custom_filled_df_fixture.data.schema,
                          spark_df_fixture.schema)

    @staticmethod
    def test_read_csv_table(custom_csv_df_fixture, spark_df_fixture):
        assert custom_csv_df_fixture.read_table().count() == 3
        assertDataFrameEqual(custom_csv_df_fixture.data, spark_df_fixture)

    @staticmethod
    def test_read_delta_table(custom_delta_df_fixture):
        assert custom_delta_df_fixture.read_table().count() == 3
