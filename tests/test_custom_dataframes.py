import pytest

from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session


@pytest.fixture(scope='session')
def spark_session_fixture():
    sparkie = create_spark_session()
    yield sparkie


@pytest.fixture(scope='session')
def custom_df_fixture(spark_session_fixture):
    df = CustomDF('elementary_exchanges_raw', spark_session_fixture)
    return df


class Test_valid_setup:
    """Validates a working setup"""

    @staticmethod
    def test_always_true():
        assert True


class Test_custom_df:
    """Tests the CustomDF class"""

    @staticmethod
    def test_custom_df_init(custom_df_fixture):
        assert custom_df_fixture._name == 'elementary_exchanges_raw'
        # assert custom_df_fixture._table_name == '`develop`.`raw`.`elementary_exchanges_raw`'
        # assert custom_df_fixture._spark_session is not None
        # assert custom_df_fixture.data is not None
        # assert custom_df_fixture.data.count() > 0
