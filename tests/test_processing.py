import pytest

from functions.processing import generate_table


class Test_valid_setup:
    """Validates a working setup"""

    @staticmethod
    def test_always_true():
        assert True


class Test_processing:
    """Tests the processing module"""

    @staticmethod
    def test_generate_not_specified_table():
        table_name = 'non_existent_table'

        with pytest.raises(ValueError) as error_info:
            generate_table(table_name)

        assert str(
            error_info.value) == 'The table: non_existent_table is not specified in the processing functions'
