from pyspark.sql import DataFrame, SparkSession
from functions.spark_session import read_table, create_spark_session
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from signalling_tables import table_name_list
from functions.signalling_rules import signalling_checks_dictionary

spark_session = create_spark_session()


def check_value_within_list(spark_session: SparkSession, dataframe: DataFrame, column_name: str, value_list: list) -> DataFrame:
    """
    Check if the values in a specified column of a PySpark DataFrame are within a list of valid values.
    
    Args:
    dataframe (DataFrame): The PySpark DataFrame to be processed.
    column_name (str): The name of the column to check for valid values.
    value_list (list): A list of valid values to check against.
    
    Returns:
         DataFrame: A DataFrame containing columns "total_count" and "valid_count."
            "total_count" - The total number of rows in the DataFrame.
            "valid_count" - The count of valid values within a list.
    """

    total_count = dataframe.count()
    valid_count = dataframe.filter(col(column_name).isin(value_list)).count()


    value_within_list_data = read_table(spark_session,'dummy_quality_check')
    value_within_list_data = value_within_list_data.withColumn('total_count', F.lit(total_count))
    value_within_list_data = value_within_list_data.withColumn('valid_count', F.lit(valid_count))

    return value_within_list_data


def calculate_filled_values(spark_session: SparkSession, dataframe: DataFrame, column_names: list) -> DataFrame:
    """
    Calculate filled values for each column in a PySpark DataFrame while excluding
    null and "NA" values.

    Args:
        dataframe (DataFrame): The PySpark DataFrame to be processed.
        column_names (list): List of column names to calculate valid counts.

    Returns:
        DataFrame: A DataFrame containing columns "total_count", "valid_count", "check_name" and "check_id"
            "total_count" - The total number of rows in the DataFrame.
            "valid_count" - The count of valid values (excluding null, "NA," and empty) in the column.
            "check_name" - The name of the quality check.
            "check_id" - The id of the quality check.
    """

    spark_session = create_spark_session()
    df = read_table(spark_session,'dummy_quality_check')
    # remove first row
    value_to_remove = "column_dummy"
    df = df.filter(df["column_name"] != value_to_remove)
    for column in column_names:
        total_count = dataframe.count()
        valid_count = dataframe.filter(
            (col(column).isNotNull()) |
            (col(column) != "NA")
        ).count()
        temp_df = read_table(spark_session,'dummy_quality_check')
        temp_df = temp_df.withColumn('column_name', F.lit(column))\
                                            .withColumn('total_count', F.lit(total_count))\
                                            .withColumn('valid_count', F.lit(valid_count))\
                                            .withColumn('check_name',F.lit('Check if values are filled'))\
                                            .withColumn('check_id',F.lit('tilt_1'))
        df = df.union(temp_df)
        

    return df


def check_values_in_range(spark_session: SparkSession, dataframe: DataFrame, column_name: str, range_start: int, range_end: int) -> DataFrame:
    """
    Check if values in a column are within a specified range.

    Args:
        dataframe (DataFrame): The PySpark DataFrame to be processed.
        column_name (str): The name of the column to check.
        range_start (int): The start of the range.
        range_end (int): The end of the range.

    Returns:
        DataFrame: A DataFrame containing columns "total_count" and "valid_count."
            "total_count" - The total number of rows in the DataFrame.
            "valid_count" - The count of valid values within a specified range.
    """
    
    total_count = dataframe.count()
    valid_count = dataframe.filter(col(column_name).between(range_start, range_end)).count()

    
    values_within_range_data = read_table(spark_session,'dummy_quality_check')
    values_within_range_data = values_within_range_data.withColumn('total_count', F.lit(total_count))
    values_within_range_data = values_within_range_data.withColumn('valid_count', F.lit(valid_count))
    return values_within_range_data



def check_signalling_issues(table_name):
    """
    Checks the signalling data quality of a DataFrame against the specified quality rule.

    Args:
        table_name (str): The name of the table for which the data quality is monitored.

    Returns:
        A dataframe, contraining check_id, table_name, column_name, check_name, total_count and valid_count for every signalling data quality check.

    """

    for table_name in table_name_list:

        dataframe = read_table(spark_session, table_name)
        dataframe_columns = dataframe.columns

        df = calculate_filled_values(spark_session, dataframe, dataframe_columns)
        df = df.withColumn('table_name',F.lit(table_name))

    for table_name in signalling_checks_dictionary:
            tables_list = signalling_checks_dictionary[table_name][0]
            check_types = tables_list.get('check')
            column_name = tables_list.get('columns')[0]

            dataframe_2 = read_table(spark_session, table_name)

            if check_types == 'values within list':
                value_list = tables_list.get('value_list')

                df_2 = check_value_within_list(spark_session, dataframe_2, column_name, value_list)

                df_2 = df_2.withColumn('table_name',F.lit(table_name))\
                        .withColumn('column_name',F.lit(column_name))\
                        .withColumn('check_name',F.lit(check_types))\
                        .withColumn('check_id',F.lit('tilt_2'))
                
            elif check_types == 'values in range':

                range_start = tables_list.get('range_start')
                range_end = tables_list.get('range_end')

                df_3 = check_values_in_range(spark_session, dataframe_2, column_name, range_start, range_end)

                df_3 = df_3.withColumn('table_name',F.lit(table_name))\
                        .withColumn('column_name',F.lit(column_name))\
                        .withColumn('check_name',F.lit(check_types))\
                        .withColumn('check_id',F.lit('tilt_3'))

    return None
