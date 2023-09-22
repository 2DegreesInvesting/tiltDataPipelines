from pyspark.sql import DataFrame, SparkSession
from functions.spark_session import read_table, create_spark_session, write_table
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from functions.signalling_rules import signalling_checks_dictionary
from pyspark.sql.types import IntegerType


def check_value_within_list(dataframe: DataFrame, column_name: str, value_list: list) -> DataFrame:
    """
    Filter a DataFrame based on a list of values within a specific column and return the count of valid rows.

    This function takes a DataFrame and filters it to include only rows where the values in the specified
    column match any of the values in the provided 'value_list'. It then returns the count of valid rows
    that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input DataFrame to filter.
    - column_name (str): The name of the column in the DataFrame to filter by.
    - value_list (list): A list of values to compare with the values in the specified column.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' match any
      of the values in 'value_list'.
    """
    valid_count = dataframe.filter(F.col(column_name).isin(value_list)).count()
    return valid_count


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
                                            .withColumn('total_count', F.lit(total_count).cast(IntegerType()))\
                                            .withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))\
                                            .withColumn('check_name',F.lit('Check if values are filled'))\
                                            .withColumn('check_id',F.lit('tilt_1'))
        df = df.union(temp_df)
        

    return df


def check_values_in_range(dataframe: DataFrame, column_name: str, range_start: int, range_end: int) -> DataFrame:
    """
    Filter a Spark DataFrame to include rows where values in a specified column are within a given range,
    and return the count of valid rows.

    This function takes a Spark DataFrame and filters it to include only rows where the values in the specified
    'column_name' fall within the inclusive range specified by 'range_start' and 'range_end'. It then returns
    the count of valid rows that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to filter.
    - column_name (str): The name of the column in the DataFrame to filter by.
    - range_start (int): The inclusive lower bound of the range for filtering.
    - range_end (int): The inclusive upper bound of the range for filtering.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' fall within
      the specified range.   
    """

    valid_count = dataframe.filter(col(column_name).between(range_start, range_end)).count()

    return valid_count

def check_values_unique(dataframe: DataFrame, column_name: str) -> int:
    """
    Check the uniqueness of values in a specified column of a Spark DataFrame and return the count of unique values.

    This function takes a Spark DataFrame and evaluates the uniqueness of values in the specified 'column_name'.
    It returns the count of unique values in that column.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to analyze.
    - column_name (str): The name of the column in the DataFrame to check for uniqueness.

    Returns:
    - valid_count (int): The count of unique values in the specified column.
    """
    valid_count = dataframe.select(F.col(column_name)).distinct().count()
    return valid_count


def check_values_format(dataframe: DataFrame, column_name: str, format: str) -> int:
    """
    Check if values in a specified column of a Spark DataFrame match a given regular expression pattern,
    and return the count of matching values.

    This function takes a Spark DataFrame and filters it to include only rows where the values in the specified
    'column_name' match the regular expression pattern provided in 'format'. It then returns the count of rows
    that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to filter.
    - column_name (str): The name of the column in the DataFrame to check for format compliance.
    - format (str): The regular expression pattern to match against the values in the specified column.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' match the
      specified regular expression pattern.
    """
    valid_count = dataframe.filter(F.col(column_name).rlike(format)).count()
    return valid_count


def check_signalling_issues(spark_session: SparkSession, table_name: str):
    """
    Checks the signalling data quality of a DataFrame against the specified quality rule.

    Args:
        table_name (str): The name of the table for which the data quality is monitored.

    Returns:
        A dataframe, contraining check_id, table_name, column_name, check_name, total_count and valid_count for every signalling data quality check.

    """

    dataframe = read_table(spark_session, table_name)
    dataframe_columns = dataframe.columns

    # df = calculate_filled_values(spark_session, dataframe, dataframe_columns)
    df = read_table(spark_session,'dummy_quality_check')
    df = df.withColumn('table_name',F.lit(table_name))

    if table_name in signalling_checks_dictionary.keys():
            for signalling_check in signalling_checks_dictionary[table_name]:
                check_types = signalling_check.get('check')
                column_name = signalling_check.get('columns')[0]

                total_count = dataframe.count()

                signalling_check_df = read_table(spark_session,'dummy_quality_check')
                signalling_check_df = signalling_check_df.withColumn('table_name',F.lit(table_name))\
                            .withColumn('column_name',F.lit(column_name))\
                            .withColumn('check_name',F.lit(check_types))\
                            .withColumn('total_count',F.lit(total_count).cast(IntegerType()))

                if check_types == 'values within list':

                    value_list = signalling_check.get('value_list')
                    valid_count = check_value_within_list(dataframe, column_name, value_list)
                    check_id = 'tilt_2'
                    
                elif check_types == 'values in range':

                    range_start = signalling_check.get('range_start')
                    range_end = signalling_check.get('range_end')
                    valid_count = check_values_in_range(dataframe, column_name, range_start, range_end)
                    check_id = 'tilt_3'

                elif check_types == 'values are unique':

                    valid_count = check_values_unique(dataframe, column_name)
                    check_id = 'tilt_4'

                elif check_types == 'values have format':
                    
                    check_format = signalling_check.get('format')
                    valid_count = check_values_format(dataframe, column_name, check_format)
                    check_id = 'tilt_5'
    
                signalling_check_df = signalling_check_df.withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))
                signalling_check_df = signalling_check_df.withColumn('check_id',F.lit(check_id))

                df = df.union(signalling_check_df)

    monitoring_values_df = read_table(spark_session,'monitoring_values')   
    monitoring_values_df = monitoring_values_df.filter(F.col('to_date')=='2099-12-31').select([F.col(column) for column in df.columns if column not in ['from_date','to_date']])
  # filter the monitoring values table to exclude all records that already exists for that table
    monitoring_values_df_filtered = monitoring_values_df.filter(F.col('table_name')!= table_name)
    monitoring_values_df = monitoring_values_df_filtered.union(df)
    print(monitoring_values_df.show(vertical=True))
    # write_table(spark_session, monitoring_values_df, 'monitoring_values')
    return monitoring_values_df
