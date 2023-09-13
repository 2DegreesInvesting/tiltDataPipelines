from pyspark.sql import DataFrame, SparkSession
from functions.spark_session import read_table, create_spark_session
from pyspark.sql.functions import col
import pyspark.sql.functions as F


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




