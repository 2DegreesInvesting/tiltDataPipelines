from pyspark.sql import DataFrame, SparkSession, Row
from functions.spark_session import read_table, create_spark_session
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
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
         DataFrame: A DataFrame containing columns "column_name," "total_count," and "valid_count."
            "column_name" - The name of the column.
            "total_count" - The total number of rows in the DataFrame.
            "valid_count" - The count of valid values within a list.
    """

    summary_data_list = []
    total_count = dataframe.count()
    valid_count = dataframe.filter(col(column_name).isin(value_list)).count()

    #summary_data_list.append((column_name, total_count, valid_count))
    #summary_data_list += [Row(column_name = column_name, total_count = total_count, valid_count = valid_count)]
   

    # FUNCTION WORKS, but creating a dataframe using spark session doesn't, NEED TO FIX
    value_within_list_data = read_table(spark_session,'dummy_quality_check')
    value_within_list_data = value_within_list_data.withColumn('total_count', F.lit(total_count))
    value_within_list_data = value_within_list_data.withColumn('valid_count', F.lit(valid_count))
    #result_data = result_data.union(summary_data_list)
    return value_within_list_data



def calculate_filled_values(spark_session: SparkSession, dataframe: DataFrame, column_names: list) -> DataFrame:
    """
    Calculate filled values for each column in a PySpark DataFrame while excluding
    null and "NA" values.

    Args:
        dataframe (DataFrame): The PySpark DataFrame to be processed.
        column_names (list): List of column names to calculate valid counts.

    Returns:
        DataFrame: A DataFrame containing columns "column_name," "total_count," and "valid_count."
            "column_name" - The name of the column.
            "total_count" - The total number of rows in the DataFrame.
            "valid_count" - The count of valid values (excluding null, "NA," and empty) in the column.
    """
    summary_data_list = []
    for column in column_names:
        total_count = dataframe.count()
        valid_count = dataframe.filter(
            (col(column).isNotNull()) |
            (col(column) != "NA")
        ).count()
        #summary_data_list.append((column, total_count, valid_count))

    # schema = StructType([
    #     StructField("column_name", StringType(), False),
    #     StructField("total_count", IntegerType(), False),
    #     StructField("valid_count", IntegerType(), False)
    # ])
    # FUNCTION WORKS, but creating a dataframe using spark session doesn't, NEED TO FIX
    quality_checks_data = read_table(spark_session,'dummy_quality_check')
    quality_checks_data = quality_checks_data.withColumn('column_name', F.lit(column))
    quality_checks_data = quality_checks_data.withColumn('total_count', F.lit(total_count))
    quality_checks_data = quality_checks_data.withColumn('valid_count', F.lit(valid_count))
    return quality_checks_data


def check_values_in_range(spark_session: SparkSession, dataframe: DataFrame, column_name: str, range_start: int, range_end: int) -> DataFrame:
    """
    Check if values in a column are within a specified range.

    Args:
        dataframe (DataFrame): The PySpark DataFrame to be processed.
        column_name (str): The name of the column to check.
        range_start (int): The start of the range.
        range_end (int): The end of the range.

    Returns:
        DataFrame: A DataFrame containing columns "column_name," "total_count," and "valid_count."
            "column_name" - The name of the column.
            "total_count" - The total number of rows in the DataFrame.
            "valid_count" - The count of valid values within a specified range.
    """
    #condition = (df[column_name] >= range_start) & (df[column_name] <= range_end)
    
    total_count = dataframe.count()
    valid_count = dataframe.filter(col(column_name).between(range_start, range_end)).count()

    
    values_within_range_data = read_table(spark_session,'dummy_quality_check')
    values_within_range_data = values_within_range_data.withColumn('total_count', F.lit(total_count))
    values_within_range_data = values_within_range_data.withColumn('valid_count', F.lit(valid_count))
    return values_within_range_data




