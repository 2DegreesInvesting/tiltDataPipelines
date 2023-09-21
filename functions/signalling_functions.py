from pyspark.sql import DataFrame, SparkSession
from functions.spark_session import read_table, create_spark_session, write_table
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from functions.signalling_rules import signalling_checks_dictionary
from pyspark.sql.types import IntegerType


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
    value_within_list_data = value_within_list_data.withColumn('total_count', F.lit(total_count).cast(IntegerType()))
    value_within_list_data = value_within_list_data.withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))

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
                                            .withColumn('total_count', F.lit(total_count).cast(IntegerType()))\
                                            .withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))\
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
    values_within_range_data = values_within_range_data.withColumn('total_count', F.lit(total_count).cast(IntegerType()))
    values_within_range_data = values_within_range_data.withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))
    return values_within_range_data



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

    df = calculate_filled_values(spark_session, dataframe, dataframe_columns)
    df = df.withColumn('table_name',F.lit(table_name))

    if table_name in signalling_checks_dictionary.keys():
            for signalling_check in signalling_checks_dictionary[table_name]:
                check_types = signalling_check.get('check')
                column_name = signalling_check.get('columns')[0]

                if check_types == 'values within list':
                    value_list = signalling_check.get('value_list')

                    df_2 = check_value_within_list(spark_session, dataframe, column_name, value_list)

                    df_2 = df_2.withColumn('table_name',F.lit(table_name))\
                            .withColumn('column_name',F.lit(column_name))\
                            .withColumn('check_name',F.lit(check_types))\
                            .withColumn('check_id',F.lit('tilt_2'))
                    df = df.union(df_2)
                elif check_types == 'values in range':

                    range_start = signalling_check.get('range_start')
                    range_end = signalling_check.get('range_end')

                    df_3 = check_values_in_range(spark_session, dataframe, column_name, range_start, range_end)

                    df_3 = df_3.withColumn('table_name',F.lit(table_name))\
                            .withColumn('column_name',F.lit(column_name))\
                            .withColumn('check_name',F.lit(check_types))\
                            .withColumn('check_id',F.lit('tilt_3'))
                    df = df.union(df_3)

    monitoring_values_df = read_table(spark_session,'monitoring_values')   
    monitoring_values_df = monitoring_values_df.filter(F.col('to_date')=='2099-12-31').select([F.col(column) for column in df.columns if column not in ['from_date','to_date']])
  # filter the monitoring values table to exclude all records that already exists for that table
    monitoring_values_df_filtered = monitoring_values_df.filter(F.col('table_name')!= table_name)
    monitoring_values_df = monitoring_values_df_filtered.union(df)
    
    write_table(spark_session, monitoring_values_df, 'monitoring_values')
    return monitoring_values_df
