from pyspark.sql import DataFrame, SparkSession
from functions.spark_session import read_table, create_spark_session, write_table
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from functions.signalling_rules import signalling_checks_dictionary
from pyspark.sql.types import IntegerType

def TransposeDF(df: DataFrame, columns: list, pivotCol: str) -> DataFrame:
    """
    Transposes a DataFrame by pivoting specified columns and aggregating values.

    This function takes a DataFrame `df` and transposes it by pivoting the values in the
    specified `columns`. It aggregates the pivoted values into new columns based on the
    unique values in the `pivotCol` column.

    Parameters:
    - df (DataFrame): The input DataFrame to be transposed.
    - columns (list): A list of column names to pivot and transpose.
    - pivotCol (str): The column name to be used for pivoting and creating new columns.

    Returns:
    - DataFrame: A transposed DataFrame with the `pivotCol` values as columns and
      aggregated values from the specified `columns`.

    """
    columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
    stackCols = ','.join(x for x in columnsValue)
    df_1 = df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")\
             .select(pivotCol, "col0", "col1")
    final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(F.concat_ws("", F.collect_list(col("col1"))))\
                   .withColumnRenamed("col0", pivotCol)
    return final_df


def check_value_within_list(dataframe: DataFrame, column_name: str, value_list: list) -> int:
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


def calculate_filled_values(table_name: str, dataframe: DataFrame) -> DataFrame:
    """
    Calculates the count of filled and total values for each column in a DataFrame.

    This function takes a DataFrame and computes the count of filled (non-null) and total
    values for each column. It generates a summary DataFrame containing the results.

    Parameters:
    - table_name (str): The name of the table associated with the DataFrame.
    - dataframe (DataFrame): The input DataFrame for which filled value counts are calculated.

    Returns:
    - DataFrame: A summary DataFrame containing the following columns:
        - 'check_id': A constant identifier ('tilt_1') for this specific check.
        - 'table_name': The name of the table associated with the DataFrame.
        - 'column_name': The name of the DataFrame's columns.
        - 'check_name': A constant description ('Check if values are filled') for this check.
        - 'total_count': The total number of rows in the DataFrame.
        - 'valid_count': The count of filled (non-null) values for each column.

    """
    total_count = dataframe.count()
    df = dataframe.select([(F.count(F.when(F.isnull(c), c).when(F.col(c) == 'NA', None))).alias(c) for c in dataframe.columns]) \
            .withColumn('column_name', F.lit('valid_count'))
    df = TransposeDF(df, dataframe.columns, 'column_name')
    df = df.withColumn('total_count', F.lit(total_count).cast(IntegerType())) \
            .withColumn('valid_count', F.lit(total_count).cast(IntegerType()) - F.col('valid_count').cast(IntegerType())) \
            .withColumn('check_name', F.lit('Check if values are filled')) \
            .withColumn('check_id', F.lit('tilt_1'))
    col_order = ['check_id', 'table_name', 'column_name', 'check_name', 'total_count', 'valid_count']
    df = df.withColumn('table_name', F.lit(table_name)).select(col_order)

    return df



def check_values_in_range(dataframe: DataFrame, column_name: str, range_start: int, range_end: int) -> int:
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

def check_values_consistent(spark_session: SparkSession, dataframe: DataFrame, column_name: str, compare_table: str, join_columns: list) -> int:
    """
    Checks the consistency of values in a specified column between the input DataFrame and a comparison table.

    Args:
        spark_session (SparkSession): The SparkSession instance.
        dataframe (DataFrame): The DataFrame to be checked for consistency.
        column_name (str): The name of the column whose values will be checked.
        compare_table (str): The name of the comparison table in the same SparkSession.
        join_columns (list): A list of column names used for joining the input DataFrame and the comparison table.

    Returns:
        int: The count of rows where the values in the specified column match between the two tables.

    Note:
        - This function performs a left join between the input DataFrame and the comparison table using the specified join columns.
        - It compares values in the 'column_name' from both tables.
        - Rows where the values match are counted, and the count is returned as an integer.

    """
    compare_df = read_table(spark_session, compare_table)
    compare_df = compare_df.select(join_columns + [F.col(column_name).alias('compare_' + column_name)])

    joined_df = dataframe.select([column_name] + join_columns).join(compare_df, on=join_columns, how='left')
    valid_count = joined_df.filter(F.col(column_name) == F.col('compare_' + column_name)).count()

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
    df = calculate_filled_values(table_name, dataframe)

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
                    
                elif check_types == 'values are consistent':
                    
                    table_to_compare = signalling_check.get('compare_table')
                    columns_to_join = signalling_check.get('join_columns')
                    valid_count = check_values_consistent(spark_session,dataframe, column_name, table_to_compare, columns_to_join)
                    check_id = 'tilt_6'
                    
    
                signalling_check_df = signalling_check_df.withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))
                signalling_check_df = signalling_check_df.withColumn('check_id',F.lit(check_id))
                
                df = df.union(signalling_check_df)
    
    monitoring_values_df = read_table(spark_session,'monitoring_values') 
    monitoring_values_df = monitoring_values_df.filter(F.col('to_date')=='2099-12-31').select([F.col(column) for column in df.columns if column not in ['from_date','to_date']])
    # filter the monitoring values table to exclude all records that already exists for that table
    monitoring_values_df_filtered = monitoring_values_df.filter(F.col('table_name')!= table_name)
    monitoring_values_df = monitoring_values_df_filtered.union(df)
    write_table(spark_session, monitoring_values_df, 'monitoring_values')
    return monitoring_values_df
