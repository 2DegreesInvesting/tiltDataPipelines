from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType
import pyspark.sql.functions as F
# from functions.spark_session import read_table


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
    columnsValue = list(map(lambda x: str("'") + str(x) +
                        str("',`") + str(x) + str("`"), columns))
    stackCols = ','.join(x for x in columnsValue)
    df_1 = df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")\
             .select(pivotCol, "col0", "col1")
    final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(F.concat_ws("", F.collect_list(col("col1"))))\
                   .withColumnRenamed("col0", pivotCol)
    return final_df


def check_value_within_list(dataframe: DataFrame, column_name: list, value_list: list) -> int:
    """
    Filter a DataFrame based on a list of values within a specific column and return the count of valid rows.

    This function takes a DataFrame and filters it to include only rows where the values in the specified
    column match any of the values in the provided 'value_list'. It then returns the count of valid rows
    that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input DataFrame to filter.
    - column_name (list): A list containing the column in the DataFrame to filter by.
    - value_list (list): A list of values to compare with the values in the specified column.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' match any
      of the values in 'value_list'.
    """
    valid_count = dataframe.filter(
        F.col(column_name[0]).isin(value_list)).count()
    return valid_count


def calculate_filled_values(dataframe: DataFrame) -> DataFrame:
    """
    Calculate the count of filled and total values for each column in a DataFrame.

    This function takes a DataFrame and computes the count of filled (non-null) and total
    values for each column. It generates a summary DataFrame containing the results.

    Parameters:
    - dataframe (DataFrame): The input DataFrame for which filled value counts are calculated.

    Returns:
    - DataFrame: A summary DataFrame containing the following columns:
        - 'check_id': A constant identifier ('tilt_1') for this specific check.
        - 'column_name': The name of the column.
        - 'check_name': A constant description ('Check if values are filled') for this check.
        - 'total_count': The total number of rows in the DataFrame.
        - 'valid_count': The count of non-null (filled) values for each column.
    """
    total_count = dataframe.count()
    df = dataframe.select([(F.count(F.when(F.isnull(c), c)
                                    .when(F.col(c) == 'NA', None)
                                    .when(F.col(c) == 'nan', None))).alias(c) for c in dataframe.columns]) \
        .withColumn('invalid_count_column', F.lit('invalid_count'))
    df = TransposeDF(df, dataframe.columns, 'invalid_count_column')
    df = df.withColumn('total_count', F.lit(total_count).cast(IntegerType())) \
        .withColumn('valid_count', F.lit(total_count).cast(IntegerType()) - F.col('invalid_count').cast(IntegerType())) \
        .withColumn('check_name', F.lit('Check if values are filled')) \
        .withColumn('check_id', F.lit('tilt_1')) \
        .withColumn('signalling_id', F.lit(None)) \
        .withColumnRenamed('invalid_count_column', 'column_name')
    col_order = ['signalling_id', 'check_id', 'column_name',
                 'check_name', 'total_count', 'valid_count']
    df = df.select(col_order).filter(~F.col('column_name').isin(
        ['from_date', 'to_date', 'tiltRecordID']))

    return df


def check_values_in_range(dataframe: DataFrame, column_name: list, range_start: int, range_end: int) -> int:
    """
    Filter a Spark DataFrame to include rows where values in a specified column are within a given range,
    and return the count of valid rows.

    This function takes a Spark DataFrame and filters it to include only rows where the values in the specified
    'column_name' fall within the inclusive range specified by 'range_start' and 'range_end'. It then returns
    the count of valid rows that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to filter.
    - column_name (list): A list containing the column in the DataFrame to filter by.
    - range_start (int): The inclusive lower bound of the range for filtering.
    - range_end (int): The inclusive upper bound of the range for filtering.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' fall within
      the specified range.   
    """

    valid_count = dataframe.filter(
        col(column_name[0]).between(range_start, range_end)).count()

    return valid_count


def check_values_unique(dataframe: DataFrame, column_name: list) -> int:
    """
    Check the uniqueness of values in a specified column of a Spark DataFrame and return the count of unique values.

    This function takes a Spark DataFrame and evaluates the uniqueness of values in the specified 'column_name'.
    It returns the count of unique values in that column.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to analyze.
    - column_name (list): A list containing the column in the DataFrame to check if it is unique.

    Returns:
    - valid_count (int): The count of unique values in the specified column.
    """
    valid_count = dataframe.select(*[F.col(col)
                                   for col in column_name]).distinct().count()
    return valid_count


def check_values_format(dataframe: DataFrame, column_name: list, format: str) -> int:
    """
    Check if values in a specified column of a Spark DataFrame match a given regular expression pattern,
    and return the count of matching values.

    This function takes a Spark DataFrame and filters it to include only rows where the values in the specified
    'column_name' match the regular expression pattern provided in 'format'. It then returns the count of rows
    that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to filter.
    - column_name (list): A list containing the column in the DataFrame to check for format compliance.
    - format (str): The regular expression pattern to match against the values in the specified column.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' match the
      specified regular expression pattern.
    """
    valid_count = dataframe.filter(F.col(column_name[0]).rlike(format)).count()
    return valid_count


def check_values_consistent(spark_session: SparkSession, dataframe: DataFrame, column_name: list, compare_df: DataFrame, join_columns: list) -> int:
    """
    Checks the consistency of values in a specified column between the input DataFrame and a comparison table.

    Args:
        spark_session (SparkSession): The SparkSession instance.
        dataframe (DataFrame): The DataFrame to be checked for consistency.
        column_name (list): A list containing the column in the DataFrame which values will be checked.
        compare_table (str): The name of the comparison table in the same SparkSession.
        join_columns (list): A list of column names used for joining the input DataFrame and the comparison table.

    Returns:
        int: The count of rows where the values in the specified column match between the two tables.

    Note:
        - This function performs a left join between the input DataFrame and the comparison table using the specified join columns.
        - It compares values in the 'column_name' from both tables.
        - Rows where the values match are counted, and the count is returned as an integer.

    """
    compare_df = compare_df.select(
        join_columns + [F.col(column_name[0]).alias('compare_' + column_name[0])])

    joined_df = dataframe.select(
        [column_name[0]] + join_columns).join(compare_df, on=join_columns, how='left')
    valid_count = joined_df.filter(
        F.col(column_name[0]) == F.col('compare_' + column_name[0])).count()

    return valid_count


def check_expected_value_count(spark_session: SparkSession, dataframe: DataFrame, groupby_columns: list, expected_count: int) -> int:
    """
    Check the count of rows in a DataFrame grouped by specific columns and compare it to an expected count.

    With this function we want to check if a combination of certain columns exists a certain amount of times in a table.
    For example when 6 benchmarks are expected per company and each benchmark has 3 risk levels, a total of 18 rows per company would be expected.

    Parameters:
    - spark_session (SparkSession): The Spark session.
    - dataframe (DataFrame): The input DataFrame to be analyzed.
    - groupby_columns (list): A list of column names to group the DataFrame by.
    - expected_count (int): The expected count to compare with.

    Returns:
    - int: The count of rows in the DataFrame that match the expected count after grouping.

    """
    groupby_columns_list = [F.col(column) for column in groupby_columns]
    valid_rows = dataframe.groupby(groupby_columns_list).agg(F.count('tiltRecordID').alias(
        'count')).filter(F.col('count') == expected_count).select(groupby_columns_list)
    valid_count = dataframe.join(
        valid_rows, how='inner', on=groupby_columns).count()

    return valid_count


def check_expected_distinct_value_count(spark_session: SparkSession, dataframe: DataFrame, groupby_columns: list, expected_count: int, distinct_columns: list) -> int:
    """
    Check the count of distinct values in specific columns of a DataFrame grouped by other columns
    and compare it to an expected count.

    With this function we check the count of different values across one group. 
    For example when checking if every company has a record for all of the benchmarks, it is possible that a benchmark exists multiple times by being subdivided into multiple sub scenarios.
    At this point we then need to check the unique amount of benchmarks to get the actual amount of applied benchmarks and not rows.

    Parameters:
    - spark_session (SparkSession): The Spark session.
    - dataframe (DataFrame): The input DataFrame to be analyzed.
    - groupby_columns (list): A list of column names to group the DataFrame by.
    - expected_count (int): The expected count of distinct values to compare with.
    - distinct_columns (list): A list of column names for which distinct values will be counted.

    Returns:
    - int: The count of rows in the DataFrame that match the expected count of distinct values after grouping.

    """
    groupby_columns_list = [F.col(column) for column in groupby_columns]
    distinct_columns_list = [F.col(column) for column in distinct_columns]
    valid_rows = dataframe.groupby(groupby_columns_list).agg(F.countDistinct(
        *distinct_columns_list).alias('count')).filter(F.col('count') == expected_count).select(groupby_columns_list)
    valid_count = dataframe.join(
        valid_rows, how='inner', on=groupby_columns).count()

    return valid_count


def column_sums_to_1(spark_session: SparkSession, dataframe: DataFrame, groupby_columns: list, sum_column: str) -> int:
    """
    Check if the sum of values in a specific column of a DataFrame, grouped by other columns,
    equals 1 and return the count of rows that meet this condition.

    In this check the aim is to make sure that columns like a share sum up to 100% or 1 in the case of our data. 
    Due to rounding differences in fractional shares, the implementation is to check if a sum lies between 98% and 102%.

    Parameters:
    - spark_session (SparkSession): The Spark session.
    - dataframe (DataFrame): The input DataFrame to be analyzed.
    - groupby_columns (list): A list of column names to group the DataFrame by.
    - sum_column (str): The column whose values will be summed and compared to 1.

    Returns:
    - int: The count of rows in the DataFrame where the sum of values in the 'sum_column' equals 1 after grouping.

    """

    groupby_columns_list = [F.col(column) for column in groupby_columns]
    valid_rows = dataframe.groupby(groupby_columns_list).agg(F.sum(sum_column).alias(
        'sum')).filter(F.col('sum').between(0.98, 1.02)).select(groupby_columns_list)
    valid_count = dataframe.join(
        valid_rows, how='inner', on=groupby_columns).count()

    return valid_count


def calculate_signalling_issues(spark_session: SparkSession, dataframe: DataFrame, signalling_check_dict: dict, signalling_check_dummy: DataFrame) -> DataFrame:
    """
    Calculate signalling issues based on a set of predefined checks for a given DataFrame.

    This function computes signalling issues in a DataFrame by applying a set of predefined checks,
    as specified in the `signalling_check_dict`. It generates a summary DataFrame containing the results.

    Parameters:
    - spark_session (SparkSession): The Spark session for working with Spark DataFrame.
    - dataframe (DataFrame): The input DataFrame on which the signalling checks will be applied.
    - signalling_check_dict (dict): A dictionary containing the specifications of signalling checks.
    - signalling_check_dummy (DataFrame): A template DataFrame for constructing the result.

    Returns:
    - DataFrame: A summary DataFrame with signalling issues, including the following columns:
        - 'column_name': The name of the column being checked.
        - 'check_name': The type of check being performed.
        - 'total_count': The total number of rows in the DataFrame.
        - 'valid_count': The count of valid values after applying the check.
        - 'check_id': A constant identifier for the specific check.

    Signalling Check Types:
    - 'values within list': Checks if values are within a specified list.
    - 'values in range': Checks if values are within a specified numerical range.
    - 'values are unique': Checks if values in the column are unique.
    - 'values have format': Checks if values match a specified format.
    - 'values are consistent': Checks if values in the column are consistent with values in another table.

    Note: Additional columns will be added based on the specific check type.
    """

    df = calculate_filled_values(dataframe)
    total_count = dataframe.count()

    for signalling_check in signalling_check_dict:
        check_types = signalling_check.get('check')
        column_name = signalling_check.get('columns')

        signalling_check_df = signalling_check_dummy.withColumn('column_name', F.lit(','.join(column_name)))\
            .withColumn('total_count', F.lit(total_count).cast(IntegerType()))
        if check_types == 'values within list':

            value_list = signalling_check.get('value_list')
            valid_count = check_value_within_list(
                dataframe, column_name, value_list)
            input_list = '","'.join([str(val) for val in value_list])[:100]
            description_string = f'values within list of: "{input_list}"'
            check_id = 'tilt_2'

        elif check_types == 'values in range':

            range_start = signalling_check.get('range_start')
            range_end = signalling_check.get('range_end')
            valid_count = check_values_in_range(
                dataframe, column_name, range_start, range_end)
            description_string = f'values between {str(range_start)} and {str(range_end)}'
            check_id = 'tilt_3'

        elif check_types == 'values are unique':

            valid_count = check_values_unique(dataframe, column_name)
            description_string = f"unique values in column `{column_name}`"
            check_id = 'tilt_4'

        elif check_types == 'values have format':

            check_format = signalling_check.get('format')
            valid_count = check_values_format(
                dataframe, column_name, check_format)
            description_string = f'values have format {check_format}'
            check_id = 'tilt_5'

        # elif check_types == 'values are consistent':

        #     table_to_compare = signalling_check.get('compare_table')
        #     columns_to_join = signalling_check.get('join_columns')
        #     df_to_compare = read_table(spark_session, table_to_compare)
        #     valid_count = check_values_consistent(spark_session,dataframe, column_name, df_to_compare, columns_to_join)
        #     input_list = '","'.join([str(val) for val in columns_to_join])[:100]
        #     description_string = f'values are consistent with column(s) "{input_list}" from table {table_to_compare}'
        #     check_id = 'tilt_6'

        elif check_types == 'values occur as expected':

            count_expected = signalling_check.get('expected_count')
            valid_count = check_expected_value_count(
                spark_session, dataframe, column_name, count_expected)
            description_string = f'values occur {count_expected} times'
            check_id = 'tilt_7'

        elif check_types == 'values sum to 1':

            sum_col = signalling_check.get('sum_column')
            valid_count = column_sums_to_1(
                spark_session, dataframe, column_name, sum_col)
            description_string = f'values in column "{sum_col}" sum to 1'
            check_id = 'tilt_8'

        elif check_types == 'distinct values occur as expected':

            count_expected = signalling_check.get('expected_count')
            distinct_columns = signalling_check.get('distinct_columns')
            valid_count = check_expected_distinct_value_count(
                spark_session, dataframe, column_name, count_expected, distinct_columns)
            input_list = '","'.join([str(val)
                                    for val in distinct_columns])[:100]
            description_string = f'{count_expected} distinct values occur in column {input_list}'
            check_id = 'tilt_9'

        signalling_check_df = signalling_check_df.withColumn('valid_count', F.lit(valid_count).cast(IntegerType()))\
            .withColumn('check_id', F.lit(check_id))\
            .withColumn('check_name', F.lit(description_string))\
            .withColumn('signalling_id', F.lit(None))

        df = df.union(signalling_check_df).select(
            ['signalling_id', 'check_id', 'column_name', 'check_name', 'total_count', 'valid_count'])

    return df
