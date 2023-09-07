# from signalling_rules import rules
from functions.spark_session import read_table, create_spark_session
# from signalling_functions import check_values_in_column
from signalling_functions import calculate_filled_values, check_value_within_list
# from pyspark.sql import DataFrame, SparkSession
from functions.tables import get_table_definition
import pyspark.sql.functions as F
from pyspark import SparkContext

spark_generate = create_spark_session()
dummy_quality_check = read_table(spark_generate,'dummy_quality_check')
print(dummy_quality_check)
result_data = dummy_quality_check.withColumn('check_id', F.lit('tilt_1'))
result_data.head(5)


spark_generate = create_spark_session()
dataframe = read_table(spark_generate, 'main_activity_raw')

value_list = dataframe.columns
test = calculate_filled_values(spark_generate, dataframe, value_list )
test.head(2)
# table_names = ['main_activity_raw']

# for table in table_names:

#     dataframe = read_table(spark_generate, table)
# column_names = dataframe.columns



signalling_checks_dictionary = {
    # 'companies_raw': [
    #     {
    #         'check': 'values in range',
    #         'columns':['year_established'],
    #         'range_start' : 1800,
    #         'range_end': 2023
    #     }
    #     ],
    'main_activity_raw' : [{
            'check':'values within list',
            'columns':['main_activity'],
            'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    }],
    # 'undefined_ao_raw' : [
    #     {
    #         'check':'values within list',
    #         'columns':['Special Activity Type'],
    #         'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     }
    # ],
    # 'cut_off_ao_raw' : [
    #     {
    #         'check':'values within list',
    #         'columns':['Special Activity Type'],
    #         'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     }
    # ],
    #   'en15804_ao_raw' : [
    #     {
    #         'check':'values within list',
    #         'columns':['Special Activity Type'],
    #         'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     }
    # ],
    # 'consequential_ao_raw' : [
    #     {
    #         'check':'values within list',
    #         'columns':['Special Activity Type'],
    #         'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     }
    # ],
    # 'activities_transformed' : [
    #     {
    #         'check':'values within list',
    #         'columns':['Special Activity Type'],
    #         'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     }
    # ]


}

def check_signalling_issues(table_name):
    # get all of the signalling check from dict
    for table_name in signalling_checks_dictionary:
        tables_list = signalling_checks_dictionary[table_name]
        check_info = tables_list[0]
        check_types = check_info.get('check')
        column_name = check_info.get('columns')[0]
        value_list = check_info.get('value_list')
        print(value_list)

   
    # read in new table
    table_name_list = ['product_matching_complete_all_cases_landingzone', 'main_activity_raw']

    df = read_table(spark_generate, "dummy_quality_check")

    df = df.withColumn('table_name',F.lit(table_name))
    df = df.withColumn('check_name',F.lit(check_types))
    df = df.withColumn('column_name',F.lit(column_name))
    df.head(5)

    # Check if all columns are filles
    for table in table_name_list:
        print(table)
        dataframe = read_table(spark_generate, table)
    dataframe_columns = dataframe.columns
    print(dataframe_columns)
    test = calculate_filled_values(spark_generate, dataframe, dataframe_columns)
    test.head()


    # Apply the checks from dictionary
    test = check_value_within_list(spark_generate, dataframe, column_name, value_list)
    test.head()
        # Iterate over list of dicts
        # Check what check to do
        # Call the function for the specific check

    # Write results to table

    return None

