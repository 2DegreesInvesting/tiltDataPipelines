from functions.spark_session import read_table, create_spark_session
from signalling_functions import calculate_filled_values, check_value_within_list, check_values_in_range
import pyspark.sql.functions as F

spark_session = create_spark_session()


signalling_checks_dictionary = {
    'companies_raw': [
        {
            'check': 'values in range',
            'columns':['year_established'],
            'range_start' : 1800,
            'range_end': 2023
        }
        ],
    'main_activity_raw' : [{
            'check':'values within list',
            'columns':['main_activity'],
            'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    }],
    'undefined_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
    'cut_off_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
      'en15804_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
    'consequential_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
    'activities_transformed' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ]


}

table_name_list = ['categories_raw', 'categories_companies_raw', 'categories_sector_ecoinvent_delimited_raw', 'clustered_raw', 'clustered_delimited_raw', 'companies_raw', 'consequential_ao_raw',
                    'country_raw', 'cut_off_ao_raw', 'delimited_raw', 'delimited_products_raw', 'elementary_exchanges_raw', 'en15804_ao_raw', 'geographies_raw', 'intermediate_exchanges_raw',
                    'issues_raw', 'issues_companies_raw', 'labelled_activity_v1.0_raw', 'lcia_methods_raw', 'main_activity_raw', 'product_matching_complete_all_cases_raw', 'products_raw', 'products_companies_raw',
                    'sea_food_raw', 'sector_ecoinvent_raw', 'sector_ecoinvent_delimited_raw', 'sector_ecoinvent_delimited_sector_ecoinvent_raw', 'sector_resolve_raw', 'undefined_ao_raw', 'activities_transformed']

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
        df.show(10)

    for table_name in signalling_checks_dictionary:
            tables_list = signalling_checks_dictionary[table_name][0]
            check_types = tables_list.get('check')
            column_name = tables_list.get('columns')[0]
            value_list = tables_list.get('value_list')
            range_start = tables_list.get('range_start')
            range_end = tables_list.get('range_end')
            dataframe_2 = read_table(spark_session, table_name)
            if check_types == 'values within list':
                df_2 = check_value_within_list(spark_session, dataframe_2, column_name, value_list)
                df_2 = df_2.withColumn('table_name',F.lit(table_name))\
                        .withColumn('column_name',F.lit(column_name))\
                        .withColumn('check_name',F.lit(check_types))\
                        .withColumn('check_id',F.lit('tilt_2'))
                df_2.show(5)
            elif check_types == 'values in range':
                df_3 = check_values_in_range(spark_session, dataframe_2, column_name, range_start, range_end)
                df_3 = df_3.withColumn('table_name',F.lit(table_name))\
                        .withColumn('column_name',F.lit(column_name))\
                        .withColumn('check_name',F.lit(check_types))\
                        .withColumn('check_id',F.lit('tilt_3'))
                df_3.show(5)

                    # get all of the signalling check from dict
    
    # Check if all columns are filles
   
    # read in new table
        
    # Apply the checks from dictionary

    
       
            


        # Iterate over list of dicts
        # Check what check to do
        # Call the function for the specific check

    # Write results to table
    return None




for table_name in table_name_list:
    test = check_signalling_issues(table_name)
    test.show(10)