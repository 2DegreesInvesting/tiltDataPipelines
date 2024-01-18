from functions.spark_session import create_spark_session, check_signalling_issues

signalling_spark_session = create_spark_session()


table_name_list = ['categories_raw', 'categories_companies_raw', 'categories_sector_ecoinvent_delimited_raw', 'clustered_raw', 'clustered_delimited_raw', 'companies_raw', 'consequential_ao_raw',
                   'country_raw', 'cut_off_ao_raw', 'delimited_raw', 'delimited_products_raw', 'elementary_exchanges_raw', 'en15804_ao_raw', 'geographies_raw', 'intermediate_exchanges_raw',
                   'issues_raw', 'issues_companies_raw', 'labelled_activity_v1.0_raw', 'lcia_methods_raw', 'main_activity_raw', 'product_matching_complete_all_cases_raw', 'products_raw', 'products_companies_raw',
                   'sea_food_companies_raw', 'sector_ecoinvent_raw', 'sector_ecoinvent_delimited_raw', 'sector_ecoinvent_delimited_sector_ecoinvent_raw', 'sector_resolve_raw', 'undefined_ao_raw',
                   'activities_transformed', 'products_transformed', 'impact_categories_raw', 'ep_ei_matcher_raw']


# Apply the checks from dictionary
for table_name in table_name_list:
    check_signalling_issues(signalling_spark_session, table_name)
