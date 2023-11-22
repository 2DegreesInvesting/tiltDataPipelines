import os
from functions.spark_session import read_table, write_table, create_spark_session
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, BooleanType, DoubleType, ShortType, DoubleType


def generate_table(table_name: str) -> None:

    spark_generate = create_spark_session()

    if table_name == 'geographies_raw':

        df = read_table(spark_generate, 'geographies_landingzone')

        # Filter out the empty values in the ID column, as empty records are read in from the source data.
        df = df.filter(~F.isnull(F.col('ID')))

        write_table(spark_generate, df,'geographies_raw')
    
    elif table_name == 'geographies_transform':

        df = read_table(spark_generate, 'geographies_raw')

        geographies_df = df.select('ID', 'Name', 'Shortname', 'Geographical Classification')

        geographies_related_df = df.select('Shortname', 'Contained and Overlapping Geographies')

        geographies_related_df = geographies_related_df.withColumn('Shortname_related', F.explode(F.split('Contained and Overlapping Geographies', ';')))

        geographies_related_df = geographies_related_df.select('Shortname', 'Shortname_related')

        write_table(spark_generate, geographies_df, 'geographies_transform')

        write_table(spark_generate, geographies_related_df, 'geographies_related')

    elif table_name == 'undefined_ao_raw':

        df = read_table(spark_generate, 'undefined_ao_landingzone')

        write_table(spark_generate, df, 'undefined_ao_raw')

    elif table_name == 'cut_off_ao_raw':

        df = read_table(spark_generate, 'cut_off_ao_landingzone')

        write_table(spark_generate, df, 'cut_off_ao_raw')

    elif table_name == 'en15804_ao_raw':

        df = read_table(spark_generate, 'en15804_ao_landingzone')

        write_table(spark_generate, df, 'en15804_ao_raw')

    elif table_name == 'consequential_ao_raw':

        df = read_table(spark_generate, 'consequential_ao_landingzone')

        write_table(spark_generate, df, 'consequential_ao_raw')

    elif table_name == 'products_activities_transformed':

        undefined_ao_df = read_table(spark_generate, 'undefined_ao_raw')
        cutoff_ao_df = read_table(spark_generate, 'cut_off_ao_raw')
        en15804_ao_df = read_table(spark_generate, 'en15804_ao_raw')
        consequential_ao_df = read_table(spark_generate, 'consequential_ao_raw')

        undefined_ao_df = undefined_ao_df.withColumn('Reference Product Name', F.lit(None))

        cutoff_ao_df = cutoff_ao_df.withColumn('Product Group', F.lit(None))
        cutoff_ao_df = cutoff_ao_df.withColumn('Product Name', F.lit(None))
        cutoff_ao_df = cutoff_ao_df.withColumn('AO Method', F.lit('CutOff'))
        en15804_ao_df = en15804_ao_df.withColumn('Product Group', F.lit(None))
        en15804_ao_df = en15804_ao_df.withColumn('Product Name', F.lit(None))
        en15804_ao_df = en15804_ao_df.withColumn('AO Method', F.lit('EN15804'))
        consequential_ao_df = consequential_ao_df.withColumn('Product Group', F.lit(None))
        consequential_ao_df = consequential_ao_df.withColumn('Product Name', F.lit(None))
        consequential_ao_df = consequential_ao_df.withColumn('AO Method', F.lit('Consequential'))

        product_list = ['Product UUID','Product Group',
                        'Product Name','Reference Product Name',
                        'CPC Classification','Unit',
                        'Product Information','CAS Number']

        activity_list = ['Activity UUID','Activity Name',
                        'Geography','Time Period','Special Activity Type',
                        'Sector','ISIC Classification','ISIC Section']

        relational_list = ['Activity UUID & Product UUID', 'Activity UUID', 'Product UUID','EcoQuery URL','AO Method']

        cutoff_products = cutoff_ao_df.select(product_list)
        cutoff_activities = cutoff_ao_df.select(activity_list)
        cutoff_relations = cutoff_ao_df.select(relational_list)
        
        undefined_products = undefined_ao_df.select(product_list)
        undefined_activities = undefined_ao_df.select(activity_list)

        en15804_products = en15804_ao_df.select(product_list)
        en15804_activities = en15804_ao_df.select(activity_list)
        en15804_relations = en15804_ao_df.select(relational_list)

        consequential_products = consequential_ao_df.select(product_list)
        consequential_activities = consequential_ao_df.select(activity_list)
        consequential_relations = consequential_ao_df.select(relational_list)

        products_df = cutoff_products.union(undefined_products)\
                        .union(en15804_products).union(consequential_products).distinct()

        activities_df = cutoff_activities.union(undefined_activities)\
                        .union(en15804_activities).union(consequential_activities).distinct()

        relational_df = cutoff_relations.union(en15804_relations)\
                        .union(consequential_relations).distinct()

        write_table(spark_generate, products_df, 'products_transformed')
        write_table(spark_generate, activities_df, 'activities_transformed')
        write_table(spark_generate, relational_df, 'products_activities_transformed','AO Method')

    elif table_name == 'lcia_methods_raw':

        df = read_table(spark_generate, 'lcia_methods_landingzone')

        write_table(spark_generate, df, 'lcia_methods_raw')

    elif table_name == 'impact_categories_raw':

        df = read_table(spark_generate, 'impact_categories_landingzone')

        write_table(spark_generate, df, 'impact_categories_raw')

    elif table_name == 'intermediate_exchanges_raw':

        df = read_table(spark_generate, 'intermediate_exchanges_landingzone')

        write_table(spark_generate, df, 'intermediate_exchanges_raw')

    elif table_name == 'elementary_exchanges_raw':

        df = read_table(spark_generate, 'elementary_exchanges_landingzone')

        write_table(spark_generate, df, 'elementary_exchanges_raw')

    elif table_name == 'issues_companies_raw':

        df = read_table(spark_generate, 'issues_companies_landingzone')

        write_table(spark_generate, df,'issues_companies_raw')
        
    elif table_name == 'issues_raw':

        df = read_table(spark_generate, 'issues_landingzone')

        write_table(spark_generate, df,'issues_raw')

    elif table_name == 'sea_food_companies_raw':

        df = read_table(spark_generate, 'sea_food_companies_landingzone')

        write_table(spark_generate, df,'sea_food_companies_raw')

    elif table_name == 'sea_food_raw':

        sea_food = read_table(spark_generate, 'sea_food_landingzone')

        # List of column names to replace to boolean values
        columns_to_replace = ["supply_chain_feed", "supply_chain_fishing", "supply_chain_aquaculture", "supply_chain_processing",
                      "supply_chain_wholesale_distribution", "supply_chain_retail", "supply_chain_foodservice", "supply_chain_fishing_vessels",
                      "supply_chain_fishing_and_aquaculture_gear_equipment", "supply_chain_other", "full_species_disclosure_for_entire_portfolio",
                      "full_species_disclosure_for_at_least_part_of_portfolio"]
        for column_name_1 in columns_to_replace:
            
            sea_food = sea_food.withColumn(column_name_1, 
                               when(F.col(column_name_1) == "yes", F.lit(True))
                               .when(F.col(column_name_1) == "no", F.lit(False))
                               .otherwise(F.lit(None)))    
            
        # List of column names to replace to float values
        columns_to_replace_float = ["reporting_precision_pt_score", "world_benchmarking_alliance_seafood_stewardship_index", "ocean_health_index_score_2012",
                      "ocean_health_index_score_2021", "ocean_health_index_score_percent_change_2021_2012", "fish_source_score_management_quality",
                      "fish_source_score_managers_compliance", "fish_source_score_fishers_compliance", "fish_source_score_current_stock_health",
                      "fish_source_score_future_stock_health", "sea_around_us_unreported_total_catch_percent","sea_around_us_bottom_trawl_total_catch_percent_35", "sea_around_us_gillnets_total_catch_percent", "global_fishing_index_data_availability_on_stock_sustainability", "global_fishing_index_proportion_of_assessed_fish_stocks_that_is_sustainable", "global_fishing_index_proportion_of_1990_2018_catches_that_is_sustainable", "global_fishing_index_proportion_of_1990_2018_catches_that_is_overfished", "global_fishing_index_proportion_of_1990_2018_catches_that_is_not_assessed", "global_fishing_index_fisheries_governance_score", "global_fishing_index_alignment_with_international_standards_for_protecting_worker_rights_and_safety_in_fisheries_assessment_score", "global_fishing_index_fishery_subsidy_program_assessment_score", "global_fishing_index_knowledge_on_fishing_fleets_assessment_score",
                      "global_fishing_index_compliance_monitoring_and_surveillance_programs_assessment_score", "global_fishing_index_severity_of_fishery_sanctions_assessment_score", "global_fishing_index_access_of_foreign_fishing_fleets_assessment_score"]

        for column_name_2 in columns_to_replace_float:
            sea_food = sea_food.withColumn(column_name_2, F.col(column_name_2).cast(DoubleType()))

        # writing the dataframe is a temporary fix to avoid getting an error within the grpc package
        temp_seafood_location = 'abfss://raw@storagetiltdevelop.dfs.core.windows.net/sea_food_temp/'
        sea_food.coalesce(1).write.mode('overwrite').parquet(temp_seafood_location)

        sea_food = spark_generate.read.format('parquet').load(temp_seafood_location)
        
        write_table(spark_generate, sea_food, 'sea_food_raw')

    elif table_name == 'products_companies_raw':

        df = read_table(spark_generate, 'products_companies_landingzone')

        write_table(spark_generate, df, 'products_companies_raw')

    elif table_name == 'companies_raw':

        companies = read_table(spark_generate, 'companies_landingzone')

        # List of column names to replace to byte values
        columns_to_replace_byte = ["min_headcount", "max_headcount", "year_established"]

        for column_name in columns_to_replace_byte:
            companies = companies.withColumn(column_name, F.col(column_name).cast(IntegerType()))

        # to boolean values
        column_name = "verified_by_europages"
        companies = companies.withColumn(column_name, F.col(column_name).cast(BooleanType()))

        # to timestamp values
        column_name = "download_datetime"
        companies = companies.withColumn(column_name, F.col(column_name).cast(TimestampType()))  

        write_table(spark_generate, companies, 'companies_raw')     

    elif table_name == 'main_activity_raw':

        df = read_table(spark_generate, 'main_activity_landingzone')

        write_table(spark_generate, df,'main_activity_raw')    

    elif table_name == 'geography_raw':

        df = read_table(spark_generate, 'geography_landingzone')

        write_table(spark_generate, df,'geography_raw') 

    elif table_name == 'country_raw':

        df = read_table(spark_generate, 'country_landingzone')

        write_table(spark_generate, df,'country_raw') 

    elif table_name == 'delimited_products_raw':

        df = read_table(spark_generate, 'delimited_products_landingzone')

        write_table(spark_generate, df,'delimited_products_raw') 

    elif table_name == 'products_raw':

        df = read_table(spark_generate, 'products_landingzone')

        write_table(spark_generate, df,'products_raw') 

    elif table_name == 'categories_companies_raw':

        df = read_table(spark_generate, 'categories_companies_landingzone')

        write_table(spark_generate, df,'categories_companies_raw') 

    elif table_name == 'delimited_raw':

        df = read_table(spark_generate, 'delimited_landingzone')

        write_table(spark_generate, df,'delimited_raw') 

    elif table_name == 'clustered_delimited_raw':

        df = read_table(spark_generate, 'clustered_delimited_landingzone')

        write_table(spark_generate, df,'clustered_delimited_raw') 

    elif table_name == 'clustered_raw':

        df = read_table(spark_generate, 'clustered_landingzone')

        write_table(spark_generate, df,'clustered_raw')

    elif table_name == 'categories_sector_ecoinvent_delimited_raw':

        df = read_table(spark_generate, 'categories_sector_ecoinvent_delimited_landingzone')

        write_table(spark_generate, df,'categories_sector_ecoinvent_delimited_raw')

    elif table_name == 'categories_raw':

        df = read_table(spark_generate, 'categories_landingzone')

        write_table(spark_generate, df,'categories_raw')

    elif table_name == 'sector_ecoinvent_delimited_sector_ecoinvent_raw':

        df = read_table(spark_generate, 'sector_ecoinvent_delimited_sector_ecoinvent_landingzone')

        write_table(spark_generate, df,'sector_ecoinvent_delimited_sector_ecoinvent_raw')

    elif table_name == 'sector_ecoinvent_delimited_raw':

        df = read_table(spark_generate, 'sector_ecoinvent_delimited_landingzone')

        write_table(spark_generate, df,'sector_ecoinvent_delimited_raw')

    elif table_name == 'sector_ecoinvent_raw':

        df = read_table(spark_generate, 'sector_ecoinvent_landingzone')

        write_table(spark_generate, df,'sector_ecoinvent_raw')

    elif table_name == 'product_matching_complete_all_cases_raw':

        df = read_table(spark_generate, 'product_matching_complete_all_cases_landingzone')

        product_matching_df = df.select('group_var', 'ep_id', 'lca_id')

        write_table(spark_generate, product_matching_df, 'product_matching_complete_all_cases_raw')

    elif table_name == 'labelled_activity_v1.0_raw':

        df = read_table(spark_generate, 'labelled_activity_v1.0_landingzone')

        activity_matching_df = df.select('index','ep_act_id', 'ep_country', 'Activity UUID & Product UUID')

        write_table(spark_generate, activity_matching_df, 'labelled_activity_v1.0_raw')

    elif table_name == 'ep_companies_NL_postcode_raw':

        df = read_table(spark_generate, 'ep_companies_NL_postcode_landingzone')

        write_table(spark_generate, df, 'ep_companies_NL_postcode_raw')

    elif table_name == 'ep_ei_matcher_raw':

        df = read_table(spark_generate, 'ep_ei_matcher_landingzone')

        ep_ei_matcher = df

        # to replace to boolean values
        column_name = "multi_match"
        ep_ei_matcher = ep_ei_matcher.withColumn(column_name, F.col(column_name).cast(BooleanType()))

        write_table(spark_generate, ep_ei_matcher, 'ep_ei_matcher_raw')  

    elif table_name == 'scenario_targets_IPR_NEW_raw':

        df = read_table(spark_generate, 'scenario_targets_IPR_NEW_landingzone')

        scenario_targets_IPR_NEW = df

        # to replace to integer values
        column_name = "Year"
        scenario_targets_IPR_NEW = scenario_targets_IPR_NEW.withColumn(column_name, F.col(column_name).cast(ShortType()))

        column_names = ["Value", "Reductions"]
        # to decimal values
        for column in column_names:
            scenario_targets_IPR_NEW = scenario_targets_IPR_NEW.withColumn(column, F.col(column).cast(DoubleType())) 

        write_table(spark_generate, scenario_targets_IPR_NEW, 'scenario_targets_IPR_NEW_raw') 

    elif table_name == 'scenario_targets_WEO_NEW_raw':

        df = read_table(spark_generate, 'scenario_targets_WEO_NEW_landingzone')

        scenario_targets_WEO_NEW = df

        # to replace to integer values
        column_name = "YEAR"
        scenario_targets_WEO_NEW = scenario_targets_WEO_NEW.withColumn(column_name, F.col(column_name).cast(ShortType()))

        column_names = ["VALUE", "REDUCTIONS"]
        # to decimal values
        for column in column_names:
            scenario_targets_WEO_NEW = scenario_targets_WEO_NEW.withColumn(column, F.col(column).cast(DoubleType()))  

        write_table(spark_generate, scenario_targets_WEO_NEW, 'scenario_targets_WEO_NEW_raw') 

    elif table_name == 'scenario_tilt_mapper_2023-07-20_raw':

        df = read_table(spark_generate, 'scenario_tilt_mapper_2023-07-20_landingzone')

        write_table(spark_generate, df, 'scenario_tilt_mapper_2023-07-20_raw')

    elif table_name == 'sector_resolve_without_tiltsector_raw':

        df = read_table(spark_generate, 'sector_resolve_without_tiltsector_landingzone')

        write_table(spark_generate, df, 'sector_resolve_without_tiltsector_raw') 

    elif table_name == 'tilt_isic_mapper_2023-07-20_raw':

        df = read_table(spark_generate, 'tilt_isic_mapper_2023-07-20_landingzone')

        write_table(spark_generate, df, 'tilt_isic_mapper_2023-07-20_raw') 

    elif table_name == 'ecoinvent-v3.9.1_raw':

        df = read_table(spark_generate, 'ecoinvent-v3.9.1_landingzone')

        ecoinvent_licenced = df

        # to replace to decimal values
        column_name = "ipcc_2021_climate_change_global_warming_potential_gwp100_kg_co2_eq"
        ecoinvent_licenced = ecoinvent_licenced.withColumn(column_name, F.col(column_name).cast(DoubleType()))

        write_table(spark_generate, ecoinvent_licenced, 'ecoinvent-v3.9.1_raw') 

    elif table_name == 'ecoinvent_inputs_overview_raw_raw':

        df = read_table(spark_generate, 'ecoinvent_inputs_overview_raw_landingzone')

        write_table(spark_generate, df, 'ecoinvent_inputs_overview_raw_raw')

    elif table_name == 'ecoinvent_input_data_relevant_columns_raw':

        df = read_table(spark_generate, 'ecoinvent_input_data_relevant_columns_landingzone')

        ecoinvent_input_data_relevant_columns_raw = df

        # to replace to decimal values
        column_name = "exchange amount"
        ecoinvent_input_data_relevant_columns_raw = ecoinvent_input_data_relevant_columns_raw.withColumn(column_name, F.col(column_name).cast(DoubleType()))

        write_table(spark_generate, ecoinvent_input_data_relevant_columns_raw, 'ecoinvent_input_data_relevant_columns_raw') 

    elif table_name == 'tilt_sector_classification_raw':

        df = read_table(spark_generate, 'tilt_sector_classification_landingzone')

        write_table(spark_generate, df, 'tilt_sector_classification_raw')
    
    elif table_name == 'ictr_products_raw':
    
        df = read_table(spark_generate, 'ictr_products_landingzone')

        ictr_products = df

        # to decimal
        column_name = "input_co2_footprint"
        ictr_products = ictr_products.withColumn(column_name,F.col(column_name).cast(DoubleType()))

        write_table(spark_generate, ictr_products, "ictr_products_raw")

    elif table_name == 'istr_companies_raw':

        df = read_table(spark_generate, 'istr_companies_landingzone')

        istr_companies = df

        # List of columns to replace string 'nan' with None values
        columns_to_replace = istr_companies.columns

        for column_name in columns_to_replace:
            istr_companies = istr_companies.withColumn(column_name, 
                                           when(F.col(column_name) == "nan", F.lit(None)))

        write_table(spark_generate, istr_companies, 'istr_companies_raw')

    elif table_name == 'istr_products_raw':

        df = read_table(spark_generate, 'istr_products_landingzone')

        istr_products = df

        # List of columns to replace string 'nan' with None values
        columns_to_replace = istr_products.columns

        for column_name in columns_to_replace:
            istr_products = istr_products.withColumn(column_name, 
                                           when(F.col(column_name) == "nan", F.lit(None)))

        write_table(spark_generate, istr_products, 'istr_products_raw')

    elif table_name == 'pctr_products_raw':

        df = read_table(spark_generate, 'pctr_products_landingzone')

        pctr_products = df

         # to decimal
        column_name = "co2_footprint"
        pctr_products = pctr_products.withColumn(column_name,F.col(column_name).cast(DoubleType()))

        # List of columns to replace string 'not available' with None values
        columns_to_replace = ["isic_4digit"]

        for column_name_2 in columns_to_replace:
            pctr_products = pctr_products.withColumn(column_name_2, 
                                           when(F.col(column_name_2) == "not available", F.lit(None)))

        write_table(spark_generate, pctr_products, 'pctr_products_raw')

    elif table_name == 'pstr_companies_raw':

        df = read_table(spark_generate, 'pstr_companies_landingzone')

        pstr_companies = df

        # List of columns to replace string 'nan' with None values
        columns_to_replace = pstr_companies.columns

        for column_name in columns_to_replace:
            pstr_companies = pstr_companies.withColumn(column_name, 
                                           when(F.col(column_name) == "nan", F.lit(None)))

        write_table(spark_generate, pstr_companies, 'pstr_companies_raw')

    elif table_name == 'str_ipr_targets_raw':

        df = read_table(spark_generate, 'str_ipr_targets_landingzone')

        # List of column names to replace to double
        columns_to_replace_double = ["value", "co2_reductions"]

        str_ipr_targets = df
        
        for column_name in columns_to_replace_double:
            str_ipr_targets = str_ipr_targets.withColumn(column_name,F.col(column_name).cast(DoubleType()))
        # to short type
        column_to_short_type = "year"
        str_ipr_targets = str_ipr_targets.withColumn(column_to_short_type,F.col(column_to_short_type).cast(ShortType()))

        write_table(spark_generate, str_ipr_targets, 'str_ipr_targets_raw')

    elif table_name == 'str_weo_targets_raw':

        df = read_table(spark_generate, 'str_weo_targets_landingzone')

        str_weo_targets = df

        # List of column names to replace to double
        columns_to_replace_double = ["value", "co2_reductions"]

        for column_name in columns_to_replace_double:
            str_weo_targets = str_weo_targets.withColumn(column_name,F.col(column_name).cast(DoubleType()))
        # to short type
        column_to_short_type = "year"
        str_weo_targets = str_weo_targets.withColumn(column_to_short_type,F.col(column_to_short_type).cast(ShortType()))

        write_table(spark_generate, str_weo_targets, 'str_weo_targets_raw')

    elif table_name == 'xctr_companies_raw':

        df = read_table(spark_generate, 'xctr_companies_landingzone')

        xctr_companies = df

        # List of columns to replace string 'nan' with None values
        columns_to_replace = xctr_companies.columns

        for column_name in columns_to_replace:
            xctr_companies = xctr_companies.withColumn(column_name, 
                                           when(F.col(column_name) == "nan", F.lit(None)))
            
        write_table(spark_generate, xctr_companies, 'xctr_companies_raw'),
    
    elif table_name == 'mapper_ep_ei_raw':

        df = read_table(spark_generate, 'mapper_ep_ei_landingzone')

        mapper_ep_ei = df

        # to boolean
        column_name = "multi_match"
        mapper_ep_ei = mapper_ep_ei.withColumn(column_name,F.col(column_name).cast(BooleanType()))

        write_table(spark_generate, mapper_ep_ei, 'mapper_ep_ei_raw')

    elif table_name == 'ep_companies_raw':

        df = read_table(spark_generate, 'ep_companies_landingzone')

        write_table(spark_generate, df, 'ep_companies_raw')

    elif table_name == 'ei_input_data_raw':

        df = read_table(spark_generate, 'ei_input_data_landingzone')

        ei_input_data = df

        # to double
        column_name = "exchange_amount"
        ei_input_data = ei_input_data.withColumn(column_name,F.col(column_name).cast(DoubleType()))

        write_table(spark_generate, ei_input_data, 'ei_input_data_raw')

    elif table_name == 'ei_activities_overview_raw':

        df = read_table(spark_generate, 'ei_activities_overview_landingzone')

        write_table(spark_generate, df, 'ei_activities_overview_raw')

    elif table_name == 'ictr_company_result_raw':

        df = read_table(spark_generate, 'ictr_company_result_landingzone')

        df = df.withColumn('ICTR_share', F.col('ICTR_share').cast(DoubleType()))

        write_table(spark_generate, df, 'ictr_company_result_raw')

    elif table_name == 'ictr_product_result_raw':

        df = read_table(spark_generate, 'ictr_product_result_landingzone')

        write_table(spark_generate, df, 'ictr_product_result_raw')

    elif table_name == 'istr_company_result_raw':

        df = read_table(spark_generate, 'istr_company_result_landingzone')

        df = df.withColumn('ISTR_share', F.col('ISTR_share').cast(DoubleType()))

        write_table(spark_generate, df, 'istr_company_result_raw')

    elif table_name == 'istr_product_result_raw':

        df = read_table(spark_generate, 'istr_product_result_landingzone')

        df = df.withColumn('year', F.col('year').cast(IntegerType()))

        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None)))   

        write_table(spark_generate, df, 'istr_product_result_raw')

    elif table_name == 'pctr_company_result_raw':

        df = read_table(spark_generate, 'pctr_company_result_landingzone')

        df = df.withColumn('PCTR_share', F.col('PCTR_share').cast(DoubleType()))

        write_table(spark_generate, df, 'pctr_company_result_raw')

    elif table_name == 'pctr_product_result_raw':

        df = read_table(spark_generate, 'pctr_product_result_landingzone')

        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None)))   

        write_table(spark_generate, df, 'pctr_product_result_raw')

    elif table_name == 'pstr_company_result_raw':

        df = read_table(spark_generate, 'pstr_company_result_landingzone')

        df = df.withColumn('year', F.col('year').cast(IntegerType()))
        df = df.withColumn('PSTR_share', F.col('PSTR_share').cast(DoubleType()))

        write_table(spark_generate, df, 'pstr_company_result_raw')

    elif table_name == 'pstr_product_result_raw':

        df = read_table(spark_generate, 'pstr_product_result_landingzone')

        df = df.withColumn('year', F.col('year').cast(IntegerType()))
        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None)))   

        write_table(spark_generate, df, 'pstr_product_result_raw')

    elif table_name == 'emission_profile_company_raw':

        df = read_table(spark_generate, 'emission_profile_company_landingzone')

        cast_to_float = ['emission_share_ew','emission_share_bc','emission_share_wc','Co2e_upper','Co2e_lower']
        for col in cast_to_float:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

        write_table(spark_generate, df, 'emission_profile_company_raw')

    elif table_name == 'emission_profile_product_raw':

        df = read_table(spark_generate, 'emission_profile_product_landingzone')

        cast_to_float = ['Co2e_upper','Co2e_lower']
        for col in cast_to_float:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))
        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None)))  

        write_table(spark_generate, df, 'emission_profile_product_raw')

    elif table_name == 'emission_upstream_profile_company_raw':

        df = read_table(spark_generate, 'emission_upstream_profile_company_landingzone')

        cast_to_float = ['emission_upstream_share_ew','emission_upstream_share_bc','emission_upstream_share_wc','Co2e_input_lower','Co2e_input_upper']
        for col in cast_to_float:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

        write_table(spark_generate, df, 'emission_upstream_profile_company_raw')

    elif table_name == 'emission_upstream_profile_product_raw':

        df = read_table(spark_generate, 'emission_upstream_profile_product_landingzone')
        
        cast_to_float = ['Co2e_input_lower','Co2e_input_upper']
        for col in cast_to_float:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))
        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None))) 

        write_table(spark_generate, df, 'emission_upstream_profile_product_raw')

    elif table_name == 'sector_profile_company_raw':

        df = read_table(spark_generate, 'sector_profile_company_landingzone')
        
        cast_to_float = ['sector_share_ew','sector_share_bc','sector_share_wc']
        for col in cast_to_float:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

        df = df.withColumn('year', F.col('year').cast(IntegerType()))

        write_table(spark_generate, df, 'sector_profile_company_raw')

    elif table_name == 'sector_profile_product_raw':

        df = read_table(spark_generate, 'sector_profile_product_landingzone')

        df = df.withColumn('year', F.col('year').cast(IntegerType()))
        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None))) 

        write_table(spark_generate, df, 'sector_profile_product_raw')

    elif table_name == 'sector_upstream_profile_company_raw':

        df = read_table(spark_generate, 'sector_upstream_profile_company_landingzone')
        
        cast_to_float = ['sector_upstream_share_ew','sector_upstream_share_bc','sector_upstream_share_wc']
        for col in cast_to_float:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))
        df = df.withColumn('year', F.col('year').cast(IntegerType()))

        write_table(spark_generate, df, 'sector_upstream_profile_company_raw')

    elif table_name == 'sector_upstream_profile_product_raw':

        df = read_table(spark_generate, 'sector_upstream_profile_product_landingzone')

        df = df.withColumn('year', F.col('year').cast(IntegerType()))
        df = df.withColumn('multi_match', 
                            when(F.col('multi_match') == "TRUE", F.lit(True))
                            .when(F.col('multi_match') == "FALSE", F.lit(False))
                            .otherwise(F.lit(None))) 

        write_table(spark_generate, df, 'sector_upstream_profile_product_raw')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session. 
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()
