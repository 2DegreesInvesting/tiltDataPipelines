from functions.spark_session import read_table, write_table, create_spark_session
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.types import FloatType, IntegerType, TimestampType, BooleanType


def generate_table(table_name: str) -> None:

    spark_generate = create_spark_session()

    if table_name == 'geographies_raw':

        df = read_table(spark_generate, 'geographies_landingzone')

        write_table(spark_generate, df,'geographies_raw')
    
    elif table_name == 'geographies_transform':

        df = read_table(spark_generate, 'geographies_raw')

        geographies_df = df.select('ID', 'Name', 'Shortname', 'Geographical Classification')

        geographies_related_df = df.select('Shortname', 'Contained and Overlapping Geographies')

        geographies_related_df = geographies_related_df.withColumn('Shortname_related', F.explode(F.split('Contained and Overlapping Geographies', ';')))

        geographies_related_df = geographies_related_df.select('Shortname', 'Shortname_related')

        write_table(spark_generate, geographies_df, 'geographies_transform')

        write_table(spark_generate, geographies_related_df, 'geographies_related')

    elif table_name == 'unindentified_ao_raw':

        df = read_table(spark_generate, 'unindentified_ao_landingzone')

        write_table(spark_generate, df, 'unindentified_ao_raw')

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

        unidentified_ao_df = read_table(spark_generate, 'unindentified_ao_raw')
        cutoff_ao_df = read_table(spark_generate, 'cut_off_ao_raw')
        en15804_ao_df = read_table(spark_generate, 'en15804_ao_raw')
        consequential_ao_df = read_table(spark_generate, 'consequential_ao_raw')

        unidentified_ao_df = unidentified_ao_df.withColumn('Reference Product Name', F.lit(None))

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

        activity_list = ['Activity UUID','EcoQuery URL','Activity Name',
                        'Geography','Time Period','Special Activity Type',
                        'Sector','ISIC Classification','ISIC Section']

        relational_list = ['Activity UUID & Product UUID', 'Activity UUID', 'Product UUID','AO Method']

        cutoff_products = cutoff_ao_df.select(product_list)
        cutoff_activities = cutoff_ao_df.select(activity_list)
        cutoff_relations = cutoff_ao_df.select(relational_list)
        
        unidentified_products = unidentified_ao_df.select(product_list)
        unidentified_activities = unidentified_ao_df.select(activity_list)

        en15804_products = en15804_ao_df.select(product_list)
        en15804_activities = en15804_ao_df.select(activity_list)
        en15804_relations = en15804_ao_df.select(relational_list)

        consequential_products = consequential_ao_df.select(product_list)
        consequential_activities = consequential_ao_df.select(activity_list)
        consequential_relations = consequential_ao_df.select(relational_list)

        products_df = cutoff_products.union(unidentified_products)\
                        .union(en15804_products).union(consequential_products).distinct()

        activities_df = cutoff_activities.union(unidentified_activities)\
                        .union(en15804_activities).union(consequential_activities).distinct()

        relational_df = cutoff_relations.union(en15804_relations)\
                        .union(consequential_relations).distinct()

        write_table(spark_generate, products_df, 'products_transformed')
        write_table(spark_generate, activities_df, 'activities_transformed')
        write_table(spark_generate, relational_df, 'products_activities_transformed','AO Method')

    elif table_name == 'lcia_methods_raw':

        df = read_table(spark_generate, 'lcia_methods_landingzone')

        write_table(spark_generate, df, 'lcia_methods_raw')

    elif table_name == 'lcia_methods_transform':

        df = read_table(spark_generate, 'lcia_methods_raw')

        write_table(spark_generate, df, 'lcia_methods_transform')

    elif table_name == 'impact_categories_raw':

        df = read_table(spark_generate, 'impact_categories_landingzone')

        write_table(spark_generate, df, 'impact_categories_raw')

    elif table_name == 'impact_categories_transform':

        df = read_table(spark_generate, 'impact_categories_raw')

        write_table(spark_generate, df, 'impact_categories_transform')

    elif table_name == 'intermediate_exchanges_raw':

        df = read_table(spark_generate, 'intermediate_exchanges_landingzone')

        write_table(spark_generate, df, 'intermediate_exchanges_raw')

    elif table_name == 'intermediate_exchanges_transform':

        df = read_table(spark_generate, 'intermediate_exchanges_raw')

        write_table(spark_generate, df, 'intermediate_exchanges_transform')

    elif table_name == 'elementary_exchanges_raw':

        df = read_table(spark_generate, 'elementary_exchanges_landingzone')

        write_table(spark_generate, df, 'elementary_exchanges_raw')

    elif table_name == 'elementary_exchanges_transform':

        df = read_table(spark_generate, 'elementary_exchanges_raw')

        write_table(spark_generate, df, 'elementary_exchanges_transform')

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

        df = read_table(spark_generate, 'sea_food_landingzone')

        sea_food = df

        # List of column names to replace to boolean values
        columns_to_replace = ["supply_chain_feed", "supply_chain_fishing", "supply_chain_aquaculture", "supply_chain_processing",
                      "supply_chain_wholesale_distribution", "supply_chain_retail", "supply_chain_foodservice", "supply_chain_fishing_vessels",
                      "supply_chain_fishing_and_aquaculture_gear_equipment", "supply_chain_other", "full_species_disclosure_for_entire_portfolio",
                      "full_species_disclosure_for_at_least_part_of_portfolio"]
        for column_name_1 in columns_to_replace:
            print("check")
            sea_food = sea_food.withColumn(column_name_1, 
                               when(col(column_name_1) == "yes", F.lit(True))
                               .when(col(column_name_1) == "no", F.lit(False))
                               .otherwise(F.lit(None)))    
            
        # List of column names to replace to float values
        columns_to_replace_float = ["reporting_precision_pt_score", "world_benchmarking_alliance_seafood_stewardship_index", "ocean_health_index_score_2012",
                      "ocean_health_index_score_2021", "ocean_health_index_score_percent_change_2021_2012", "fish_source_score_management_quality",
                      "fish_source_score_managers_compliance", "fish_source_score_fishers_compliance", "fish_source_score_current_stock_health",
                      "fish_source_score_future_stock_health", "sea_around_us_unreported_total_catch_percent","sea_around_us_bottom_trawl_total_catch_percent_35", "sea_around_us_gillnets_total_catch_percent", "global_fishing_index_data_availability_on_stock_sustainability", "global_fishing_index_proportion_of_assessed_fish_stocks_that_is_sustainable", "global_fishing_index_proportion_of_1990_2018_catches_that_is_sustainable", "global_fishing_index_proportion_of_1990_2018_catches_that_is_overfished", "global_fishing_index_proportion_of_1990_2018_catches_that_is_not_assessed", "global_fishing_index_fisheries_governance_score", "global_fishing_index_alignment_with_international_standards_for_protecting_worker_rights_and_safety_in_fisheries_assessment_score", "global_fishing_index_fishery_subsidy_program_assessment_score", "global_fishing_index_knowledge_on_fishing_fleets_assessment_score",
                      "global_fishing_index_compliance_monitoring_and_surveillance_programs_assessment_score", "global_fishing_index_severity_of_fishery_sanctions_assessment_score", "global_fishing_index_access_of_foreign_fishing_fleets_assessment_score"]

        for column_name_2 in columns_to_replace_float:
            sea_food = sea_food.withColumn(column_name_2, col(column_name_2).cast(FloatType()))
        #print(sea_food.filter(col("global_fishing_index_access_of_foreign_fishing_fleets_assessment_score").isNotNull()).head())
        write_table(spark_generate, sea_food, 'sea_food_raw')

    elif table_name == 'products_companies_raw':

        df = read_table(spark_generate, 'products_companies_landingzone')

        write_table(spark_generate, df,'products_companies_raw')

    elif table_name == 'companies_raw':

        df = read_table(spark_generate, 'companies_landingzone')

        companies = df

        # List of column names to replace to byte values
        columns_to_replace_byte = ["min_headcount", "max_headcount"]

        for column_name in columns_to_replace_byte:
            companies = companies.withColumn(column_name, col(column_name).cast(IntegerType()))

        # to boolean values
        column_name = "verified_by_europages"
        companies = companies.withColumn(column_name, col(column_name).cast(BooleanType()))

        # to timestamp values
        column_name = "download_datetime"
        companies = companies.withColumn(column_name, col(column_name).cast(TimestampType()))  

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

    spark_generate.stop()
