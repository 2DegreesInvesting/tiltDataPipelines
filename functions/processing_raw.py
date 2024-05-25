import os
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, ShortType, DateType, ByteType, DecimalType
from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session
from functions.dataframe_helpers import rename


def generate_table(table_name: str) -> None:
    """
    Generate a specified table from the raw data using either a remote or the Databricks SparkSession.

    This function generates a specified table from the raw data. The table to be generated is determined by the 'table_name' argument.

    Args:
        table_name (str): The name of the table to generate.

    Returns:
        None. The function writes the generated tables to storage, in this case an Azure Storage Account.

    Raises:
        ValueError: If the 'table_name' input is not a name mentioned in the function, a ValueError is raised.

    Note:
        - The function uses the 'CustomDF' class to handle DataFrames.
        - The function writes the generated tables to storage, in this case an Azure Storage Account, using the 'write_table' method of the 'CustomDF' class.
        - If the fuction is run in Databricks it will not terminate the SparkSession. This is to ensure that the cluster can be used for other processes.
    """

    spark_generate = create_spark_session()

    if table_name == 'companies_europages_raw':

        companies_europages_landingzone = CustomDF(
            'companies_europages_landingzone', spark_generate)

        column_names = ["min_headcount", "max_headcount", "year_established"]

        companies_europages_landingzone.convert_data_types(
            column_names, IntegerType())

        companies_europages_landingzone.convert_data_types(
            ['verified_by_europages'], BooleanType())

        companies_europages_landingzone.convert_data_types(
            ['download_datetime'], DateType())

        companies_europages_landingzone.data = companies_europages_landingzone.data.filter(
            F.col('id') != 'na_00000004265865-001')

        companies_europages_raw = CustomDF(
            'companies_europages_raw', spark_generate, initial_df=companies_europages_landingzone.data)

        companies_europages_raw.write_table()

    if table_name == 'companies_companyinfo_raw':
        companies_companyinfo_landingzone = CustomDF(
            'companies_companyinfo_landingzone', spark_generate)
        
        companies_companyinfo_landingzone.data = companies_companyinfo_landingzone.data.withColumn("company_name", rename(F.col("Instellingsnaam"), F.col("Statutaire_naam")))

        col_rename_dict = {
            'Kamer_van_Koophandel_nummer_12-cijferig': 'kvk_number',
            'Bedrijfsomschrijving': 'description',
            'Vestigingsadres': 'address',
            'Vestigingsadres_postcode': 'postcode',
            'Vestigingsadres_plaats': 'company_city',
            'SBI-code_locatie': 'sbi_code',
            'SBI-code_locatie_Omschrijving': 'sbi_code_description',
        }

        companies_companyinfo_landingzone.rename_columns(rename_dict=col_rename_dict)
        companies_companyinfo_landingzone.data = companies_companyinfo_landingzone.data.select('kvk_number', 'company_name', 'description', 'address', 'postcode', 'company_city', 'sbi_code', 'sbi_code_description')

        companies_companyinfo_raw = CustomDF(
            'companies_companyinfo_raw', spark_generate, initial_df=companies_companyinfo_landingzone.data)

        companies_companyinfo_raw.write_table()

    elif table_name == 'country_raw':

        country_landingzone = CustomDF(
            'country_landingzone', spark_generate)

        country_raw = CustomDF(
            'country_raw', spark_generate, initial_df=country_landingzone.data)

        country_raw.write_table()

    elif table_name == 'sources_mapper_raw':

        sources_mapper_landingzone = CustomDF(
            'sources_mapper_landingzone', spark_generate)

        sources_mapper_raw = CustomDF(
            'sources_mapper_raw', spark_generate, initial_df=sources_mapper_landingzone.data)

        sources_mapper_raw.write_table()

    elif table_name == 'countries_mapper_raw':

        countries_mapper_landingzone = CustomDF(
            'countries_mapper_landingzone', spark_generate)

        countries_mapper_landingzone.data = countries_mapper_landingzone.data.withColumn('country_un',
                                                                                         F.when(countries_mapper_landingzone.data['country'] == 'Namibia', 'NA').otherwise(countries_mapper_landingzone.data['country_un']))

        countries_mapper_raw = CustomDF(
            'countries_mapper_raw', spark_generate, initial_df=countries_mapper_landingzone.data)

        countries_mapper_raw.write_table()

    elif table_name == 'geography_ecoinvent_mapper_raw':

        geography_mapper_landingzone = CustomDF(
            'geography_mapper_landingzone', spark_generate)

        column_names = ["priority", "input_priority"]

        geography_mapper_landingzone.convert_data_types(
            column_names, ByteType())

        geography_ecoinvent_mapper_raw = CustomDF(
            'geography_ecoinvent_mapper_raw', spark_generate, initial_df=geography_mapper_landingzone.data)

        geography_ecoinvent_mapper_raw.write_table()

    elif table_name == 'EP_tilt_sector_unmatched_mapper_raw':

        EP_tilt_sector_mapper_landingzone = CustomDF(
            'EP_tilt_sector_mapper_landingzone', spark_generate)

        EP_tilt_sector_unmatched_mapper_raw = CustomDF(
            'EP_tilt_sector_unmatched_mapper_raw', spark_generate, initial_df=EP_tilt_sector_mapper_landingzone.data)

        EP_tilt_sector_unmatched_mapper_raw.write_table()

    elif table_name == 'tilt_sector_isic_mapper_raw':

        tilt_isic_mapper_landingzone = CustomDF(
            'tilt_isic_mapper_2023-07-20_landingzone', spark_generate)

        tilt_sector_isic_mapper_raw = CustomDF(
            'tilt_sector_isic_mapper_raw', spark_generate, initial_df=tilt_isic_mapper_landingzone.data)

        tilt_sector_isic_mapper_raw.write_table()

    elif table_name == 'tilt_sector_scenario_mapper_raw':

        tilt_scenario_mapper_landingzone = CustomDF(
            'scenario_tilt_mapper_2023-07-20_landingzone', spark_generate)

        tilt_sector_scenario_mapper_raw = CustomDF(
            'tilt_sector_scenario_mapper_raw', spark_generate, initial_df=tilt_scenario_mapper_landingzone.data)

        tilt_sector_scenario_mapper_raw.write_table()

    elif table_name == 'scenario_targets_IPR_raw':

        scenario_targets_IPR_NEW_landingzone = CustomDF(
            'scenario_targets_IPR_NEW_landingzone', spark_generate)

        scenario_targets_IPR_NEW_landingzone.convert_data_types(
            ['Year'], ShortType())

        scenario_targets_IPR_NEW_landingzone.convert_data_types(
            ['Value'], DoubleType())
# Removing reductions in this step, because the column is incorrect and not used anymore
        scenario_targets_IPR_raw = CustomDF(
            'scenario_targets_IPR_raw', spark_generate, initial_df=scenario_targets_IPR_NEW_landingzone.data.drop('Reductions'))

        scenario_targets_IPR_raw.write_table()

    elif table_name == 'scenario_targets_WEO_raw':

        scenario_targets_WEO_NEW_landingzone = CustomDF(
            'scenario_targets_WEO_NEW_landingzone', spark_generate)

        scenario_targets_WEO_NEW_landingzone.convert_data_types(
            ['YEAR'], ShortType())

        scenario_targets_WEO_NEW_landingzone.convert_data_types(
            ['VALUE'], DoubleType())
# Removing reductions in this step, because the column is incorrect and not used anymore
        scenario_targets_WEO_raw = CustomDF(
            'scenario_targets_WEO_raw', spark_generate, initial_df=scenario_targets_WEO_NEW_landingzone.data.drop('REDUCTIONS'))

        scenario_targets_WEO_raw.write_table()

    # elif table_name == 'geography_mapper_raw':

    #     geography_mapper_landingzone = CustomDF(
    #         'geography_mapper_landingzone', spark_generate)

    #     geography_mapper_raw = CustomDF(
    #         'geography_mapper_raw', spark_generate, initial_df=geography_mapper_landingzone.data)

    #     geography_mapper_raw.write_table()

    elif table_name == 'geographies_raw':

        geographies_landingzone = CustomDF(
            'geographies_landingzone', spark_generate)

        # Filter out the empty values in the ID column, as empty records are read in from the source data.
        geographies_landingzone.data = geographies_landingzone.data.filter(
            ~F.isnull(F.col('ID')))

        geographies_raw = CustomDF(
            'geographies_raw', spark_generate, initial_df=geographies_landingzone.data)

        geographies_raw.write_table()

    # elif table_name == 'geographies_transform':

    #     geographies_raw = CustomDF('geographies_raw', spark_generate)

    #     geographies_transform = CustomDF('geographies_transform', spark_generate, initial_df=geographies_raw.data.select(
    #         'ID', 'Name', 'Shortname', 'Geographical Classification'))

    #     geographies_related_df = geographies_raw.data.select(
    #         'Shortname', 'Contained and Overlapping Geographies')

    #     geographies_related_df = geographies_related_df.withColumn(
    #         'Shortname_related', F.explode(F.split('Contained and Overlapping Geographies', ';')))

    #     geographies_related = CustomDF('geographies_related', spark_generate, initial_df=geographies_related_df.select(
    #         'Shortname', 'Shortname_related'))

    #     geographies_transform.write_table()
    #     geographies_related.write_table()

    elif table_name == 'undefined_ao_raw':

        undefined_ao_landingzone = CustomDF(
            'undefined_ao_landingzone', spark_generate)

        undefined_ao_landingzone.data = undefined_ao_landingzone.data.distinct()

        undefined_ao_raw = CustomDF(
            'undefined_ao_raw', spark_generate, initial_df=undefined_ao_landingzone.data)
        undefined_ao_raw.write_table()

    elif table_name == 'cut_off_ao_raw':

        cut_off_ao_landingzone = CustomDF(
            'cut_off_ao_landingzone', spark_generate)

        cut_off_ao_raw = CustomDF(
            'cut_off_ao_raw', spark_generate, initial_df=cut_off_ao_landingzone.data)
        cut_off_ao_raw.write_table()

    elif table_name == 'en15804_ao_raw':

        en15804_ao_landingzone = CustomDF(
            'en15804_ao_landingzone', spark_generate)

        en15804_ao_raw = CustomDF(
            'en15804_ao_raw', spark_generate, initial_df=en15804_ao_landingzone.data)
        en15804_ao_raw.write_table()

    elif table_name == 'consequential_ao_raw':

        consequential_ao_landingzone = CustomDF(
            'consequential_ao_landingzone', spark_generate)

        consequential_ao_raw = CustomDF(
            'consequential_ao_raw', spark_generate, initial_df=consequential_ao_landingzone.data)
        consequential_ao_raw.write_table()

    elif table_name == 'lcia_methods_raw':

        lcia_methods_landingzone = CustomDF(
            'lcia_methods_landingzone', spark_generate)

        lcia_methods_raw = CustomDF(
            'lcia_methods_raw', spark_generate, initial_df=lcia_methods_landingzone.data)
        lcia_methods_raw.write_table()

    elif table_name == 'impact_categories_raw':

        impact_categories_landingzone = CustomDF(
            'impact_categories_landingzone', spark_generate)

        impact_categories_raw = CustomDF(
            'impact_categories_raw', spark_generate, initial_df=impact_categories_landingzone.data)
        impact_categories_raw.write_table()

    elif table_name == 'intermediate_exchanges_raw':

        intermediate_exchanges_landingzone = CustomDF(
            'intermediate_exchanges_landingzone', spark_generate)

        intermediate_exchanges_raw = CustomDF(
            'intermediate_exchanges_raw', spark_generate, initial_df=intermediate_exchanges_landingzone.data)
        intermediate_exchanges_raw.write_table()

    elif table_name == 'elementary_exchanges_raw':

        elementary_exchanges_landingzone = CustomDF(
            'elementary_exchanges_landingzone', spark_generate)

        elementary_exchanges_raw = CustomDF(
            'elementary_exchanges_raw', spark_generate, initial_df=elementary_exchanges_landingzone.data)
        elementary_exchanges_raw.write_table()

    elif table_name == 'ecoinvent_co2_raw':

        ecoinvent_co2_landingzone = CustomDF(
            'cut-off_cumulative_LCIA_v3.9.1_landingzone', spark_generate)

        ecoinvent_co2_landingzone.data = ecoinvent_co2_landingzone.data.withColumn('IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq', F.regexp_replace(
            'IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq', '[,]', '.'))

        ecoinvent_co2_landingzone.convert_data_types(
            ['IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq'], DecimalType(15, 10))

        ecoinvent_co2_raw = CustomDF(
            'ecoinvent_co2_raw', spark_generate, initial_df=ecoinvent_co2_landingzone.data)
        ecoinvent_co2_raw.write_table()

    elif table_name == 'ecoinvent_input_data_raw':

        ecoinvent_input_data_relevant_columns_landingzone = CustomDF(
            'ecoinvent_input_data_relevant_columns_landingzone', spark_generate)

        ecoinvent_input_data_relevant_columns_landingzone.convert_data_types(
            ['exchange_amount'], DecimalType(25, 10))

        ecoinvent_input_data_raw = CustomDF(
            'ecoinvent_input_data_raw', spark_generate, initial_df=ecoinvent_input_data_relevant_columns_landingzone.data.distinct())
        ecoinvent_input_data_raw.write_table()

    elif table_name == 'ep_ei_matcher_raw':

        ep_ei_matcher_landingzone = CustomDF(
            'ep_ei_matcher_landingzone', spark_generate)

        ep_ei_matcher_landingzone.convert_data_types(
            ['multi_match'], BooleanType())

        ep_ei_matcher_raw = CustomDF(
            'ep_ei_matcher_raw', spark_generate, initial_df=ep_ei_matcher_landingzone.data)

        ep_ei_matcher_raw.write_table()

    elif table_name == 'mapper_ep_ei_raw':

        mapper_ep_ei_landingzone = CustomDF(
            'mapper_ep_ei_landingzone', spark_generate)

        mapper_ep_ei_landingzone.convert_data_types(
            ['multi_match'], BooleanType())

        mapper_ep_ei_raw = CustomDF(
            'mapper_ep_ei_raw', spark_generate, initial_df=mapper_ep_ei_landingzone.data)

        mapper_ep_ei_raw.write_table()

    elif table_name == 'emission_profile_company_raw':

        emission_profile_company_landingzone = CustomDF(
            'emission_profile_company_landingzone', spark_generate)

        cast_to_float = ['emission_profile_share']

        emission_profile_company_landingzone.convert_data_types(
            cast_to_float, DoubleType())

        emission_profile_company_landingzone.data = emission_profile_company_landingzone.data.drop(
            'batch')

        emission_profile_company_landingzone.data = emission_profile_company_landingzone.data.distinct()

        emission_profile_company_raw = CustomDF(
            'emission_profile_company_raw', spark_generate, initial_df=emission_profile_company_landingzone.data)

        emission_profile_company_raw.write_table()

    elif table_name == 'emission_profile_product_raw':

        emission_profile_product_landingzone = CustomDF(
            'emission_profile_product_landingzone', spark_generate)

        emission_profile_product_landingzone.data = emission_profile_product_landingzone.data.withColumn('multi_match',
                                                                                                         F.when(
                                                                                                             F.col('multi_match') == "TRUE", F.lit(True))
                                                                                                         .when(F.col('multi_match') == "FALSE", F.lit(False))
                                                                                                         .otherwise(F.lit(None)))

        emission_profile_product_landingzone.data = emission_profile_product_landingzone.data.drop(
            'batch')

        emission_profile_product_landingzone.data = emission_profile_product_landingzone.data.distinct()

        emission_profile_product_raw = CustomDF(
            'emission_profile_product_raw', spark_generate, initial_df=emission_profile_product_landingzone.data)

        emission_profile_product_raw.write_table()

    elif table_name == 'emission_upstream_profile_company_raw':

        emission_upstream_profile_company_landingzone = CustomDF(
            'emission_upstream_profile_company_landingzone', spark_generate)

        cast_to_float = ['emission_usptream_profile_share']

        emission_upstream_profile_company_landingzone.convert_data_types(
            cast_to_float, DoubleType())

        emission_upstream_profile_company_landingzone.data = emission_upstream_profile_company_landingzone.data.drop(
            'batch')

        col_order = emission_upstream_profile_company_landingzone.data.columns

        for num, val in enumerate(col_order):
            if val == 'emission_usptream_profile':
                col_order[num] = 'emission_upstream_profile'
            if val == 'emission_usptream_profile_share':
                col_order[num] = 'emission_upstream_profile_share'

        emission_upstream_profile_company_landingzone.data = emission_upstream_profile_company_landingzone.data.withColumn(
            'emission_upstream_profile', F.col('emission_usptream_profile')).drop('emission_usptream_profile')
        emission_upstream_profile_company_landingzone.data = emission_upstream_profile_company_landingzone.data.withColumn(
            'emission_upstream_profile_share', F.col('emission_usptream_profile_share')).drop('emission_usptream_profile_share').select(col_order)

        emission_upstream_profile_company_landingzone.data = emission_upstream_profile_company_landingzone.data.distinct()

        emission_upstream_profile_company_raw = CustomDF(
            'emission_upstream_profile_company_raw', spark_generate, initial_df=emission_upstream_profile_company_landingzone.data)

        emission_upstream_profile_company_raw.write_table()

    elif table_name == 'emission_upstream_profile_product_raw':

        emission_upstream_profile_product_landingzone = CustomDF(
            'emission_upstream_profile_product_landingzone', spark_generate)

        emission_upstream_profile_product_landingzone.data = emission_upstream_profile_product_landingzone.data.withColumn('multi_match',
                                                                                                                           F.when(
                                                                                                                               F.col('multi_match') == "TRUE", F.lit(True))
                                                                                                                           .when(F.col('multi_match') == "FALSE", F.lit(False))
                                                                                                                           .otherwise(F.lit(None)))

        emission_upstream_profile_product_landingzone.data = emission_upstream_profile_product_landingzone.data.drop(
            'batch')

        col_order = emission_upstream_profile_product_landingzone.data.columns

        for num, val in enumerate(col_order):
            if val == 'emission_usptream_profile':
                col_order[num] = 'emission_upstream_profile'
            if val == 'emission_usptream_profile_share':
                col_order[num] = 'emission_upstream_profile_share'

        emission_upstream_profile_product_landingzone.data = emission_upstream_profile_product_landingzone.data.withColumn(
            'emission_upstream_profile', F.col('emission_usptream_profile')).drop('emission_usptream_profile').select(col_order)

        emission_upstream_profile_product_landingzone.data = emission_upstream_profile_product_landingzone.data.distinct()

        emission_upstream_profile_product_raw = CustomDF(
            'emission_upstream_profile_product_raw', spark_generate, initial_df=emission_upstream_profile_product_landingzone.data)

        emission_upstream_profile_product_raw.write_table()

    elif table_name == 'sector_profile_company_raw':

        sector_profile_company_landingzone = CustomDF(
            'sector_profile_company_landingzone', spark_generate)

        cast_to_float = ['sector_profile_share']

        sector_profile_company_landingzone.convert_data_types(
            cast_to_float, DoubleType())

        sector_profile_company_landingzone.convert_data_types(
            ['year'], IntegerType())

        sector_profile_company_landingzone.data = sector_profile_company_landingzone.data.drop(
            'batch')

        sector_profile_company_landingzone.data = sector_profile_company_landingzone.data.distinct()

        sector_profile_company_raw = CustomDF(
            'sector_profile_company_raw', spark_generate, initial_df=sector_profile_company_landingzone.data)

        sector_profile_company_raw.write_table()

    elif table_name == 'sector_profile_product_raw':

        sector_profile_product_landingzone = CustomDF(
            'sector_profile_product_landingzone', spark_generate)

        sector_profile_product_landingzone.convert_data_types(
            ['year'], IntegerType())
        sector_profile_product_landingzone.data = sector_profile_product_landingzone.data.withColumn('multi_match',
                                                                                                     F.when(
                                                                                                         F.col('multi_match') == "TRUE", F.lit(True))
                                                                                                     .when(F.col('multi_match') == "FALSE", F.lit(False))
                                                                                                     .otherwise(F.lit(None)))

        sector_profile_product_landingzone.data = sector_profile_product_landingzone.data.drop(
            'batch')

        sector_profile_product_landingzone.data = sector_profile_product_landingzone.data.distinct()

        sector_profile_product_raw = CustomDF(
            'sector_profile_product_raw', spark_generate, initial_df=sector_profile_product_landingzone.data)

        sector_profile_product_raw.write_table()

    elif table_name == 'sector_upstream_profile_company_raw':

        sector_upstream_profile_company_landingzone = CustomDF(
            'sector_upstream_profile_company_landingzone', spark_generate)

        cast_to_float = ['sector_profile_upstream_share']
        sector_upstream_profile_company_landingzone.convert_data_types(
            cast_to_float, DoubleType())
        sector_upstream_profile_company_landingzone.convert_data_types(
            ['year'], IntegerType())

        sector_upstream_profile_company_landingzone.data = sector_upstream_profile_company_landingzone.data.drop(
            'batch')

        sector_upstream_profile_company_landingzone.data = sector_upstream_profile_company_landingzone.data.distinct()

        sector_upstream_profile_company_raw = CustomDF(
            'sector_upstream_profile_company_raw', spark_generate, initial_df=sector_upstream_profile_company_landingzone.data)
        sector_upstream_profile_company_raw.write_table()

    elif table_name == 'sector_upstream_profile_product_raw':

        sector_upstream_profile_product_landingzone = CustomDF(
            'sector_upstream_profile_product_landingzone', spark_generate)

        sector_upstream_profile_product_landingzone.convert_data_types(
            ['year'], IntegerType())
        sector_upstream_profile_product_landingzone.data = sector_upstream_profile_product_landingzone.data.withColumn('multi_match',
                                                                                                                       F.when(
                                                                                                                           F.col('multi_match') == "TRUE", F.lit(True))
                                                                                                                       .when(F.col('multi_match') == "FALSE", F.lit(False))
                                                                                                                       .otherwise(F.lit(None)))

        sector_upstream_profile_product_landingzone.data = sector_upstream_profile_product_landingzone.data.drop(
            'batch')

        sector_upstream_profile_product_landingzone.data = sector_upstream_profile_product_landingzone.data.distinct()

        sector_upstream_profile_product_raw = CustomDF(
            'sector_upstream_profile_product_raw', spark_generate, initial_df=sector_upstream_profile_product_landingzone.data)
        sector_upstream_profile_product_raw.write_table()

    elif table_name == 'isic_mapper_raw':

        isic_4_digit_codes_landingzone = CustomDF(
            'isic_4_digit_codes_landingzone', spark_generate)

        isic_mapper_raw = CustomDF(
            'isic_mapper_raw', spark_generate, initial_df=isic_4_digit_codes_landingzone.data)
        isic_mapper_raw.write_table()

    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()
