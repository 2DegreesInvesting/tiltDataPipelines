import os
import pyspark.sql.functions as F
from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session
from functions.dataframe_helpers import create_sha_values
from pyspark.sql.functions import col, substring


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

    # Companies data

    if table_name == 'companies_datamodel':

        companies_europages_raw = CustomDF(
            'companies_europages_raw', spark_generate)

        countries_mapper_raw = CustomDF('countries_mapper_raw', spark_generate)

        # Define a dictionary to map old column names to new column names
        rename_dict = {"id": "company_id"}

        companies_europages_raw.rename_columns(rename_dict)

        # Fill in source information for Europages
        value_to_fill = 'source_1_ep'
        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            "source_id", F.lit(value_to_fill))

        # Capitalize the first letter of the values in the "country" column to match with countries_mapperpoductr
        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            "country", F.initcap("country"))

        joined_companies_countries_mapper = companies_europages_raw.custom_join(
            countries_mapper_raw, "country")

        companies_raw_final = joined_companies_countries_mapper.custom_drop(
            ["country"])

        companies_raw_final = companies_raw_final.custom_select(
            ['company_id', 'country_un', 'source_id', 'company_name', 'address', 'company_city', 'postcode']).custom_distinct()

        companies_datamodel = CustomDF(
            'companies_datamodel', spark_generate, initial_df=companies_raw_final.data)

        companies_datamodel.write_table()

    elif table_name == 'products_datamodel':

        companies_europages_raw = CustomDF(
            'companies_europages_raw', spark_generate)

        companies_europages_raw.data = companies_europages_raw.data.withColumn("product_name", F.explode(F.split("products_and_services", "\|")))\
            .drop("products_and_services")

        companies_europages_raw = companies_europages_raw.custom_select(
            ["product_name"])

        # create product_id
        # TODO: I dont think we should take the from date in here, since that might change the product id if something arbitrary in the other columns changes
        sha_columns = [F.col(col_name) for col_name in companies_europages_raw.data.columns if col_name not in [
            'tiltRecordID', 'to_date', 'map_companies_europages_raw']]

        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            'product_id', F.sha2(F.concat_ws('|', *sha_columns), 256))

        companies_europages_raw = companies_europages_raw.custom_select(
            ['product_id', 'product_name']).custom_distinct()

        products_datamodel = CustomDF(
            'products_datamodel', spark_generate, initial_df=companies_europages_raw.data)

        products_datamodel.write_table()

    elif table_name == 'companies_products_datamodel':

        companies_europages_raw = CustomDF(
            'companies_europages_raw', spark_generate)

        products_datamodel = CustomDF('products_datamodel', spark_generate)

        rename_dict = {"id": "company_id"}

        companies_europages_raw.rename_columns(rename_dict)

        companies_europages_raw.data = companies_europages_raw.data.withColumn("product_name", F.explode(F.split("products_and_services", "\|")))\
            .drop("products_and_services")
        # TODO: I dont know if we have to reuse the product id created in the products datamodel table, because we can just repoduce it based on the same information
        companies_joined_product_id = companies_europages_raw.custom_join(
            products_datamodel, custom_on="product_name")

        companies_joined_without_product_name = companies_joined_product_id.custom_drop(
            ["product_name"])

        companies_joined_without_product_name = companies_joined_without_product_name.custom_select(
            ["company_id", "product_id"]).custom_distinct()

        companies_products_datamodel = CustomDF(
            'companies_products_datamodel', spark_generate, initial_df=companies_joined_without_product_name.data)
        companies_products_datamodel.write_table()

    # Ecoinvent data

    elif table_name == 'intermediate_exchanges_datamodel':

        intermediate_exchanges_raw = CustomDF(
            'intermediate_exchanges_raw', spark_generate)

        rename_dict = {"ID": "exchange_id",
                       "Name": "exchange_name", "Unit_Name": "unit_name"}

        intermediate_exchanges_raw.rename_columns(rename_dict)

        intermediate_exchanges_raw = intermediate_exchanges_raw.custom_select(
            ['exchange_id', 'exchange_name', 'unit_name'])

        intermediate_exchanges_datamodel = CustomDF(
            'intermediate_exchanges_datamodel', spark_generate, initial_df=intermediate_exchanges_raw.data)

        intermediate_exchanges_datamodel.write_table()

    elif table_name == 'ecoinvent_co2_datamodel':

        ecoinvent_co2_raw = CustomDF('ecoinvent_co2_raw', spark_generate)

        rename_dict = {"Activity_UUID_Product_UUID": "activity_uuid_product_uuid",
                       "IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq": "co2_footprint"}

        ecoinvent_co2_raw.rename_columns(rename_dict)

        ecoinvent_co2_raw.custom_select(
            ['activity_uuid_product_uuid', 'co2_footprint'])

        ecoinvent_co2_datamodel = CustomDF(
            'ecoinvent_co2_datamodel', spark_generate, initial_df=ecoinvent_co2_raw.data)

        ecoinvent_co2_datamodel.write_table()

    elif table_name == 'ecoinvent_cut_off_datamodel':

        cut_off_ao_raw = CustomDF('cut_off_ao_raw', spark_generate)

        rename_dict = {"Activity_UUID_&_Product_UUID": "activity_uuid_product_uuid",
                       "Activity_UUID": "activity_uuid",
                       "Product_UUID": "product_uuid"}

        cut_off_ao_raw.rename_columns(rename_dict)

        cut_off_ao_raw = cut_off_ao_raw.custom_select(
            ['activity_uuid_product_uuid', 'activity_uuid', 'product_uuid'])

        ecoinvent_cut_off_datamodel = CustomDF(
            'ecoinvent_cut_off_datamodel', spark_generate, initial_df=cut_off_ao_raw.data)

        ecoinvent_cut_off_datamodel.write_table()

    elif table_name == 'ecoinvent_product_datamodel':

        cut_off_ao_raw = CustomDF('cut_off_ao_raw', spark_generate)

        rename_dict = {"Product_UUID": "product_uuid",
                       "Reference_Product_Name": "reference_product_name", 'Unit': 'unit'}

        cut_off_ao_raw.rename_columns(rename_dict)

        cut_off_ao_raw = cut_off_ao_raw.custom_select(
            ['product_uuid', 'reference_product_name', 'unit']).custom_distinct()

        ecoinvent_product_datamodel = CustomDF(
            'ecoinvent_product_datamodel', spark_generate, initial_df=cut_off_ao_raw.data)

        ecoinvent_product_datamodel.write_table()

    elif table_name == 'ecoinvent_activity_datamodel':

        cut_off_ao_raw = CustomDF('cut_off_ao_raw', spark_generate)

        rename_dict = {"Activity_UUID": "activity_uuid",
                       "Activity_Name": "activity_name", 'Geography': 'geography'}

        cut_off_ao_raw.rename_columns(rename_dict)

        cut_off_ao_raw.data = cut_off_ao_raw.data.withColumn(
            "isic_4digit", substring(col("ISIC_Classification"), 1, 4))

        cut_off_ao_raw = cut_off_ao_raw.custom_select(
            ['activity_uuid', 'activity_name', 'geography', 'isic_4digit']).custom_distinct()

        ecoinvent_activity_datamodel = CustomDF(
            'ecoinvent_activity_datamodel', spark_generate, initial_df=cut_off_ao_raw.data)

        ecoinvent_activity_datamodel.write_table()

    elif table_name == 'ecoinvent_input_data_datamodel':

        ecoinvent_input_data_raw = CustomDF(
            'ecoinvent_input_data_raw', spark_generate)

        ecoinvent_input_data_raw = ecoinvent_input_data_raw.custom_select(['activityId', 'activityName', 'geography', 'reference_product', 'group', 'exchange_name',
                                                                           'activityLinkId', 'activityLink_activityName', 'activityLink_geography', 'exchange_unitName']).custom_distinct()

        ecoinvent_input_data_datamodel = CustomDF(
            'ecoinvent_input_data_datamodel', spark_generate, initial_df=ecoinvent_input_data_raw.data)

        ecoinvent_input_data_datamodel.write_table()

    # Mappers data

    elif table_name == 'sources_mapper_datamodel':

        sources_mapper_raw = CustomDF('sources_mapper_raw', spark_generate)

        sources_mapper_datamodel = CustomDF(
            'sources_mapper_datamodel', spark_generate, initial_df=sources_mapper_raw.data)

        sources_mapper_datamodel.write_table()

    elif table_name == 'countries_mapper_datamodel':

        countries_mapper_raw = CustomDF('countries_mapper_raw', spark_generate)

        countries_mapper_raw.data = countries_mapper_raw.data.withColumn('country_un',
                                                                         F.when(countries_mapper_raw.data['country'] == 'Namibia', 'NA').otherwise(countries_mapper_raw.data['country_un']))

        countries_mapper_datamodel = CustomDF(
            'countries_mapper_datamodel', spark_generate, initial_df=countries_mapper_raw.data)

        countries_mapper_datamodel.write_table()

    elif table_name == 'geography_ecoinvent_mapper_datamodel':

        geography_ecoinvent_mapper_raw = CustomDF(
            'geography_ecoinvent_mapper_raw', spark_generate)

        country_raw = CustomDF('country_raw', spark_generate)

        countries_mapper_raw = CustomDF('countries_mapper_raw', spark_generate)

        rename_dict = {"lca_geo": "ecoinvent_geography"}

        geography_ecoinvent_mapper_raw.rename_columns(rename_dict)

        geography_country_name = geography_ecoinvent_mapper_raw.custom_join(
            country_raw, custom_on="country_id")

        geography_country_name.data = geography_country_name.data.withColumn(
            "country", F.initcap(col("country")))

        geography_country_un = geography_country_name.custom_join(
            countries_mapper_raw, "country")

        geography_country_un = geography_country_un.custom_drop(
            ["country_id", "country"])

        geography_country_un = geography_country_un.custom_select(
            ['geography_id', 'country_un', 'ecoinvent_geography', 'priority', 'input_priority'])

        geography_ecoinvent_mapper_datamodel = CustomDF(
            'geography_ecoinvent_mapper_datamodel', spark_generate, initial_df=geography_country_un.data)

        geography_ecoinvent_mapper_datamodel.write_table()

    elif table_name == 'EP_tilt_sector_unmatched_mapper_datamodel':

        EP_tilt_sector_unmatched_mapper_raw = CustomDF(
            'EP_tilt_sector_unmatched_mapper_raw', spark_generate)

        EP_tilt_sector_unmatched_mapper_datamodel = CustomDF(
            'EP_tilt_sector_unmatched_mapper_datamodel', spark_generate, initial_df=EP_tilt_sector_unmatched_mapper_raw.data)

        EP_tilt_sector_unmatched_mapper_datamodel.write_table()

    elif table_name == 'tilt_sector_isic_mapper_datamodel':

        tilt_sector_isic_mapper_raw = CustomDF(
            'tilt_sector_isic_mapper_raw', spark_generate)

        tilt_sector_isic_mapper_raw = tilt_sector_isic_mapper_raw.custom_select(
            ['tilt_sector', 'tilt_subsector', 'isic_4digit', 'isic_section'])

        tilt_sector_isic_mapper_datamodel = CustomDF(
            'tilt_sector_isic_mapper_datamodel', spark_generate, initial_df=tilt_sector_isic_mapper_raw.data)

        tilt_sector_isic_mapper_datamodel.write_table()

    elif table_name == 'tilt_sector_scenario_mapper_datamodel':

        tilt_sector_scenario_weo_mapper_raw = CustomDF(
            'tilt_sector_scenario_mapper_raw', spark_generate)

        tilt_sector_scenario_ipr_mapper_raw = CustomDF(
            'tilt_sector_scenario_mapper_raw', spark_generate)

        tilt_sector_scenario_weo_mapper_raw.data = tilt_sector_scenario_weo_mapper_raw.custom_select(
            ["tilt_sector", "tilt_subsector", "weo_product", "weo_flow"]).data.withColumn("scenario_type", F.lit("weo"))

        rename_ipr_dict = {'ipr_sector': 'scenario_sector',
                           'ipr_subsector': 'scenario_subsector'}
        rename_weo_dict = {"weo_product": "scenario_sector",
                           "weo_flow": "scenario_subsector"}

        tilt_sector_scenario_weo_mapper_raw.rename_columns(rename_weo_dict)

        tilt_sector_scenario_ipr_mapper_raw.data = tilt_sector_scenario_ipr_mapper_raw.custom_select(
            ["tilt_sector", "tilt_subsector", "ipr_sector", "ipr_subsector"]).data.withColumn("scenario_type", F.lit("ipr"))

        tilt_sector_scenario_ipr_mapper_raw.rename_columns(rename_ipr_dict)

        both_scenarios = tilt_sector_scenario_weo_mapper_raw.custom_union(
            tilt_sector_scenario_ipr_mapper_raw)

        # Filtering null and non_match
        both_scenarios.data = both_scenarios.data.filter(
            (col('tilt_sector') != "no_match") & (col('tilt_sector').isNotNull()))

        both_scenarios = both_scenarios.custom_select(
            ['tilt_sector', 'tilt_subsector', 'scenario_type', 'scenario_sector', 'scenario_subsector'])

        tilt_sector_scenario_mapper_datamodel = CustomDF(
            'tilt_sector_scenario_mapper_datamodel', spark_generate, initial_df=both_scenarios.data)

        tilt_sector_scenario_mapper_datamodel.write_table()

    # Scenario data

    elif table_name == 'scenario_targets_IPR_datamodel':

        scenario_targets_IPR_raw = CustomDF(
            'scenario_targets_IPR_raw', spark_generate)

        scenario_targets_IPR_raw = scenario_targets_IPR_raw.custom_select(
            ['Scenario', 'Region', 'Sector', 'Sub_Sector', 'Year', 'Value'])

        rename_dict = {"Scenario": "scenario", "Region": "region", "Sector": "ipr_sector", "Sub_Sector": "ipr_subsector",
                       "Year": "year", "Value": "value"}

        scenario_targets_IPR_raw.rename_columns(rename_dict)

        # Select all columns that are needed for the creation of a record ID
        sha_columns = [F.col(col_name) for col_name in scenario_targets_IPR_raw.data.columns if col_name not in [
            'tiltRecordID', 'to_date', 'map_scenario_targets_IPR_raw']]

        # Create the SHA256 record ID by concatenating all relevant columns
        scenario_targets_IPR_raw.data = scenario_targets_IPR_raw.data.withColumn(
            'scenario_targets_ipr_id', F.sha2(F.concat_ws('|', *sha_columns), 256))

        scenario_targets_IPR_raw = scenario_targets_IPR_raw.custom_select(
            ['scenario_targets_ipr_id', 'scenario', 'region', 'ipr_sector', 'ipr_subsector', 'year', 'value']).custom_distinct()

        scenario_targets_IPR_datamodel = CustomDF(
            'scenario_targets_IPR_datamodel', spark_generate, initial_df=scenario_targets_IPR_raw.data)

        scenario_targets_IPR_datamodel.write_table()

    elif table_name == 'scenario_targets_WEO_datamodel':

        scenario_targets_WEO_raw = CustomDF(
            'scenario_targets_WEO_raw', spark_generate)

        scenario_targets_WEO_raw = scenario_targets_WEO_raw.custom_select(
            ['SCENARIO', 'REGION', 'PRODUCT', 'FLOW', 'YEAR', 'VALUE'])

        rename_dict = {"SCENARIO": "scenario", "REGION": "region", "PRODUCT": "weo_sector",
                       "FLOW": "weo_subsector", "YEAR": "year", "VALUE": "value"}

        scenario_targets_WEO_raw.rename_columns(rename_dict)

        # Select all columns that are needed for the creation of a record ID
        sha_columns = [F.col(col_name) for col_name in scenario_targets_WEO_raw.data.columns if col_name not in [
            'tiltRecordID', 'to_date', 'map_scenario_targets_WEO_raw']]

        # Create the SHA256 record ID by concatenating all relevant columns
        scenario_targets_WEO_raw.data = scenario_targets_WEO_raw.data.withColumn(
            'scenario_targets_weo_id', F.sha2(F.concat_ws('|', *sha_columns), 256))

        scenario_targets_WEO_raw = scenario_targets_WEO_raw.custom_select(
            ['scenario_targets_weo_id', 'scenario', 'region', 'weo_sector', 'weo_subsector', 'year', 'value']).custom_distinct()

        scenario_targets_WEO_datamodel = CustomDF(
            'scenario_targets_WEO_datamodel', spark_generate, initial_df=scenario_targets_WEO_raw.data)

        scenario_targets_WEO_datamodel.write_table()

    elif table_name == 'isic_mapper_datamodel':

        isic_4_digit_codes_landingzone = CustomDF(
            'isic_mapper_raw', spark_generate)

        isic_4_digit_codes_landingzone = isic_4_digit_codes_landingzone.custom_select(
            ['Code', 'ISIC_Rev_4_label'])

        rename_dict = {'Code': 'isic_4digit',
                       'ISIC_Rev_4_label': 'isic_4digit_name'}

        isic_4_digit_codes_landingzone.rename_columns(rename_dict)

        isic_mapper_datamodel = CustomDF(
            'isic_mapper_datamodel', spark_generate, initial_df=isic_4_digit_codes_landingzone.data)
        isic_mapper_datamodel.write_table()

    elif table_name == 'tiltLedger_datamodel':

        tiltLedger_raw = CustomDF('tiltLedger_raw', spark_generate)

        rename_dict = {'CPC21code': 'CPC_Code',
                       'CPC21title': 'CPC_Name',
                       'ISIC4code': 'ISIC_4digit',
                       'Description': 'ISIC_Name'}

        tiltLedger_raw.rename_columns(rename_dict)

        final_columns = ['CPC_Code', 'CPC_Name', 'ISIC_4digit',
                         'ISIC_Name', 'Activity_Type', 'Geography']

        tiltLedger_raw.custom_select(final_columns)

        tiltLedger_datamodel = CustomDF(
            'tiltLedger_datamodel', spark_generate, initial_df=tiltLedger_raw.data)
        tiltLedger_datamodel.write_table()

    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()
