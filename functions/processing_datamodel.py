import os
import pyspark.sql.functions as F
from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session
from functions.dataframe_helpers import (
    format_postcode,
    keep_one_name,
)
from pyspark.sql.functions import col, substring, lpad, when, length
from pyspark.sql.types import DoubleType
from Levenshtein import jaro_winkler


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

    if table_name == "companies_datamodel":

        # process EP data

        companies_europages_raw = CustomDF("companies_europages_raw", spark_generate)

        # rename columns per schema
        rename_dict = {
            "id": "europages_company_id",
            "information": "company_description",
        }
        companies_europages_raw.rename_columns(rename_dict)

        # EP: select only the necessary columns
        companies_europages_raw = companies_europages_raw.custom_select(
            [
                "europages_company_id",
                "company_name",
                "company_description",
                "address",
                "postcode",
                "company_city",
                "country",
            ]
        )

        # process CI data
        companies_companyinfo_raw = CustomDF(
            "companies_companyinfo_raw", spark_generate
        )

        # CI: rename columns per schema
        rename_dict = {
            "Kamer_van_Koophandel_nummer_12_cijferig": "companyinfo_company_id",
            "Bedrijfsomschrijving": "company_description",
            "Vestigingsadres": "address",
            "Vestigingsadres_plaats": "company_city",
            "Vestigingsadres_postcode": "postcode",
        }
        companies_companyinfo_raw.rename_columns(rename_dict)

        # CI: unify company name
        companies_companyinfo_raw.data = companies_companyinfo_raw.data.withColumn(
            "company_name",
            keep_one_name(F.col("Instellingsnaam"), F.col("Statutaire_naam")),
        )

        # CI: select only the necessary columns
        companies_companyinfo_raw = companies_companyinfo_raw.custom_select(
            [
                "companyinfo_company_id",
                "company_name",
                "company_description",
                "address",
                "postcode",
                "company_city",
            ]
        )

        # # CI: add country colum
        companies_companyinfo_raw.data = companies_companyinfo_raw.data.withColumn(
            "country", F.lit("netherlands")
        )

        # process source_id

        ep_only = "source_1_ep"
        ci_only = "source_3_ci"
        ep_ci = "source_4_ep_ci"

        companies_match_result_datamodel = CustomDF(
            "companies_match_result_datamodel", spark_generate
        )

        # CMR: matched companies
        companies_match_result_datamodel.data = (
            companies_match_result_datamodel.data.withColumn("source", F.lit(ep_ci))
        )
        match_source = companies_match_result_datamodel.custom_select(
            ["europages_company_id", "companyinfo_company_id", "source"]
        )

        # EP: get companies
        companies_europages = companies_europages_raw.custom_select(
            ["europages_company_id"]
        )

        # Add other EP companies to the table
        europages_matched = companies_europages.custom_join(
            match_source, custom_on="europages_company_id", custom_how="left_outer"
        )

        # CI: get companies
        companies_companyinfo = companies_companyinfo_raw.custom_select(
            ["companyinfo_company_id"]
        )

        both_matched = companies_companyinfo.custom_join(
            europages_matched,
            custom_on="companyinfo_company_id",
            custom_how="full_outer",
        )

        both_matched = both_matched.custom_distinct()

        both_matched.data = both_matched.data.withColumn(
            "source_id",
            F.when(col("source").isNotNull(), F.lit(ep_ci)).otherwise(
                F.when(
                    col("europages_company_id").isNotNull(), F.lit(ep_only)
                ).otherwise(F.lit(ci_only))
            ),
        )

        both_matched.data = both_matched.data.withColumn(
            "company_id",
            F.when(
                col("source_id").isin([ep_ci, ci_only]), col("companyinfo_company_id")
            ).otherwise(col("europages_company_id")),
        )

        companies = both_matched.custom_drop(
            ["companyinfo_company_id", "europages_company_id", "source"]
        )

        companyinfo_data_filled = companies.custom_join(
            companies_companyinfo_raw,
            custom_on=companies.data.company_id
            == companies_companyinfo_raw.data.companyinfo_company_id,
            custom_how="inner",
        )

        companyinfo_data_filled = companyinfo_data_filled.custom_drop(
            ["companyinfo_company_id", "europages_company_id", "source"]
        )

        europages_data_filled = companies.custom_join(
            companies_europages_raw,
            companies.data.company_id
            == companies_europages_raw.data.europages_company_id,
            custom_how="inner",
        )
        europages_data_filled = europages_data_filled.custom_drop(
            ["companyinfo_company_id", "europages_company_id", "source"]
        )

        both_data_filled = europages_data_filled.custom_union(companyinfo_data_filled)

        # process country data
        countries_mapper_raw = CustomDF("countries_mapper_raw", spark_generate)

        # Capitalize the first letter of the values in the 'country' column to match with countries_mapperpoductr
        both_data_filled.data = both_data_filled.data.withColumn(
            "country", F.initcap("country")
        )

        joined_companies_countries_mapper = both_data_filled.custom_join(
            countries_mapper_raw, custom_on="country", custom_how="inner"
        )

        companies_raw_final = joined_companies_countries_mapper.custom_drop(["country"])

        companies_raw_final.data = companies_raw_final.data.filter(
            # col("company_description").isNotNull() &
            col("address").isNotNull()
            & col("company_city").isNotNull()
            & col("postcode").isNotNull()
        )

        companies_raw_final = companies_raw_final.custom_select(
            [
                "company_id",
                "country_un",
                "source_id",
                "company_name",
                "company_description",
                "address",
                "company_city",
                "postcode",
            ]
        )
        companies_raw_final = companies_raw_final.custom_distinct()

        companies_raw_final.data = companies_raw_final.data.dropDuplicates(
            ["company_id"]
        )

        # Add data granularity score
        companies_sbi_activities = CustomDF(
            "companies_SBI_activities_datamodel", spark_generate
        )

        companies_raw_final = companies_raw_final.custom_join(
            companies_sbi_activities, "company_id", "left"
        )

        companies_products = CustomDF("companies_products_datamodel", spark_generate)
        products = CustomDF("products_datamodel", spark_generate)

        companies_products = companies_products.custom_join(
            products, "product_id", "left"
        )

        # first aggregate all product names related to a company into one string
        # then if that string is in fact an empty string, it should also count as null (no products related to the company)
        companies_products.data = (
            companies_products.data.groupBy("company_id")
            .agg(
                F.concat_ws(", ", F.collect_list("product_name")).alias("product_name")
            )
            .withColumn(
                "product_name",
                F.when(F.col("product_name") == "", None).otherwise(
                    F.col("product_name")
                ),
            )
            .orderBy("company_id")
        )

        companies_raw_final.data = companies_raw_final.data.join(
            companies_products.data, "company_id", "left"
        )
        # Add data granularity score
        companies_raw_final.data = companies_raw_final.data.withColumn(
            "data_granularity",
            F.when(F.col("product_name").isNotNull(), 2)
            .when(F.col("sbi_code").isNotNull(), 3)
            .when(F.col("company_description").isNotNull(), 4)
            .otherwise(5),
        )

        companies_raw_final = companies_raw_final.custom_select(
            [
                "company_id",
                "country_un",
                "source_id",
                "company_name",
                "company_description",
                "address",
                "company_city",
                "postcode",
                "data_granularity",
            ]
        )
        companies_datamodel = CustomDF(
            "companies_datamodel", spark_generate, initial_df=companies_raw_final.data
        )

        companies_datamodel.write_table()

    elif table_name == "EP_products_datamodel":

        companies_europages_raw = CustomDF("companies_europages_raw", spark_generate)

        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            "product_name", F.explode(F.split("products_and_services", "\|"))
        )
        companies_europages_raw = companies_europages_raw.custom_drop(
            "products_and_services"
        ).custom_select("product_name")

        # create product_id
        sha_columns = [
            F.col(col_name)
            for col_name in companies_europages_raw.data.columns
            if col_name not in ["tiltRecordID", "to_date"]
        ]

        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            "product_id", F.sha2(F.concat_ws("|", *sha_columns), 256)
        )

        products_datamodel = CustomDF(
            "products_datamodel",
            spark_generate,
            initial_df=companies_europages_raw.custom_select(
                "product_id", "product_name"
            ).distinct(),
        )

        products_datamodel.write_table()

    elif table_name == "companies_EP_products_datamodel":

        companies_europages_raw = CustomDF("companies_europages_raw", spark_generate)

        products_datamodel = CustomDF("EP_products_datamodel", spark_generate)

        rename_dict = {"id": "company_id"}

        companies_europages_raw.rename_columns(rename_dict)

        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            "product_name", F.explode(F.split("products_and_services", "\|"))
        )
        companies_europages_raw = companies_europages_raw.custom_drop(
            "products_and_services"
        )

        companies_joined_product_id = companies_europages_raw.custom_join(
            products_datamodel.data, "product_name"
        )

        companies_joined_without_product_name = companies_joined_product_id.custom_drop(
            "product_name"
        )

        companies_products_datamodel = CustomDF(
            "companies_products_datamodel",
            spark_generate,
            initial_df=companies_joined_without_product_name.custom_select(
                "company_id", "product_id"
            ),
        )
        companies_products_datamodel = companies_products_datamodel.custom_distinct()

        companies_products_datamodel.write_table()

    elif table_name == "companies_match_result_datamodel":

        companies_europages_raw = CustomDF("companies_europages_raw", spark_generate)

        companies_europages_raw.data = companies_europages_raw.data.filter(
            F.col("country") == "netherlands"
        )

        companies_europages_raw.data = companies_europages_raw.data.withColumn(
            "postcode_join", format_postcode(col("postcode"), col("company_city"))
        )

        europages = companies_europages_raw.custom_select(
            ["id", "company_name", "postcode_join"]
        )

        rename_dict = {"id": "europages_company_id", "company_name": "company_name_ep"}

        europages.rename_columns(rename_dict)

        companies_companyinfo_raw = CustomDF(
            "companies_companyinfo_raw", spark_generate
        )

        companies_companyinfo_raw.data = companies_companyinfo_raw.data.withColumn(
            "company_name",
            keep_one_name(F.col("Instellingsnaam"), F.col("Statutaire_naam")),
        )

        companies_companyinfo_raw.rename_columns(
            {
                "Kamer_van_Koophandel_nummer_12_cijferig": "companyinfo_company_id",
                "company_name": "company_name_ci",
                "Vestigingsadres_postcode": "postcode_join",
            }
        )

        companyinfo = companies_companyinfo_raw.custom_select(
            ["companyinfo_company_id", "postcode_join", "company_name_ci"]
        )

        jaro_winkler_udf = F.udf(jaro_winkler, DoubleType())

        joined_ep_ci = europages.custom_join(
            companyinfo, custom_on="postcode_join", custom_how="inner"
        )

        joined_ep_ci.data = joined_ep_ci.data.withColumn(
            "similarity_score",
            jaro_winkler_udf(col("company_name_ep"), col("company_name_ci")),
        )

        SIMILARITY_THRESHOLD = 0.95

        joined_ep_ci.data = joined_ep_ci.data.filter(
            col("similarity_score") >= F.lit(SIMILARITY_THRESHOLD)
        )

        matched = joined_ep_ci.custom_select(
            ["europages_company_id", "companyinfo_company_id"]
        ).custom_distinct()

        companies_match_result_datamodel = CustomDF(
            "companies_match_result_datamodel", spark_generate, initial_df=matched.data
        )

        companies_match_result_datamodel.write_table()

    # Ecoinvent data

    elif table_name == "intermediate_exchanges_datamodel":

        intermediate_exchanges_raw = CustomDF(
            "intermediate_exchanges_raw", spark_generate
        )

        rename_dict = {
            "ID": "exchange_id",
            "Name": "exchange_name",
            "Unit_Name": "unit_name",
        }

        intermediate_exchanges_raw.rename_columns(rename_dict)

        intermediate_exchanges_raw = intermediate_exchanges_raw.custom_select(
            ["exchange_id", "exchange_name", "unit_name"]
        )

        intermediate_exchanges_datamodel = CustomDF(
            "intermediate_exchanges_datamodel",
            spark_generate,
            initial_df=intermediate_exchanges_raw.data,
        )

        intermediate_exchanges_datamodel.write_table()

    elif table_name == "ecoinvent_co2_datamodel":

        ecoinvent_co2_raw = CustomDF("ecoinvent_co2_raw", spark_generate)

        rename_dict = {
            "Activity_UUID_Product_UUID": "activity_uuid_product_uuid",
            "IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq": "co2_footprint",
        }

        ecoinvent_co2_raw.rename_columns(rename_dict)

        ecoinvent_co2_raw.custom_select(["activity_uuid_product_uuid", "co2_footprint"])

        ecoinvent_co2_datamodel = CustomDF(
            "ecoinvent_co2_datamodel", spark_generate, initial_df=ecoinvent_co2_raw.data
        )

        ecoinvent_co2_datamodel.write_table()

    elif table_name == "ecoinvent_cut_off_datamodel":

        cut_off_ao_raw = CustomDF("cut_off_ao_raw", spark_generate)

        rename_dict = {
            "Activity_UUID_&_Product_UUID": "activity_uuid_product_uuid",
            "Activity_UUID": "activity_uuid",
            "Product_UUID": "product_uuid",
        }

        cut_off_ao_raw.rename_columns(rename_dict)

        cut_off_ao_raw = cut_off_ao_raw.custom_select(
            ["activity_uuid_product_uuid", "activity_uuid", "product_uuid"]
        )

        ecoinvent_cut_off_datamodel = CustomDF(
            "ecoinvent_cut_off_datamodel",
            spark_generate,
            initial_df=cut_off_ao_raw.data,
        )

        ecoinvent_cut_off_datamodel.write_table()

    elif table_name == "ecoinvent_product_datamodel":

        cut_off_ao_raw = CustomDF("cut_off_ao_raw", spark_generate)

        rename_dict = {
            "Product_UUID": "product_uuid",
            "Reference_Product_Name": "reference_product_name",
            "Unit": "unit",
        }

        cut_off_ao_raw.rename_columns(rename_dict)

        cut_off_ao_raw.data = cut_off_ao_raw.data.withColumn(
            "cpc_code", F.trim(F.split("CPC_Classification", ":")[0])
        )
        cut_off_ao_raw.data = cut_off_ao_raw.data.withColumn(
            "cpc_name", F.trim(F.split("CPC_Classification", ":")[1])
        )

        cut_off_ao_raw = cut_off_ao_raw.custom_select(
            ["product_uuid", "reference_product_name", "unit", "cpc_code", "cpc_name"]
        ).custom_distinct()

        ecoinvent_product_datamodel = CustomDF(
            "ecoinvent_product_datamodel",
            spark_generate,
            initial_df=cut_off_ao_raw.data,
        )

        ecoinvent_product_datamodel.write_table()

    elif table_name == "ecoinvent_activity_datamodel":

        cut_off_ao_raw = CustomDF("cut_off_ao_raw", spark_generate)

        rename_dict = {
            "Activity_UUID": "activity_uuid",
            "Activity_Name": "activity_name",
            "Geography": "geography",
            "Special_Activity_Type": "activity_type",
        }

        cut_off_ao_raw.rename_columns(rename_dict)

        cut_off_ao_raw.data = cut_off_ao_raw.data.withColumn(
            "isic_4digit", substring(col("ISIC_Classification"), 1, 4)
        )

        cut_off_ao_raw = cut_off_ao_raw.custom_select(
            [
                "activity_uuid",
                "activity_name",
                "activity_type",
                "geography",
                "isic_4digit",
            ]
        ).custom_distinct()

        ecoinvent_activity_datamodel = CustomDF(
            "ecoinvent_activity_datamodel",
            spark_generate,
            initial_df=cut_off_ao_raw.data,
        )

        ecoinvent_activity_datamodel.write_table()

    elif table_name == "ecoinvent_input_data_datamodel":

        ecoinvent_input_data_raw = CustomDF("ecoinvent_input_data_raw", spark_generate)

        ecoinvent_input_data_raw = ecoinvent_input_data_raw.custom_select(
            [
                "activityId",
                "activityName",
                "geography",
                "reference_product",
                "group",
                "exchange_name",
                "activityLinkId",
                "activityLink_activityName",
                "activityLink_geography",
                "exchange_unitName",
            ]
        ).custom_distinct()

        ecoinvent_input_data_datamodel = CustomDF(
            "ecoinvent_input_data_datamodel",
            spark_generate,
            initial_df=ecoinvent_input_data_raw.data,
        )

        ecoinvent_input_data_datamodel.write_table()

    # Mappers data

    elif table_name == "sources_mapper_datamodel":

        sources_mapper_raw = CustomDF("sources_mapper_raw", spark_generate)

        sources_mapper_raw.data = sources_mapper_raw.data.withColumn(
            "data_source_reliability",
            F.when(F.col("source_id") == "source_1_ep", 4)
            .when(F.col("source_id") == "source_2_eco", 2)
            .when(F.col("source_id") == "source_3_ci", 2)
            .when(F.col("source_id") == "source_4_ep_ci", 4)
            .otherwise(None),
        )

        sources_mapper_datamodel = CustomDF(
            "sources_mapper_datamodel",
            spark_generate,
            initial_df=sources_mapper_raw.data,
        )

        sources_mapper_datamodel.write_table()

    elif table_name == "countries_mapper_datamodel":

        countries_mapper_raw = CustomDF("countries_mapper_raw", spark_generate)

        countries_mapper_raw.data = countries_mapper_raw.data.withColumn(
            "country_un",
            F.when(countries_mapper_raw.data["country"] == "Namibia", "NA").otherwise(
                countries_mapper_raw.data["country_un"]
            ),
        )

        countries_mapper_datamodel = CustomDF(
            "countries_mapper_datamodel",
            spark_generate,
            initial_df=countries_mapper_raw.data,
        )

        countries_mapper_datamodel.write_table()

    elif table_name == "geography_ecoinvent_mapper_datamodel":

        geography_ecoinvent_mapper_raw = CustomDF(
            "geography_ecoinvent_mapper_raw", spark_generate
        )

        country_raw = CustomDF("country_raw", spark_generate)

        countries_mapper_raw = CustomDF("countries_mapper_raw", spark_generate)

        rename_dict = {"lca_geo": "ecoinvent_geography"}

        geography_ecoinvent_mapper_raw.rename_columns(rename_dict)

        geography_country_name = geography_ecoinvent_mapper_raw.custom_join(
            country_raw, custom_on="country_id"
        )

        geography_country_name.data = geography_country_name.data.withColumn(
            "country", F.initcap(col("country"))
        )

        geography_country_un = geography_country_name.custom_join(
            countries_mapper_raw, "country"
        )

        geography_country_un = geography_country_un.custom_drop(
            ["country_id", "country"]
        )

        geography_country_un = geography_country_un.custom_select(
            [
                "geography_id",
                "country_un",
                "ecoinvent_geography",
                "priority",
                "input_priority",
            ]
        )

        geography_ecoinvent_mapper_datamodel = CustomDF(
            "geography_ecoinvent_mapper_datamodel",
            spark_generate,
            initial_df=geography_country_un.data,
        )

        geography_ecoinvent_mapper_datamodel.write_table()

    elif table_name == "EP_tilt_sector_unmatched_mapper_datamodel":

        EP_tilt_sector_unmatched_mapper_raw = CustomDF(
            "EP_tilt_sector_unmatched_mapper_raw", spark_generate
        )

        EP_tilt_sector_unmatched_mapper_datamodel = CustomDF(
            "EP_tilt_sector_unmatched_mapper_datamodel",
            spark_generate,
            initial_df=EP_tilt_sector_unmatched_mapper_raw.data,
        )

        EP_tilt_sector_unmatched_mapper_datamodel.write_table()

    elif table_name == "tilt_sector_isic_mapper_datamodel":

        tilt_sector_isic_mapper_raw = CustomDF(
            "tilt_sector_isic_mapper_raw", spark_generate
        )

        tilt_sector_isic_mapper_raw = tilt_sector_isic_mapper_raw.custom_select(
            ["tilt_sector", "tilt_subsector", "isic_4digit", "isic_section"]
        )

        tilt_sector_isic_mapper_datamodel = CustomDF(
            "tilt_sector_isic_mapper_datamodel",
            spark_generate,
            initial_df=tilt_sector_isic_mapper_raw.data,
        )

        tilt_sector_isic_mapper_datamodel.write_table()

    elif table_name == "tilt_sector_scenario_mapper_datamodel":

        tilt_sector_scenario_weo_mapper_raw = CustomDF(
            "tilt_sector_scenario_mapper_raw", spark_generate
        )

        tilt_sector_scenario_ipr_mapper_raw = CustomDF(
            "tilt_sector_scenario_mapper_raw", spark_generate
        )

        tilt_sector_scenario_weo_mapper_raw.data = (
            tilt_sector_scenario_weo_mapper_raw.custom_select(
                ["tilt_sector", "tilt_subsector", "weo_product", "weo_flow"]
            ).data.withColumn("scenario_type", F.lit("weo"))
        )

        rename_ipr_dict = {
            "ipr_sector": "scenario_sector",
            "ipr_subsector": "scenario_subsector",
        }
        rename_weo_dict = {
            "weo_product": "scenario_sector",
            "weo_flow": "scenario_subsector",
        }

        tilt_sector_scenario_weo_mapper_raw.rename_columns(rename_weo_dict)

        tilt_sector_scenario_ipr_mapper_raw.data = (
            tilt_sector_scenario_ipr_mapper_raw.custom_select(
                ["tilt_sector", "tilt_subsector", "ipr_sector", "ipr_subsector"]
            ).data.withColumn("scenario_type", F.lit("ipr"))
        )

        tilt_sector_scenario_ipr_mapper_raw.rename_columns(rename_ipr_dict)

        both_scenarios = tilt_sector_scenario_weo_mapper_raw.custom_union(
            tilt_sector_scenario_ipr_mapper_raw
        )

        # Filtering null and non_match
        both_scenarios.data = both_scenarios.data.filter(
            (col("tilt_sector") != "no_match") & (col("tilt_sector").isNotNull())
        )

        both_scenarios = both_scenarios.custom_select(
            [
                "tilt_sector",
                "tilt_subsector",
                "scenario_type",
                "scenario_sector",
                "scenario_subsector",
            ]
        )

        tilt_sector_scenario_mapper_datamodel = CustomDF(
            "tilt_sector_scenario_mapper_datamodel",
            spark_generate,
            initial_df=both_scenarios.data,
        )

        tilt_sector_scenario_mapper_datamodel.write_table()

    # Scenario data

    elif table_name == "scenario_targets_IPR_datamodel":

        scenario_targets_IPR_raw = CustomDF("scenario_targets_IPR_raw", spark_generate)

        scenario_targets_IPR_raw = scenario_targets_IPR_raw.custom_select(
            ["Scenario", "Region", "Sector", "Sub_Sector", "Year", "Value"]
        )

        rename_dict = {
            "Scenario": "scenario",
            "Region": "region",
            "Sector": "ipr_sector",
            "Sub_Sector": "ipr_subsector",
            "Year": "year",
            "Value": "value",
        }

        scenario_targets_IPR_raw.rename_columns(rename_dict)

        # Select all columns that are needed for the creation of a record ID
        sha_columns = [
            F.col(col_name)
            for col_name in scenario_targets_IPR_raw.data.columns
            if col_name
            not in [
                "tiltRecordID",
                "to_date",
                "map_scenario_targets_IPR_raw",
                "from_date",
            ]
        ]

        # Create the SHA256 record ID by concatenating all relevant columns
        scenario_targets_IPR_raw.data = scenario_targets_IPR_raw.data.withColumn(
            "scenario_targets_ipr_id", F.sha2(F.concat_ws("|", *sha_columns), 256)
        )

        scenario_targets_IPR_raw = scenario_targets_IPR_raw.custom_select(
            [
                "scenario_targets_ipr_id",
                "scenario",
                "region",
                "ipr_sector",
                "ipr_subsector",
                "year",
                "value",
            ]
        ).custom_distinct()

        scenario_targets_IPR_datamodel = CustomDF(
            "scenario_targets_IPR_datamodel",
            spark_generate,
            initial_df=scenario_targets_IPR_raw.data,
        )

        scenario_targets_IPR_datamodel.write_table()

    elif table_name == "scenario_targets_WEO_datamodel":

        scenario_targets_WEO_raw = CustomDF("scenario_targets_WEO_raw", spark_generate)

        scenario_targets_WEO_raw = scenario_targets_WEO_raw.custom_select(
            ["SCENARIO", "REGION", "PRODUCT", "FLOW", "YEAR", "VALUE"]
        )

        rename_dict = {
            "SCENARIO": "scenario",
            "REGION": "region",
            "PRODUCT": "weo_sector",
            "FLOW": "weo_subsector",
            "YEAR": "year",
            "VALUE": "value",
        }

        scenario_targets_WEO_raw.rename_columns(rename_dict)

        # Select all columns that are needed for the creation of a record ID
        sha_columns = [
            F.col(col_name)
            for col_name in scenario_targets_WEO_raw.data.columns
            if col_name
            not in [
                "tiltRecordID",
                "to_date",
                "map_scenario_targets_WEO_raw",
                "from_date",
            ]
        ]

        # Create the SHA256 record ID by concatenating all relevant columns
        scenario_targets_WEO_raw.data = scenario_targets_WEO_raw.data.withColumn(
            "scenario_targets_weo_id", F.sha2(F.concat_ws("|", *sha_columns), 256)
        )

        scenario_targets_WEO_raw = scenario_targets_WEO_raw.custom_select(
            [
                "scenario_targets_weo_id",
                "scenario",
                "region",
                "weo_sector",
                "weo_subsector",
                "year",
                "value",
            ]
        ).custom_distinct()

        scenario_targets_WEO_datamodel = CustomDF(
            "scenario_targets_WEO_datamodel",
            spark_generate,
            initial_df=scenario_targets_WEO_raw.data,
        )

        scenario_targets_WEO_datamodel.write_table()

    elif table_name == "isic_mapper_datamodel":

        isic_4_digit_codes_landingzone = CustomDF("isic_mapper_raw", spark_generate)

        isic_4_digit_codes_landingzone = isic_4_digit_codes_landingzone.custom_select(
            ["Code", "ISIC_Rev_4_label"]
        )

        rename_dict = {"Code": "isic_4digit", "ISIC_Rev_4_label": "isic_4digit_name"}

        isic_4_digit_codes_landingzone.rename_columns(rename_dict)

        isic_mapper_datamodel = CustomDF(
            "isic_mapper_datamodel",
            spark_generate,
            initial_df=isic_4_digit_codes_landingzone.data,
        )
        isic_mapper_datamodel.write_table()

    elif table_name == "SBI_activities_datamodel":

        SBI_activities_raw = CustomDF("SBI_activities_raw", spark_generate)

        rename_dict = {"SBI": "sbi_code", "Omschrijving": "sbi_code_description"}

        SBI_activities_raw.rename_columns(rename_dict)

        SBI_activities_datamodel = CustomDF(
            "SBI_activities_datamodel",
            spark_generate,
            initial_df=SBI_activities_raw.data,
        )
        SBI_activities_datamodel.write_table()

    elif table_name == "companies_SBI_activities_datamodel":

        companies_companyinfo_raw = CustomDF(
            "companies_companyinfo_raw", spark_generate
        )

        companies_companyinfo_raw = companies_companyinfo_raw.custom_select(
            ["Kamer_van_Koophandel_nummer_12_cijferig", "SBI_code_locatie"]
        )

        rename_dict = {
            "Kamer_van_Koophandel_nummer_12_cijferig": "company_id",
            "SBI_code_locatie": "sbi_code",
        }

        companies_companyinfo_raw.rename_columns(rename_dict)

        companies_sbi_activities = companies_companyinfo_raw.custom_distinct()

        companies_sbi_activities.data = companies_sbi_activities.data.withColumn(
            "sbi_code",
            when(length(col("sbi_code")) < 4, lpad(col("sbi_code"), 4, "0")).otherwise(
                col("sbi_code")
            ),
        )

        companies_SBI_activities_datamodel = CustomDF(
            "companies_SBI_activities_datamodel",
            spark_generate,
            initial_df=companies_sbi_activities.data,
        )

        companies_SBI_activities_datamodel.write_table()

    elif table_name == "tiltLedger_datamodel":

        tiltLedger_raw = CustomDF("tiltLedger_raw", spark_generate)

        final_columns = [
            "CPC_Code",
            "CPC_Name",
            "ISIC_Code",
            "ISIC_Name",
            "Activity_Type",
            "Geography",
        ]

        tiltLedger_raw.custom_select(final_columns)

        # create tiltLedger_id
        sha_columns = [F.col(col_name) for col_name in final_columns]

        tiltLedger_raw.data = tiltLedger_raw.data.withColumn(
            "tiltLedger_id", F.sha2(F.concat_ws("|", *sha_columns), 256)
        )

        tiltLedger_raw.data = tiltLedger_raw.data.withColumn(
            "tiltLedger_id", F.sha2(F.concat_ws("|", *sha_columns), 256)
        )

        tiltLedger_raw.data = tiltLedger_raw.data.withColumn(
            "model_certainty",
            F.when(
                # Manually reviewed OR Verified Source
                (F.col("Manual_Review") == 1) | (F.col("Verified_Source") == 1),
                1,
            )
            .when(
                # Not Manually reveiwed and Not Verified source and 2 <= Distance <= 3
                (F.col("Manual_Review") == 0)
                & (F.col("Verified_Source") == 0)
                & (F.col("Distance") >= 2)
                & (F.col("Distance") <= 3),
                3,
            )
            .when(
                # Not Manually reveiwed and Not Verified source and Distance < 3
                (F.col("Manual_Review") == 0)
                & (F.col("Verified_Source") == 0)
                & (F.col("Distance") > 3),
                5,
            )
            .otherwise(None),
        )

        tiltLedger_raw_final = tiltLedger_raw.custom_select(
            [
                "tiltLedger_id",
                "CPC_Code",
                "CPC_Name",
                "ISIC_Code",
                "ISIC_Name",
                "Activity_Type",
                "Geography",
                "Distance",
                "Manual_Review",
                "Verified_Source",
                "model_certainty",
            ]
        )

        tiltLedger_datamodel = CustomDF(
            "tiltLedger_datamodel", spark_generate, initial_df=tiltLedger_raw_final.data
        )

        tiltLedger_datamodel.write_table()

    else:
        raise ValueError(
            f"The table: {table_name} is not specified in the processing functions"
        )

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not "DATABRICKS_RUNTIME_VERSION" in os.environ:
        spark_generate.stop()
