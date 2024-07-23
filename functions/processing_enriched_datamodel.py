import os
import pyspark.sql.functions as F
from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session
from functions.dataframe_helpers import *
from pyspark.sql.functions import col, substring


def generate_table(table_name: str) -> None:
    """
    Generate a specified table from the datamodel layer using either a remote or the Databricks SparkSession.

    This function generates a specified enriched datamodel table from the normalized datamodel. The table to be generated is determined by the 'table_name' argument.

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

    if table_name == 'emission_profile_ledger_enriched':
        # Load in the necessary dataframes
        emission_enriched_ledger = CustomDF("ledger_ecoinvent_mapping_datamodel", spark_generate)
        tilt_sector_isic_mapper = CustomDF("tilt_sector_isic_mapper_datamodel", spark_generate)

        # prepping
        tilt_sector_isic_mapper.rename_columns({"isic_4digit": "isic_code"})
        input_emission_profile_ledger = emission_enriched_ledger.custom_join(tilt_sector_isic_mapper, custom_on="isic_code", custom_how="left").custom_drop("isic_section")
        # convert co2 column to decimal
        co2_column = [s for s in input_emission_profile_ledger.data.columns if "co2_footprint" in s][0]
        input_emission_profile_ledger.data = input_emission_profile_ledger.data.withColumn(co2_column, F.col(co2_column).cast("decimal(10,5)"))

        input_emission_profile_ledger = input_emission_profile_ledger.custom_select(["tiltledger_id", "co2_footprint", "reference_product_name", "activity_name", "geography", "isic_code", "tilt_sector", "tilt_subsector", "unit"])
        input_emission_profile_ledger.data = emissions_profile_compute(input_emission_profile_ledger.data, "combined")
        input_emission_profile_ledger = input_emission_profile_ledger.custom_select(["tiltledger_id","benchmark_group", "risk_category", "average_profile_ranking", "product_name", "activity_name"])

        emission_profile_ledger_level = CustomDF("emission_profile_ledger_enriched", spark_generate,
                                    initial_df=input_emission_profile_ledger.data)
        
        emission_profile_ledger_level.write_table()
    elif table_name == 'emission_profile_ledger_upstream_enriched':
        # Load in the necessary dataframes
        emission_enriched_ledger = CustomDF("ledger_ecoinvent_mapping_datamodel", spark_generate)
        tilt_sector_isic_mapper = CustomDF("tilt_sector_isic_mapper_datamodel", spark_generate)
        ecoinvent_input_data = CustomDF("ecoinvent_input_data_datamodel", spark_generate)
        input_geography_filter = CustomDF("geography_ecoinvent_mapper_datamodel", spark_generate).custom_select(["geography_id", "ecoinvent_geography", "priority", "input_priority"])

        # prepping
        tilt_sector_isic_mapper.rename_columns({"isic_4digit": "isic_code"})
        # Prefix all column names with "input_"
        input_emission_enriched_ledger = emission_enriched_ledger.custom_select([
            *[F.col(c).alias("input_" + c) for c in emission_enriched_ledger.data.columns[:-1]]]
        )

        intermediate_upstream_ledger = emission_enriched_ledger.custom_join(ecoinvent_input_data,"Activity_UUID_Product_UUID", 
                                    custom_how="left")
        intermediate_upstream_ledger.data = intermediate_upstream_ledger.data.filter(F.col("input_activity_uuid_product_uuid").isNotNull())
   
        intermediate_upstream_ledger = intermediate_upstream_ledger.custom_join(input_emission_enriched_ledger, "input_activity_uuid_product_uuid", custom_how="left")
        intermediate_upstream_ledger.data = intermediate_upstream_ledger.data.filter(F.col("input_tiltledger_id").isNotNull()).filter(F.col("input_co2_footprint").isNotNull())
        intermediate_upstream_ledger = intermediate_upstream_ledger.custom_select([
                                        "input_tiltledger_id", "tiltledger_id", "activity_name", "geography", "input_reference_product_name", "input_co2_footprint", 
                                        "input_geography", "input_isic_code", "input_unit"]
                                    )

        intermediate_upstream_ledger.rename_columns({"geography":"product_geography", "input_reference_product_name":"input_product_name"})

        input_data_filtered = intermediate_upstream_ledger.custom_join(input_geography_filter, (F.col("input_geography") == F.col("ecoinvent_geography")), custom_how="left")
        # Define the window specification
        window_spec = Window.partitionBy("tiltledger_id", "input_product_name").orderBy(F.col("input_priority").asc())
        # Add a row number column within each group
        input_data_filtered.data = input_data_filtered.data.withColumn("row_num", F.row_number().over(window_spec))

        geography_checker(input_data_filtered.data)

        input_data_filtered.data = input_data_filtered.data.filter(F.col("row_num") <= 1).drop("row_num").withColumn("index", F.monotonically_increasing_id())
        input_data_filtered = input_data_filtered.custom_drop(["index", "priority", "geography_id", "country"])
        emission_enriched_ledger_upstream_data = input_data_filtered.custom_select(["input_tiltledger_id", "tiltledger_id", "activity_name", "product_geography", "input_product_name", "input_co2_footprint", "input_geography", "input_isic_code", "input_unit"
                                                                                 , "input_priority"]).custom_distinct()

        input_emission_profile_ledger_upstream = (emission_enriched_ledger_upstream_data.custom_join(tilt_sector_isic_mapper, 
                                                                                        (F.col("input_isic_code") == F.col("isic_code")),
                                                                                            custom_how='left')
                                    .custom_distinct().custom_drop(["isic_code", "isic_section"])
        )
        input_emission_profile_ledger_upstream.data = emissions_profile_upstream_compute(input_emission_profile_ledger_upstream.data, "combined")

        input_emission_profile_ledger_upstream = input_emission_profile_ledger_upstream.custom_select(["input_tiltledger_id", "tiltledger_id", "benchmark_group", "risk_category", "profile_ranking",
                                                                                                        "input_product_name", "input_co2_footprint"])

        emission_profile_ledger_upstream_level = CustomDF("emission_profile_ledger_upstream_enriched", spark_generate,
                                                          initial_df=input_emission_profile_ledger_upstream.data)
        
        emission_profile_ledger_upstream_level.write_table()

    elif table_name == 'sector_profile_ledger_enriched':
        # Load in the necessary dataframes
        tilt_ledger = CustomDF("tiltLedger_datamodel", spark_generate)
        tilt_sector_isic_mapper = CustomDF("tilt_sector_isic_mapper_datamodel", spark_generate)
        tilt_sector_scenario_mapper = CustomDF("tilt_sector_scenario_mapper_datamodel", spark_generate)
        scenario_targets_weo = CustomDF("scenario_targets_WEO_datamodel", spark_generate)
        scenario_targets_ipr = CustomDF("scenario_targets_IPR_datamodel", spark_generate)

        # prepping
        tilt_sector_isic_mapper.rename_columns({"isic_4digit": "isic_code"})
        scenario_targets_weo.data = scenario_targets_weo.data.filter(F.col("scenario") == "Net Zero Emissions by 2050 Scenario")
        scenario_targets_ipr.data = scenario_targets_ipr.data.filter(F.col("scenario") == "1.5C Required Policy Scenario")

        # add tilt sector and subsector to tilt ledger
        sector_enriched_ledger = tilt_ledger.custom_join(tilt_sector_isic_mapper, custom_on="isic_code", custom_how="left").custom_select(["tiltledger_id", "cpc_name", "tilt_sector", "tilt_subsector"])

        # add scenario to tilt ledger
        scenario_enriched_ledger = sector_enriched_ledger.custom_join(tilt_sector_scenario_mapper, custom_on=["tilt_sector", "tilt_subsector"], custom_how="left")
        scenario_enriched_ledger.rename_columns({"cpc_name": "product_name"})
        scenario_enriched_ledger.data = scenario_enriched_ledger.data.dropna(subset=["scenario_type"])
        scenario_enriched_ledger = scenario_enriched_ledger.custom_distinct()

        scenario_targets_weo.data = calculate_reductions(scenario_targets_weo.data, name_replace_dict = {'Net Zero Emissions by 2050 Scenario': 'NZ 2050'})
        scenario_targets_ipr.data = calculate_reductions(scenario_targets_ipr.data, {'1.5C Required Policy Scenario': '1.5C RPS'})
        
        scenario_targets_ipr.data = scenario_preparing(scenario_targets_ipr.data)
        scenario_targets_weo.data = scenario_preparing(scenario_targets_weo.data)
        combined_scenario_targets = get_combined_targets(scenario_targets_ipr, scenario_targets_weo)
        
        combined_scenario_targets.rename_columns(
            {"scenario_type": "scenario_type_y", "scenario_sector":"scenario_sector_y", "scenario_subsector":"scenario_subsector_y"}
            )
        
        input_sector_profile_ledger = scenario_enriched_ledger.custom_join(combined_scenario_targets, 
                                                            (F.col("scenario_type") == F.col("scenario_type_y")) &
                                                            (F.col("scenario_sector").eqNullSafe(F.col("scenario_sector_y"))) &
                                                            (F.col("scenario_subsector").eqNullSafe(F.col("scenario_subsector_y"))),
                                                            custom_how="left")

        input_sector_profile_ledger = input_sector_profile_ledger.custom_select(["tiltledger_id","tilt_sector", "tilt_subsector", "product_name", "scenario_type",
                                                                                "scenario_sector", "scenario_subsector", "scenario_name", "region", "year",
                                                                                "value", "reductions"])
        
        input_sector_profile_ledger.data = input_sector_profile_ledger.data.dropna(subset="scenario_name")
        input_sector_profile_ledger.data = sector_profile_compute(input_sector_profile_ledger.data)
        
        input_sector_profile_ledger = input_sector_profile_ledger.custom_select(["tiltledger_id", "risk_category", "benchmark_group", "profile_ranking", "product_name", "tilt_sector", "scenario_name", "scenario_type", "year"]).custom_distinct()

        sector_profile_ledger_level = CustomDF("sector_profile_ledger_enriched", spark_generate, 
                                               initial_df=input_sector_profile_ledger.data)

        # print(sector_profile_ledger_level.data.columns)
        sector_profile_ledger_level.write_table()
    
    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()
