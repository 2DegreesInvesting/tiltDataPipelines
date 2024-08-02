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
        print(f"Loading data for {table_name}")
        ## LOAD
        # ecoinvent
        ecoinvent_product = CustomDF("ecoinvent_product_datamodel", spark_generate)
        ecoinvent_activity = CustomDF("ecoinvent_activity_datamodel", spark_generate)
        ecoinvent_co2 = CustomDF("ecoinvent_co2_datamodel", spark_generate)
        ecoinvent_cut_off = CustomDF("ecoinvent_cut_off_datamodel", spark_generate)
        # mappers
        ledger_ecoinvent_mapping = CustomDF("ledger_ecoinvent_mapping_datamodel", spark_generate)
        tilt_sector_isic_mapper = CustomDF("tilt_sector_isic_mapper_datamodel", spark_generate)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        ei_record_info = ecoinvent_product.custom_join(ecoinvent_cut_off, "product_uuid", custom_how="left").custom_join(ecoinvent_activity, "activity_uuid", custom_how="left").custom_join(ecoinvent_co2, "activity_uuid_product_uuid", custom_how="left").custom_select(
            ['activity_uuid_product_uuid','activity_uuid','product_uuid','reference_product_name','unit','cpc_code','cpc_name','activity_name','activity_type','geography','isic_4digit','co2_footprint']
        )
        ei_record_info.data = ei_record_info.data.withColumn("geography", F.lower(F.col("geography")))
        ei_record_info.rename_columns({"isic_4digit": "isic_code"})

        ## PREPPING
        tilt_sector_isic_mapper.rename_columns({"isic_4digit": "isic_code"})
        sector_enriched_ecoinvent_co2 = ei_record_info.custom_join(tilt_sector_isic_mapper, custom_on="isic_code", custom_how="left").custom_drop("isic_section")

        print("Running checks...")
        ## CHECK
        check_nonempty_tiltsectors_for_nonempty_isic_pyspark(sector_enriched_ecoinvent_co2.data)

        ## CHECK 
        check_null_product_name_pyspark(sector_enriched_ecoinvent_co2.data)

        ## CHECK
        column_check(sector_enriched_ecoinvent_co2.data)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        co2_column = [s for s in sector_enriched_ecoinvent_co2.data.columns if "co2_footprint" in s][0]
        sector_enriched_ecoinvent_co2.data = sector_enriched_ecoinvent_co2.data.withColumn(co2_column, F.col(co2_column).cast("decimal(10,5)"))

        ## PREPPING
        sector_enriched_ecoinvent_co2.data = sanitize_co2(sector_enriched_ecoinvent_co2.data)

        ## PREPPING
        sector_enriched_ecoinvent_co2 = sector_enriched_ecoinvent_co2.custom_distinct()
        sector_enriched_ecoinvent_co2.data = prepare_co2(sector_enriched_ecoinvent_co2.data)

        ## PREPPING
        emission_profile_ledger_level = sector_enriched_ecoinvent_co2.custom_select(["activity_uuid_product_uuid", "co2_footprint", "reference_product_name", "activity_name", "geography", "isic_code", "tilt_sector", "tilt_subsector", "unit"])

        print(f"Calculating indicators for {table_name}")
        ## CALCULATION
        emission_profile_ledger_level.data = emissions_profile_compute(emission_profile_ledger_level.data, ledger_ecoinvent_mapping.data, "combined")

        ## PREPPING
        emission_profile_ledger_level = emission_profile_ledger_level.custom_select(["tiltledger_id","benchmark_group", "risk_category", "average_profile_ranking", "product_name", "average_co2_footprint"])

        ## DF CREATION
        emission_profile_ledger_level = CustomDF("emission_profile_ledger_enriched", spark_generate,
                                    initial_df=emission_profile_ledger_level.data)
        
        print(f"Writing data for {table_name}")
        ## WRITE
        emission_profile_ledger_level.write_table()
        print("Data written successfully!\n")

    elif table_name == 'emission_profile_ledger_upstream_enriched':
        print(f"Loading data for {table_name}")
        ## LOAD
        # tilt data
        tilt_ledger = CustomDF("tiltLedger_datamodel", spark_generate)
        # ecoinvent
        ecoinvent_input_data = CustomDF("ecoinvent_input_data_datamodel", spark_generate)
        ecoinvent_product = CustomDF("ecoinvent_product_datamodel", spark_generate)
        ecoinvent_activity = CustomDF("ecoinvent_activity_datamodel", spark_generate)
        ecoinvent_co2 = CustomDF("ecoinvent_co2_datamodel", spark_generate)
        ecoinvent_cut_off = CustomDF("ecoinvent_cut_off_datamodel", spark_generate)
        # mappers
        ledger_ecoinvent_mapping = CustomDF("ledger_ecoinvent_mapping_datamodel", spark_generate)
        tilt_sector_isic_mapper = CustomDF("tilt_sector_isic_mapper_datamodel", spark_generate)
        geography_ecoinvent_mapper = CustomDF("geography_ecoinvent_mapper_datamodel", spark_generate).custom_select(["geography_id", "ecoinvent_geography", "priority", "input_priority"])

        print(f"Preparing data for {table_name}")
        ## PREPPING
        valid_countries = ['NL', 'AT', 'GB', 'DE', 'ES', 'FR', 'IT']
        tilt_ledger.data = tilt_ledger.data.dropna(subset=["CPC_Code", "isic_code", "Geography"])
        tilt_ledger.data = tilt_ledger.data.filter(tilt_ledger.data.Geography.isin(valid_countries)).withColumn("Geography", F.lower(F.col("Geography"))).select([F.col(column).alias(column.lower()) for column in tilt_ledger.data.columns])
        tilt_ledger.data = tilt_ledger.data.withColumn("cpc_name", F.regexp_replace(F.trim(F.lower(F.col("cpc_name"))), "<.*?>", "")).withColumn("activity_type", F.lower(F.col("activity_type")))
        tilt_sector_isic_mapper.rename_columns({"isic_4digit": "isic_code"})
        ei_record_info = ecoinvent_product.custom_join(ecoinvent_cut_off, "product_uuid", custom_how="left").custom_join(ecoinvent_activity, "activity_uuid", custom_how="left").custom_join(ecoinvent_co2, "activity_uuid_product_uuid", custom_how="left").custom_select(
            ['activity_uuid_product_uuid','activity_uuid','product_uuid','reference_product_name','unit','cpc_code','cpc_name','activity_name','activity_type','geography','isic_4digit','co2_footprint']
        )
        ei_record_info.data = ei_record_info.data.withColumn("geography", F.lower(F.col("geography")))
        ei_record_info.rename_columns({"isic_4digit": "isic_code"})

        ## PREPPING
        input_ei_record_info = ei_record_info.custom_select([
            *[F.col(c).alias("input_" + c) for c in ei_record_info.data.columns[:-1]]]
        )
        
        ## PREPPING
        input_ei_mapping = ei_record_info.custom_join(ecoinvent_input_data,"Activity_UUID_Product_UUID", 
                                    custom_how="left")
        input_ei_mapping.data = input_ei_mapping.data.filter(F.col("input_activity_uuid_product_uuid").isNotNull())

        ## PREPPING
        input_ei_mapping = input_ei_mapping.custom_join(input_ei_record_info, "input_activity_uuid_product_uuid", custom_how="left")
        input_ei_mapping.data = input_ei_mapping.data.filter(F.col("input_activity_uuid_product_uuid").isNotNull())
        input_ei_mapping.data = input_ei_mapping.data.filter(F.col("input_co2_footprint").isNotNull())
        input_ei_mapping = input_ei_mapping.custom_select([
                                        "input_activity_uuid_product_uuid", "activity_uuid_product_uuid", "activity_name", "geography", "input_reference_product_name", "input_co2_footprint", 
                                        "input_geography", "input_isic_code", "input_unit"]
                                    )

        ## PREPPING
        input_ei_mapping.rename_columns({"geography":"product_geography", "input_reference_product_name":"input_product_name"})

        ## PREPPING
        input_ei_mapping_w_geography = input_ei_mapping.custom_join(geography_ecoinvent_mapper, (F.col("input_geography") == F.col("ecoinvent_geography")), custom_how="left")
        window_spec = Window.partitionBy("activity_uuid_product_uuid", "input_product_name").orderBy(F.col("input_priority").asc())
        input_ei_mapping_w_geography.data = input_ei_mapping_w_geography.data.withColumn("row_num", F.row_number().over(window_spec))

        print("Running checks...")
        ## CHECK
        ei_geography_checker(input_ei_mapping_w_geography.data)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        input_ei_mapping_w_geography.data = input_ei_mapping_w_geography.data.filter(F.col("row_num") <= 1)
        input_ei_mapping_geo_filtered = input_ei_mapping_w_geography.custom_drop(["row_num","priority", "geography_id", "country"])
        emission_enriched_ledger_upstream_data = input_ei_mapping_geo_filtered.custom_select(["input_activity_uuid_product_uuid", "activity_uuid_product_uuid", "activity_name", "product_geography", "input_product_name", "input_co2_footprint", "input_geography", "input_isic_code", "input_unit"
                                                                                 , "input_priority"])
        ## PREPPING
        input_tilt_sector_isic_mapper = tilt_sector_isic_mapper.custom_select([
            *[F.col(c).alias("input_" + c) for c in tilt_sector_isic_mapper.data.columns[:-1]]
            ]
        )

        ## PREPPING
        emission_enriched_ledger_upstream_data = (emission_enriched_ledger_upstream_data.custom_join(input_tilt_sector_isic_mapper, 
                                                                                   "input_isic_code",
                                                                                    custom_how='left')
        )
        emission_enriched_ledger_upstream_data.data = prepare_co2(emission_enriched_ledger_upstream_data.data)

        print("Running checks...")
        ## CHECK
        check_nonempty_tiltsectors_for_nonempty_isic_pyspark(emission_enriched_ledger_upstream_data.data)

        ## CHECK 
        check_null_product_name_pyspark(emission_enriched_ledger_upstream_data.data)

        ## CHECK
        column_check(emission_enriched_ledger_upstream_data.data)

        print(f"Calculating indicators for {table_name}")

        ## CALCULATION
        emission_enriched_ledger_upstream_data.data = emissions_profile_upstream_compute(emission_enriched_ledger_upstream_data.data, ledger_ecoinvent_mapping.data, "combined")

        print(f"Preparing data for {table_name}")
        ## PREPPING
        emission_enriched_ledger_upstream_data = emission_enriched_ledger_upstream_data.custom_select(["input_activity_uuid_product_uuid", "tiltledger_id", "benchmark_group", "risk_category", "profile_ranking",
                                                                                                        "input_product_name", "input_co2_footprint"])
        
        
        path = "abfss://landingzone@storagetiltdevelop.dfs.core.windows.net/test/"
        emission_enriched_ledger_upstream_data.data.write.mode("overwrite").parquet(path)

        df = spark_generate.read.format("parquet").load(path)

        df.show()
        print(f"Writing data for {table_name}")
        # DF CREATION
        emission_profile_ledger_upstream_level = CustomDF("emission_profile_ledger_upstream_enriched", spark_generate,
                                                          initial_df=df)
        ## WRITE
        emission_profile_ledger_upstream_level.write_table()
        print("Data written successfully!")

    elif table_name == 'sector_profile_ledger_enriched':
        print(f"Loading data for {table_name}")
        ## LOAD
        # tilt data
        tilt_ledger = CustomDF("tiltLedger_datamodel", spark_generate)
        # mappers
        tilt_sector_isic_mapper = CustomDF("tilt_sector_isic_mapper_datamodel", spark_generate)
        tilt_sector_scenario_mapper = CustomDF("tilt_sector_scenario_mapper_datamodel", spark_generate)
        # scenario data
        scenario_targets_weo = CustomDF("scenario_targets_WEO_datamodel", spark_generate)
        scenario_targets_ipr = CustomDF("scenario_targets_IPR_datamodel", spark_generate)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        valid_countries = ['NL', 'AT', 'GB', 'DE', 'ES', 'FR', 'IT']
        tilt_ledger.data = tilt_ledger.data.dropna(subset=["CPC_Code", "isic_code", "Geography"])
        tilt_ledger.data = tilt_ledger.data.filter(tilt_ledger.data.Geography.isin(valid_countries)).withColumn("Geography", F.lower(F.col("Geography"))).select([F.col(column).alias(column.lower()) for column in tilt_ledger.data.columns])
        tilt_ledger.data = tilt_ledger.data.withColumn("cpc_name", F.regexp_replace(F.trim(F.lower(F.col("cpc_name"))), "<.*?>", "")).withColumn("activity_type", F.lower(F.col("activity_type")))
        tilt_sector_isic_mapper.rename_columns({"isic_4digit": "isic_code"})
        scenario_targets_weo.data = scenario_targets_weo.data.filter(F.col("scenario") == "Net Zero Emissions by 2050 Scenario")
        scenario_targets_ipr.data = scenario_targets_ipr.data.filter(F.col("scenario") == "1.5C Required Policy Scenario")

        ## PREPPING
        sector_enriched_ledger = tilt_ledger.custom_join(tilt_sector_isic_mapper, custom_on="isic_code", custom_how="left").custom_select(["tiltledger_id", "cpc_name", "tilt_sector", "tilt_subsector"])

        ## PREPPING
        scenario_enriched_ledger = sector_enriched_ledger.custom_join(tilt_sector_scenario_mapper, custom_on=["tilt_sector", "tilt_subsector"], custom_how="left")
        scenario_enriched_ledger.rename_columns({"cpc_name": "product_name"})
        scenario_enriched_ledger.data = scenario_enriched_ledger.data.dropna(subset=["scenario_type"])
        scenario_enriched_ledger = scenario_enriched_ledger.custom_distinct()

        ## PREPPING
        scenario_targets_weo.data = calculate_reductions(scenario_targets_weo.data, name_replace_dict = {'Net Zero Emissions by 2050 Scenario': 'NZ 2050'})
        scenario_targets_ipr.data = calculate_reductions(scenario_targets_ipr.data, {'1.5C Required Policy Scenario': '1.5C RPS'})
        
        ## PREPPING
        scenario_targets_ipr.data = scenario_preparing(scenario_targets_ipr.data)
        scenario_targets_weo.data = scenario_preparing(scenario_targets_weo.data)
        combined_scenario_targets = get_combined_targets(scenario_targets_ipr, scenario_targets_weo)
        
        ## PREPPING
        combined_scenario_targets.rename_columns(
            {"scenario_type": "scenario_type_y", "scenario_sector":"scenario_sector_y", "scenario_subsector":"scenario_subsector_y"}
            )
        
        ## PREPPING
        input_sector_profile_ledger = scenario_enriched_ledger.custom_join(combined_scenario_targets, 
                                                            (F.col("scenario_type") == F.col("scenario_type_y")) &
                                                            (F.col("scenario_sector").eqNullSafe(F.col("scenario_sector_y"))) &
                                                            (F.col("scenario_subsector").eqNullSafe(F.col("scenario_subsector_y"))),
                                                            custom_how="left")
        ## PREPPING
        input_sector_profile_ledger = input_sector_profile_ledger.custom_select(["tiltledger_id","tilt_sector", "tilt_subsector", "product_name", "scenario_type",
                                                                                "scenario_sector", "scenario_subsector", "scenario_name", "region", "year",
                                                                                "value", "reductions"])
        
        print("Running checks...")
        ## CHECK 
        check_null_product_name_pyspark(input_sector_profile_ledger.data)

        print(f"Calculating indicators for {table_name}")
        ## CALCULATION
        input_sector_profile_ledger.data = input_sector_profile_ledger.data.dropna(subset="scenario_name")
        input_sector_profile_ledger.data = sector_profile_compute(input_sector_profile_ledger.data)
        
        print(f"Preparing data for {table_name}")
        ## PREPPING
        input_sector_profile_ledger = input_sector_profile_ledger.custom_select(["tiltledger_id", "benchmark_group", "risk_category", "profile_ranking", "product_name", "tilt_sector", "scenario_name", "scenario_type", "year"]).custom_distinct()

        ## DF CREATION
        sector_profile_ledger_level = CustomDF("sector_profile_ledger_enriched", spark_generate, 
                                               initial_df=input_sector_profile_ledger.data)
        print(f"Writing data for {table_name}")
        ## WRITE
        sector_profile_ledger_level.write_table()
        print("Data written successfully!\n")
    
    elif table_name == 'sector_profile_ledger_upstream_enriched':
        print(f"Loading data for {table_name}")
        ## LOAD
        # tilt data
        tilt_ledger = CustomDF("tiltLedger_datamodel", spark_generate)
        # ecoinvent
        ecoinvent_input_data = CustomDF("ecoinvent_input_data_datamodel", spark_generate)
        # mappers
        ledger_ecoinvent_mapping = CustomDF("ledger_ecoinvent_mapping_datamodel", spark_generate)
        geography_ecoinvent_mapper = CustomDF("geography_ecoinvent_mapper_datamodel", spark_generate).custom_select(["geography_id", "ecoinvent_geography", "priority", "input_priority"])
        # indicators
        sector_profile = CustomDF("sector_profile_ledger_enriched", spark_generate)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        valid_countries = ['NL', 'AT', 'GB', 'DE', 'ES', 'FR', 'IT']
        tilt_ledger.data = tilt_ledger.data.dropna(subset=["CPC_Code", "isic_code", "Geography"])
        tilt_ledger.data = tilt_ledger.data.filter(tilt_ledger.data.Geography.isin(valid_countries)).withColumn("Geography", F.lower(F.col("Geography"))).select([F.col(column).alias(column.lower()) for column in tilt_ledger.data.columns])
        tilt_ledger.data = tilt_ledger.data.withColumn("cpc_name", F.regexp_replace(F.trim(F.lower(F.col("cpc_name"))), "<.*?>", "")).withColumn("activity_type", F.lower(F.col("activity_type")))
        
        ## PREPPING
        input_sector_profile = sector_profile.custom_select([
                    *[F.col(c).alias("input_" + c) for c in sector_profile.data.columns[:-1]]]
                )
        
        ## PREPPING
        ledger_ecoinvent_mapping_w_geography = ledger_ecoinvent_mapping.custom_join(tilt_ledger,"tiltledger_id", custom_how="left").custom_select(["tiltledger_id", "activity_uuid_product_uuid", "geography"])
        ledger_ecoinvent_mapping_w_geography = ledger_ecoinvent_mapping_w_geography.custom_select([
            *[F.col(c).alias("input_" + c) for c in ledger_ecoinvent_mapping_w_geography.data.columns[:-1]]
            ]
        )

        ## PREPPING
        input_ledger_mapping = ledger_ecoinvent_mapping.custom_select(
            ["tiltledger_id", "activity_uuid_product_uuid"]).custom_join(
                ecoinvent_input_data, custom_on="activity_uuid_product_uuid", custom_how="left").custom_drop(["product_name", "amount","activity_uuid_product_uuid"])
        input_ledger_mapping.data = input_ledger_mapping.data.filter(F.col("Input_Activity_UUID_Product_UUID").isNotNull())
        input_ledger_mapping = input_ledger_mapping.custom_join(ledger_ecoinvent_mapping_w_geography,"input_activity_uuid_product_uuid", custom_how="left").custom_drop(["input_activity_uuid_product_uuid"])
        input_ledger_mapping.data= input_ledger_mapping.data.filter(F.col("input_tiltledger_id").isNotNull())

        print(f"Calculating indicators for {table_name}")
        ## CALCULATION
        input_sector_profile_ledger = input_ledger_mapping.custom_join(input_sector_profile, "input_tiltledger_id", custom_how="left")

        print(f"Preparing data for {table_name}")
        ## PREPPING
        input_data_filtered = input_sector_profile_ledger.custom_join(geography_ecoinvent_mapper, (F.col("input_geography") == F.col("ecoinvent_geography")), custom_how="left")
        # Define the window specification
        window_spec = Window.partitionBy("tiltledger_id", "input_product_name").orderBy(F.col("input_priority").asc())
        # Add a row number column within each group
        input_data_filtered.data= input_data_filtered.data.withColumn("row_num", F.row_number().over(window_spec))

        ## CHECK 
        # ledger_geography_checker(input_data_filtered)

        ## PREPPING
        input_data_filtered.data = input_data_filtered.data.filter(F.col("row_num") <= 1)
        input_data_filtered = input_data_filtered.custom_drop(["row_num","index", "priority", "geography_id", "country"])

        print("Running checks...")
        ## CHECK 
        check_null_product_name_pyspark(input_data_filtered.data)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        sector_enriched_ledger_upstream_data = input_data_filtered.custom_select([
                                "input_tiltledger_id", "tiltledger_id","input_benchmark_group", "input_risk_category","input_profile_ranking","input_product_name", "input_tilt_sector","input_scenario_name", "input_scenario_type", "input_year"]).custom_distinct()

        sector_enriched_ledger_upstream_data.data = sector_enriched_ledger_upstream_data.data.dropna(subset="input_scenario_name")
        sector_enriched_ledger_upstream_data.rename_columns({"input_benchmark_group":"benchmark_group", "input_risk_category":"risk_category", "input_profile_ranking":"profile_ranking"})
        # print(sector_enriched_ledger_upstream_data.data.select("tiltledger_id").distinct().count())

        ## DF CREATION
        sector_profile_ledger_upstream_level = CustomDF("sector_profile_ledger_upstream_enriched", spark_generate,
                                                        initial_df=sector_enriched_ledger_upstream_data.data)
        
        print(f"Writing data for {table_name}")
        ## WRITE
        sector_profile_ledger_upstream_level.write_table()
        print("Data written successfully!\n")

    elif table_name == 'transition_risk_ledger_enriched':
        print(f"Loading data for {table_name}")
        # LOAD
        emission_profile_ledger = CustomDF("emission_profile_ledger_enriched", spark_generate)
        sector_profile_ledger = CustomDF("sector_profile_ledger_enriched", spark_generate)

        print(f"Preparing data for {table_name}")
        # PREPPING
        emission_profile_ledger = emission_profile_ledger.custom_select(["tiltledger_id", "benchmark_group", "risk_category", "average_profile_ranking", "product_name"])
        sector_profile_ledger = sector_profile_ledger.custom_select(["tiltledger_id","scenario_name", "year", "profile_ranking", "product_name"])

        # PREPPING
        sector_profile_ledger.data = sector_profile_ledger.data.withColumn(
                "scenario_year",
                F.lower(F.concat_ws("_", F.col("scenario_name"), F.col("year")))
            )
        sector_profile_ledger = sector_profile_ledger.custom_drop(["scenario_name", "year", "product_name"])

        # PREPPING
        trs_product = emission_profile_ledger.custom_join(sector_profile_ledger, custom_on="tiltledger_id", custom_how="inner")

        print(f"Calculating indicators for {table_name}")
        # CALCULATION
        trs_product.data = transition_risk_compute(trs_product.data)

        print(f"Preparing data for {table_name}")
        ## PREPPING
        trs_product = trs_product.custom_drop(["scenario_year", "risk_category"])

        ## PREPPING
        trs_product.rename_columns({"profile_ranking":"reductions"})

        ## PREPPING
        trs_product = trs_product.custom_select(["tiltledger_id", "benchmark_group", "average_profile_ranking","product_name", "reductions", "transition_risk_score"])

        # DF CREATION
        transition_risk_ledger_level = CustomDF("transition_risk_ledger_enriched", spark_generate,
                                                initial_df=trs_product.data)
        
        print(f"Writing data for {table_name}")
        # WRITE
        transition_risk_ledger_level.write_table()
        print("Data written successfully!\n")
    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()