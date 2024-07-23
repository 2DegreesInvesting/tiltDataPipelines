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

    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()
