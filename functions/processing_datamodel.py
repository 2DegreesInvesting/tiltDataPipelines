import os
import pyspark.sql.functions as F
from functions.custom_dataframes import CustomDF
from functions.spark_session import create_spark_session


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

    # Ecoinvent data

    if table_name == 'intermediate_exchanges_datamodel':

        intermediate_exchanges_raw = CustomDF('intermediate_exchanges_raw', spark_generate)

        intermediate_exchanges_datamodel = CustomDF(
            'intermediate_exchanges_datamodel', spark_generate, initial_df=intermediate_exchanges_raw.data.select('ID', 'Name', 'Unit_Name'))
        
        # Define a dictionary to map old column names to new column names
        rename_dict = {"ID": "exchange_id", "Name": "exchange_name", "Unit_Name": "unit_name"}

        intermediate_exchanges_datamodel.rename_columns(rename_dict)

        intermediate_exchanges_datamodel.write_table()

    # elif table_name == 'ecoinvent_co2_datamodel':

    #     ecoinvent_co2_raw = CustomDF('ecoinvent_co2_raw', spark_generate)
        
    #     ecoinvent_co2_datamodel = CustomDF(
    #         'ecoinvent_co2_datamodel', spark_generate, initial_df=ecoinvent_co2_raw.data
    #         .select('Activity_UUID_Product_UUID', 'IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq'))
        
    #     rename_dict = {"Activity_UUID_Product_UUID": "activity_uuid_product_uuid",
    #                    "IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq": "co2_footprint"}

        # ecoinvent_co2_datamodel.rename_columns(rename_dict)

        # ecoinvent_co2_datamodel.write_table()

    # elif table_name == 'ecoinvent_cut_off_datamodel':

    #     cut_off_ao_raw = CustomDF('cut_off_ao_raw', spark_generate)

    #     relational_list = ['Activity_UUID_&_Product_UUID',
    #                        'Activity_UUID', 'Product_UUID']

    #     cut_off_ao_raw.data = cut_off_ao_raw.data.select(relational_list)

    #     ecoinvent_cutoff_datamodel = CustomDF(
    #         'ecoinvent_cut_off_datamodel', spark_generate, cut_off_ao_raw.data)
        
    #     rename_dict = {"Activity_UUID_&_Product_UUID": "activity_uuid_product_uuid",
    #                    "Activity_UUID": "activity_uuid",
    #                    "Product_UUID": "product_uuid"}

    #     ecoinvent_cutoff_datamodel.rename_columns(rename_dict)

    #     ecoinvent_cutoff_datamodel.write_table()

    elif table_name == 'ecoinvent_product_datamodel':

        cut_off_ao_raw = CustomDF('cut_off_ao_raw', spark_generate)

        ecoinvent_product_datamodel = CustomDF(
            'ecoinvent_product_datamodel', spark_generate, initial_df=cut_off_ao_raw.data
            .select('Product_UUID', 'Reference_Product_Name', 'Unit').distinct())
        
        rename_dict = {"Product_UUID": "product_uuid", "Reference_Product_Name": "reference_product_name", 'Unit': 'unit'}

        ecoinvent_product_datamodel.rename_columns(rename_dict)

        ecoinvent_product_datamodel.write_table()

    elif table_name == 'ecoinvent_activity_datamodel':

        cut_off_ao_raw = CustomDF('cut_off_ao_raw', spark_generate)

        ecoinvent_activity_datamodel = CustomDF(
            'ecoinvent_activity_datamodel', spark_generate, initial_df=cut_off_ao_raw.data
            .select('Activity_UUID', 'Activity_Name', 'Geography', 'ISIC_Classification', 'ISIC_Section').distinct())
        
        rename_dict = {"Activity_UUID": "activity_uuid", "Activity_Name": "activity_name", 'Geography': 'geography',
                       "ISIC_Classification": "isic_classification", "ISIC_Section": "isic_section"}
        
        ecoinvent_activity_datamodel.rename_columns(rename_dict)

        ecoinvent_activity_datamodel.write_table()

    # elif table_name == 'ecoinvent_input_data_datamodel':

    #     ecoinvent_input_data_raw = CustomDF('ecoinvent_input_data_raw', spark_generate)

    #     ecoinvent_input_data_datamodel = CustomDF(
    #         'ecoinvent_input_data_datamodel', spark_generate, initial_df=ecoinvent_input_data_raw.data
    #         .select('activityId', 'activityName', 'geography', 'reference_product', 'group', 'exchange_name',
    #                'activityLinkId', 'activityLink_activityName', 'activityLink_geography', 'exchange_unitName'))
    
    #     ecoinvent_input_data_datamodel.write_table()

    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')

    # If the code is run as a workflow on databricks, we do not want to shutdown the spark session.
    # This will cause the cluster to be unusable for other spark processes
    if not 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        spark_generate.stop()