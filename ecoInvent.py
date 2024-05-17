"""
ecoInvent.py

This module is part of a larger project that processes and transforms data from the ecoInvent database.

The module calls the `generate_table` function from the `processing` module to generate various tables from raw data. 
The tables generated include 'geographies_raw', 'geographies_transform', 'undefined_ao_raw', 'cut_off_ao_raw', 
'en15804_ao_raw', 'consequential_ao_raw', 'products_activities_transformed', 'lcia_methods_raw', 
'impact_categories_raw', 'intermediate_exchanges_raw', and 'elementary_exchanges_raw'.

Each call to `generate_table` generates a specific table by filtering and transforming the raw data as necessary. 
The specific transformations performed depend on the table being generated.

This module is intended to be run as a standalone script and does not return any values.

Note:
    - The module uses the 'CustomDF' class to handle DataFrames.
    - The module writes the generated tables to disk using the 'write_table' method of the 'CustomDF' class.
"""
from functions.processing_raw import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel

if __name__ == '__main__':

# raw layer
    # generate_table_raw('geographies_raw')

    # generate_table_raw('undefined_ao_raw')
    # generate_table_raw('cut_off_ao_raw')
    # generate_table_raw('en15804_ao_raw')
    # generate_table_raw('consequential_ao_raw')

    # generate_table_raw('lcia_methods_raw')
    # generate_table_raw('impact_categories_raw')
    # generate_table_raw('intermediate_exchanges_raw')
    # generate_table_raw('elementary_exchanges_raw')

    # generate_table_raw('ecoinvent_co2_raw') 

    # generate_table_raw('ecoinvent_input_data_raw') 


# datamodel layer
    
    # from cut_off
    # generate_table_datamodel('ecoinvent_cut_off_datamodel') 
    generate_table_datamodel('ecoinvent_product_datamodel')
    # generate_table_datamodel('ecoinvent_activity_datamodel')

    # generate_table_datamodel('intermediate_exchanges_datamodel')

    # generate_table_datamodel('ecoinvent_co2_datamodel') 

    # generate_table_datamodel('ecoinvent_input_data_datamodel') 

