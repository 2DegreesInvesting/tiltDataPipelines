from functions.processing import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel

if __name__ == '__main__':

# raw layer
    
    generate_table_raw('scenario_targets_IPR_raw')
    generate_table_raw('scenario_targets_WEO_raw')

# data model layer
    
    generate_table_datamodel('scenario_targets_IPR_datamodel')
    generate_table_datamodel('scenario_targets_WEO_datamodel')