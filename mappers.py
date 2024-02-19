from functions.processing import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel

if __name__ == '__main__':

# raw layer
    # main_activity_ecoinvent_mapper - find original source/is this table ever used?

    generate_table_raw('sources_mapper_raw')
    # generate_table_raw('countries_mapper_raw') error - null constraint is violated

    generate_table_raw('geography_ecoinvent_mapper_raw')

    generate_table_raw('tilt_sector_isic_mapper_raw')

    # isic_mapper - find online version of the full list + other codes

    generate_table_raw('tilt_scenario_mapper_raw') 

    generate_table_raw('scenario_targets_IPR_raw')
    generate_table_raw('scenario_targets_WEO_raw')


# data model layer
    
    generate_table_datamodel('sources_mapper_datamodel')
    generate_table_datamodel('geography_ecoinvent_mapper_datamodel')
    generate_table_datamodel('tilt_sector_isic_mapper_datamodel')
    generate_table_datamodel('tilt_scenario_mapper_datamodel')

