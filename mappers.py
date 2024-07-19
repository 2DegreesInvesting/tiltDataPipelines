from functions.processing_raw import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel

if __name__ == '__main__':

# raw layer

    generate_table_raw('sources_mapper_raw')
    generate_table_raw('countries_mapper_raw') 
    generate_table_raw('main_activity_ecoinvent_mapper_raw')

    generate_table_raw('country_raw')

    generate_table_raw('geography_ecoinvent_mapper_raw')
    generate_table_raw('EP_tilt_sector_unmatched_mapper_raw')
    generate_table_raw('tilt_sector_isic_mapper_raw')
    generate_table_raw('tilt_sector_scenario_mapper_raw') 

    generate_table_raw('isic_mapper_raw')


# data model layer

    generate_table_datamodel('sources_mapper_datamodel')
    generate_table_datamodel('main_activity_ecoinvent_mapper_datamodel')
    generate_table_datamodel('countries_mapper_datamodel') 
    generate_table_datamodel('geography_ecoinvent_mapper_datamodel')
    generate_table_datamodel('EP_tilt_sector_unmatched_mapper_datamodel')
    generate_table_datamodel('tilt_sector_isic_mapper_datamodel')
    generate_table_datamodel('tilt_sector_scenario_mapper_datamodel')

    generate_table_datamodel('isic_mapper_datamodel')


