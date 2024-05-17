from functions.processing_raw import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel


if __name__ == '__main__':
    # raw layer
    # generate_table_raw('companies_europages_raw')
    # generate_table_raw('country_raw')
    # generate_table_raw("mapper_ep_ei_raw")

    # datamodel layer
    # generate_table_datamodel('companies_datamodel')
    generate_table_datamodel('products_datamodel')
    # generate_table_datamodel('companies_products_datamodel')


    
