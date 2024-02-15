from functions.processing import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel


if __name__ == '__main__':
    # raw layer
    generate_table_raw('companies_europages_raw')

    # datamodel layer
    generate_table_datamodel('companies_datamodel')
    generate_table_datamodel('companies_products_datamodel')
    generate_table_datamodel('companies_products_datamodel')
    
