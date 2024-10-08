from functions.processing_raw import generate_table as generate_table_raw
from functions.processing_datamodel import generate_table as generate_table_datamodel


if __name__ == "__main__":
    # raw layer
    generate_table_raw("tiltledger_embedding_raw")

    generate_table_raw("companies_embedding_raw")
    generate_table_raw("companies_companyinfo_raw")
    generate_table_raw("country_raw")

    # company_info SBI activities
    generate_table_datamodel("SBI_activities_raw")

    # datamodel layer
    generate_table_datamodel("companies_datamodel")

    generate_table_datamodel("companies_match_result_datamodel")

    # company_info SBI activities
    generate_table_datamodel("SBI_activities_datamodel")
    generate_table_datamodel("companies_SBI_activities_datamodel")

    # Europages products
