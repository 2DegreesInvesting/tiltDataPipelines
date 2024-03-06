from functions.processing_raw import generate_table

if __name__ == '__main__':
    # New Sector Profiles
    generate_table('emission_profile_company_raw')
    generate_table('emission_profile_product_raw')
    generate_table('emission_upstream_profile_product_raw')
    generate_table('emission_upstream_profile_company_raw')
    generate_table('sector_profile_company_raw')
    generate_table('sector_profile_product_raw')
    generate_table('sector_upstream_profile_company_raw')
    generate_table('sector_upstream_profile_product_raw')
