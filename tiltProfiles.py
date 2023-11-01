from functions.processing import generate_table
# Old Sector Profiles
# generate_table('ictr_company_result_raw')
# generate_table('ictr_product_result_raw')
# generate_table('istr_company_result_raw')
# generate_table('istr_product_result_raw')
# generate_table('pctr_company_result_raw')
# generate_table('pctr_product_result_raw')
# generate_table('pstr_company_result_raw')
# generate_table('pstr_product_result_raw')

# New Sector Profiles
generate_table('emission_profile_company_raw')
generate_table('emission_profile_product_raw')
generate_table('emission_upstream_profile_productraw')
generate_table('emission_upstream_profile_company_raw')
generate_table('sector_profile_company_raw')
generate_table('sector_profile_product_raw')
generate_table('sector_upstream_profile_company_raw')
generate_table('sector_upstream_profile_product_raw')
