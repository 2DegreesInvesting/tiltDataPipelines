from functions.processing import generate_table

generate_table('geographies_raw')

generate_table('undefined_ao_raw')
generate_table('cut_off_ao_raw')
generate_table('en15804_ao_raw')
generate_table('consequential_ao_raw')

generate_table('ecoinvent_products_datamodel')
generate_table('ecoinvent_activities_datamodel')
generate_table('ecoinvent_cut_off_datamodel')

generate_table('lcia_methods_raw')

generate_table('impact_categories_raw')

generate_table('intermediate_exchanges_raw')

generate_table('elementary_exchanges_raw')
