from functions.processing import generate_table

generate_table('geographies_raw')
generate_table('geographies_transform')

generate_table('unindentified_ao_raw')
generate_table('cut_off_ao_raw')
generate_table('en15804_ao_raw')
generate_table('consequential_ao_raw')
generate_table('products_activities_transformed')

generate_table('lcia_methods_raw')
generate_table('lcia_methods_transform')

generate_table('impact_categories_raw')
generate_table('impact_categories_transform')

generate_table('intermediate_exchanges_raw')
generate_table('intermediate_exchanges_transform')

generate_table('elementary_exchanges_raw')
generate_table('elementary_exchanges_transform')

