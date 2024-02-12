signalling_checks_dictionary = {
    'undefined_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': ['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'cut_off_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': ['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'en15804_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': ['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'consequential_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': ['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'ecoinvent_products_datamodel': [
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'impact_categories_raw': [
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'intermediate_exchanges_raw': [
        {
            'check': 'values within list',
            'columns': ['Unit_Name'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'elementary_exchanges_raw': [
        {
            'check': 'values within list',
            'columns': ['Unit_Name'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'ecoinvent-v3.9.1_raw': [{
        'check': 'values are unique',
        'columns': ['activity_name']
    }],
    'emission_profile_company_raw': [
        {
            'check': 'values occur as expected',
            'columns': ['companies_id'],
            'expected_count': 18
        },
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
        },
        {
            'check': 'values sum to 1',
            'columns': ['companies_id', 'benchmark'],
            'sum_column': 'emission_profile_share'
        },
        # {
        #     'check':'values sum to 1',
        #     'columns':['companies_id','benchmark'],
        #     'sum_column': 'emission_share_wc'
        # },
        # {
        #     'check':'values sum to 1',
        #     'columns':['companies_id','benchmark'],
        #     'sum_column': 'emission_share_bc'
        # },
        {
            'check': 'values within list',
            'columns': ['benchmark'],
            'value_list': ['all', 'isic_4digit', 'tilt_sec', 'unit', 'unit_tilt_sec', 'unit_isic_4digit']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
        },
        # {
        #     'check':'values within list',
        #     'columns':['main_tilt_sector'],
        #     'value_list': ['Industry','Land Use','Metals','Construction','Non-metallic Minerals','Transport']
        # },
        # {
        #     'check':'values within list',
        #     'columns':['main_tilt_sub_sector'],
        #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        # },
        {
            'check': 'values are unique',
            'columns': ['companies_id', 'benchmark', 'matching_certainty_company_average', 'emission_profile', 'emission_profile_share']
        }, 
    ],
    'emission_profile_product_raw': [
        {
            'check': 'values within list',
            'columns': ['emission_profile'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values occur as expected',
            'columns': ['companies_id', 'activity_uuid_product_uuid'],
            'expected_count': 6
        },
        {
            'check': 'distinct values occur as expected',
            'columns': ['companies_id', 'activity_uuid_product_uuid'],
            'distinct_columns': ['benchmark'],
            'expected_count': 6
        },
        {
            'check': 'values within list',
            'columns': ['unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['benchmark'],
            'value_list': ['all', 'isic_4digit', 'tilt_sec', 'unit', 'unit_tilt_sec', 'unit_isic_4digit']
        },
        {
            'check': 'values within list',
            'columns': ['tilt_sector'],
            'value_list': ['industry', 'land use', 'metals', 'construction', 'non-metallic minerals', 'transport']
        },
        {
            'check': 'values within list',
            'columns': ['tilt_subsector'],
            'value_list': ['other non-metallic minerals', 'agriculture & livestock', 'construction buildings', 'food related products', 'fishing & forestry', 'bioenergy & waste power', 'other industry', 'cement', 'bioenergy & waste energy', 'automotive ldv', 'renewable energy', 'iron & steel', 'aviation', 'oil energy', 'other metals', 'machinery & equipment', 'other transport', 'coal energy', 'construction residential', 'total power', 'chemicals', 'shipping', 'gas energy', 'total energy', 'automotive hdv']
        },
        {
            'check': 'distinct values occur as expected',
            'columns': ['activity_uuid_product_uuid'],
            'distinct_columns': ['tilt_sector'],
            'expected_count': 1
        },
        {
            'check': 'distinct values occur as expected',
            'columns': ['activity_uuid_product_uuid'],
            'distinct_columns': ['tilt_subsector'],
            'expected_count': 1
        },
        {
            'check': 'distinct values occur as expected',
            'columns': ['activity_uuid_product_uuid'],
            'distinct_columns': ['isic_4digit'],
            'expected_count': 1
        },
        { 
            'check': 'values are unique',
            'columns': ['companies_id', 'emission_profile', 'benchmark', 'isic_4digit', 'activity_uuid_product_uuid', 'ep_product']
        },
    ],
    'emission_upstream_profile_company_raw': [
        {
            'check': 'values occur as expected',
            'columns': ['companies_id'],
            'expected_count': 18
        },
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
        },
        {
            'check': 'values within list',
            'columns': ['emission_upstream_profile'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['benchmark'],
            'value_list': ['all', 'input_isic_4digit', 'input_tilt_sec', 'input_unit', 'input_unit_tilt_sec', 'input_unit_isic_4digit']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high']
        },
        # {
        #     'check':'values within list',
        #     'columns':['main_tilt_sector'],
        #     'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
        # },
        # {
        #     'check':'values within list',
        #     'columns':['main_tilt_sub_sector'],
        #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        # },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
        {
            'check': 'values are unique',
            'columns': ['companies_id', 'benchmark', 'emission_upstream_profile', 'matching_certainty_company_average', 'emission_upstream_profile_share']
        },
    ],
    'emission_upstream_profile_product_raw': [
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
        },
        {
            'check': 'values within list',
            'columns': ['benchmark'],
            'value_list': ['all', 'input_isic_4digit', 'input_tilt_sec', 'input_unit', 'input_unit_tilt_sec', 'input_unit_isic_4digit']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['input_unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
        },
        {
            'check':'values are unique',
            'columns':['companies_id', 'emission_upstream_profile', 'ep_product', 'benchmark', 'matched_activity_name', 'matching_certainty_company_average', 'input_tilt_sector', 'input_tilt_subsector', 'input_isic_4digit', 'activity_uuid_product_uuid', 'reduction_targets', 'main_activity', 'input_name', 'input_unit']
        },
    ],
    'sector_profile_company_raw': [
        {
            'check': 'values occur as expected',
            'columns': ['companies_id', 'scenario'],
            'expected_count': 6
        },
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
        },
        {
            'check': 'values sum to 1',
            'columns': ['companies_id', 'scenario', 'year'],
            'sum_column': 'sector_profile_share'
        },
        # {
        #     'check':'values sum to 1',
        #     'columns':['companies_id','scenario','year'],
        #     'sum_column': 'sector_share_bc'
        # },
        # {
        #     'check':'values sum to 1',
        #     'columns':['companies_id','scenario','year'],
        #     'sum_column': 'sector_share_wc'
        # },
        # {
        #     'check':'values within list',
        #     'columns': ['sector_category'],
        #     'value_list':['low','medium','high']
        # },
        {
            'check': 'values within list',
            'columns': ['scenario'],
            'value_list': ['NZ 2050', '1.5C RPS']
        },
        {
            'check': 'values within list',
            'columns': ['year'],
            'value_list': ['2030', '2050']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high', None]
        },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
        },
        # {
        #     'check':'values within list',
        #     'columns':['sector'],
        #     'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
        # },
        # {
        #     'check':'values within list',
        #     'columns':['subsector'],
        #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        # },
        {
            'check': 'values are unique',
            'columns': ['companies_id', 'scenario', 'sector_profile', 'matching_certainty_company_average', 'year']
        },
    ],
    'sector_profile_product_raw': [
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
        },
        # {
        #     'check':'values within list',
        #     'columns': ['sector_category'],
        #     'value_list':['low','medium','high']
        # },
        {
            'check': 'values occur as expected',
            'columns': ['companies_id', 'ep_product', 'scenario'],
            'expected_count': 2
        },
        {
            'check': 'values within list',
            'columns': ['scenario'],
            'value_list': ['NZ 2050', '1.5C RPS']
        },
        {
            'check': 'values within list',
            'columns': ['year'],
            'value_list': ['2030', '2050']
        },
        {
            'check': 'values within list',
            'columns': ['tilt_sector'],
            'value_list': ['industry', 'land Use', 'metals', 'no_match', 'construction', 'non-metallic minerals', 'transport']
        },
        {
            'check': 'values within list',
            'columns': ['tilt_subsector'],
            'value_list': ['other non-metallic minerals', 'agriculture & livestock', 'construction buildings', 'food related products', 'fishing & forestry', 'bioenergy & waste power', 'other industry', 'cement', 'bioenergy & waste energy', 'automotive ldv', 'renewable energy', 'iron & steel', 'aviation', 'no_match', 'oil energy', 'other metals', 'machinery & equipment', 'other transport', 'coal energy', 'construction residential', 'total power', 'chemicals', 'shipping', 'gas energy', 'Total Energy', 'Automotive HDV']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty'],
            'value_list': ['low', 'medium', 'high', None]
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high', None]
        },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler','missing']
        },
        {
            'check': 'values are unique',
            'columns': ['companies_id', 'sector_profile', 'scenario', 'year', 'ep_product', 'activity_uuid_product_uuid']
        },
    ],
    'sector_upstream_profile_company_raw': [
        {
            'check': 'values sum to 1',
            'columns': ['companies_id', 'scenario', 'year'],
            'sum_column': 'sector_profile_upstream_share'
        },
        # {
        #     'check':'values sum to 1',
        #     'columns':['companies_id','scenario','year'],
        #     'sum_column': 'sector_upstream_share_bc'
        # },
        # {
        #     'check':'values sum to 1',
        #     'columns':['companies_id','scenario','year'],
        #     'sum_column': 'sector_upstream_share_wc'
        # },
        {
            'check': 'values within list',
            'columns': ['sector_profile_upstream'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['scenario'],
            'value_list': ['NZ 2050', '1.5C RPS']
        },
        {
            'check': 'values within list',
            'columns': ['year'],
            'value_list': ['2030', '2050']
        },
        {
            'check': 'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list': ['low', 'medium', 'high']
        },
        # {
        #     'check':'values within list',
        #     'columns':['main_tilt_sector'],
        #     'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
        # },
        # {
        #     'check':'values within list',
        #     'columns':['main_tilt_sub_sector'],
        #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        # },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
        },
        {
            'check':'values are unique',
            'columns':['companies_id', 'sector_profile_upstream', 'scenario', 'year']
        },
    ],
    'sector_upstream_profile_product_raw': [
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
        },
        {
            'check': 'values within list',
            'columns': ['sector_profile_upstream'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['scenario'],
            'value_list': ['NZ 2050', '1.5C RPS']
        },
        {
            'check': 'values within list',
            'columns': ['year'],
            'value_list': ['2030', '2050']
        },
        {
            'check': 'values within list',
            'columns': ['tilt_sector'],
            'value_list': ['industry', 'land use', 'metals', 'no_match', 'construction', 'non-metallic minerals', 'transport']
        },
        # {
        #     'check':'values within list',
        #     'columns':['tilt_subsector'],
        #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        # },
        {
            'check': 'values within list',
            'columns': ['main_activity'],
            'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
        },
        {
            'check': 'values within list',
            'columns': ['unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
        {
            'check': 'values within list',
            'columns': ['input_unit'],
            'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
        {
            'check': 'values within list',
            'columns': ['input_tilt_sector'],
            'value_list': ['Industry', 'Land Use', 'Metals', 'no_match', 'Construction', 'Non-metallic Minerals', 'Transport']
        },
        {
            'check':'values within list',
            'columns':['input_tilt_subsector'],
            'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        },
        { 
            'check':'values are unique',
            'columns':['companies_id', 'sector_profile_upstream', 'scenario', 'year', 'activity_uuid_product_uuid', 'reduction_targets', 'input_name', 'input_unit', 'ep_product']
        },
    ],
    'ep_ei_matcher_raw': [
        {
            'check': 'values within list',
            'columns': ['ep_main_act'],
            'value_list': ['manufacturer/ producer', 'distributor', 'wholesaler', 'agent/ representative', 'retailer']
        },
        {
            'check': 'values within list',
            'columns': ['completion'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values are unique',
            'columns': ['group_var']
        },
    ],
}
