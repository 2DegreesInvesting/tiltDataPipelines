special_activity_type = ['market group', 'market activity', 'ordinary transforming activity', 'production mix']
unit_measures_ecoinvent = ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year', 'EUR2005', 'Sm3', 'kBq']

signalling_checks_dictionary = {
# checks on raw layer
    'undefined_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': special_activity_type
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'cut_off_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': special_activity_type
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'en15804_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': special_activity_type
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'consequential_ao_raw': [
        {
            'check': 'values within list',
            'columns': ['Special_Activity_Type'],
            'value_list': special_activity_type
        },
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'impact_categories_raw': [
        {
            'check': 'values within list',
            'columns': ['Unit'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'intermediate_exchanges_raw': [
        {
            'check': 'values within list',
            'columns': ['Unit_Name'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'elementary_exchanges_raw': [
        {
            'check': 'values within list',
            'columns': ['Unit_Name'],
            'value_list': unit_measures_ecoinvent
        }
    ],
    'companies_companyinfo_raw': [
        {
            'check': 'values have format',
            'columns': ['sbi_code'],
            'format': r'^\d{1,6}$'
        }
    ],
    # 'inputProducts_raw': [
    #     {
    #         'check': 'values are unique',
    #         'columns': ['Product_Name']
    #     }
    # ],
    # 'tiltledger_raw': [
    #     {
    #         'check': 'values within list',
    #         'columns': 'Activity_Type',
    #         'value_list': ['Transforming Activity', 'Market Activity']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': 'Geography',
    #         'value_list': ['NL', 'AT', 'IT', 'ES', 'FR', 'DE', 'GB']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': 'Manual_Review',
    #         'value_list': ['0', '1']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': 'Verified_Source',
    #         'value_list': ['0', '1']
    #     },
    #     {
    #         'check': 'values have format',
    #         'columns': ['CPC_Code'],
    #         'format': r'^\d{1,6}$'
    #     },
    #     {
    #         'check': 'values have format',
    #         'columns': ['ISIC_Code'],
    #         'format': r'^[A-Z]$|^\d{1,4}$'
    #     }
    # ],
# checks on datamodel layer
    'companies_datamodel': [
        {
            'check': 'values are unique',
            'columns': ['company_id']
        },
        {
            'check': 'values within list',
            'columns': ['country_un'],
            'value_list': ['NL', 'AT', 'IT', 'ES', 'FR', 'DE', 'GB']
        },
        {
            'check': 'values are consistent',
            'columns': ['company_description', 'address', 'company_city', 'postcode'],
            'compare_table': 'companies_companyinfo_raw',
            'compare_columns': ['Bedrijfsomschrijving', 'Vestigingsadres', 'Vestigingsadres_plaats', 'Vestigingsadres_postcode'],
            'join_columns': [('company_id', "Kamer_van_Koophandel_nummer_12_cijferig")]
        },
        {
            'check': 'values are consistent',
            'columns': ['company_description', 'address','company_city'],
            'compare_table': 'companies_europages_raw',
            'compare_columns': ['information', 'address', 'company_city'],
            'join_columns': [('company_id', "id")]
        } 
    ],
    'ecoinvent_product_datamodel': [
        {
            'check': 'values within list',
            'columns': ['unit'],
            'value_list': unit_measures_ecoinvent
        },
        {
            'check': 'values have format',
            'columns': ['cpc_code'],
            'format': r'^\d{1,6}$'
        }
    ],
    'ecoinvent_activity_datamodel': [
        {
            'check': 'values have format',
            'columns': ['isic_4digit'],
            'format': r'^[A-Z]$|^\d{1,4}$'
        }
    ],
    'SBI_activities_datamodel': [
        {
            'check': 'values have format',
            'columns': ['sbi_code'],
            'format': r'^\d{1,6}$'
        }
    ],
    'companies_SBI_activities_datamodel': [
        {
            'check': 'values have format',
            'columns': ['sbi_code'], 
            'format': r'^\d{1,6}$'
        }
    ],
    # 'isic_mapper_datamodel': [
    #     {
    #         'check': 'values have format',
    #         'columns': ['isic_4digit'],
    #         'format': r'^[A-Z]$|^\d{1,4}$'
    #     }
    # ],
    'tilt_sector_isic_mapper_datamodel': [
        {  
            'check': 'values within list',
            'columns': ['tilt_sector'],
            'value_list': ['Energy', 'Power', 'Industry', 'Land Use', 'Metals', 'Construction', 'Non-metallic Minerals', 'Transport', 'no_match']
        },
        {
            'check':'values within list',
            'columns':['tilt_subsector'],
            'value_list': ['Other Non-metallic Minerals','Construction Buildings','Food Related Products','Fishing & Forestry','Other Industry','Cement','Automotive LDV','Aviation','Iron & Steel', 'no_match','Other Metals','Machinery & Equipment','Other Transport','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        },
        {
            'check': 'values have format',
            'columns': ['isic_4digit'],
            'format': r'^[A-Z]$|^\d{1,4}$'
        }
    ],
    'tilt_sector_scenario_mapper_datamodel': [
        {  
            'check': 'values within list',
            'columns': ['tilt_sector'],
            'value_list': ['Energy', 'Power', 'Industry', 'Land Use', 'Metals', 'Construction', 'Non-metallic Minerals', 'Transport', 'no_match']
        },
        {
            'check':'values within list',
            'columns':['tilt_subsector'],
            'value_list': ['Other Non-metallic Minerals','Construction Buildings','Food Related Products','Fishing & Forestry','Other Industry','Cement','Automotive LDV','Aviation','Iron & Steel', 'no_match','Other Metals','Machinery & Equipment','Other Transport','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        },
        {
            'check': 'values within list',
            'columns': ['scenario_type'],
            'value_list': ['weo', 'ipr']
        }
    ],
    'tiltLedger_datamodel': [
        {
            'check': 'values have format',
            'columns': ['CPC_Code'],
            'format': r'^\d{1,6}$'
        },
        {
            'check': 'values have format',
            'columns': ['ISIC_Code'],
            'format': r'^[A-Z]$|^\d{1,4}$'
        },
        {
            'check': 'values within list',
            'columns': ['Activity_Type'],
            'value_list': ['Transforming Activity', 'Market Activity']
        },
        {
            'check': 'values within list',
            'columns': ['Geography'],
            'value_list': ['NL', 'AT', 'IT', 'ES', 'FR', 'DE', 'GB']
        },
        {
            'check': 'values within list',
            'columns': ['Manual_Review'],
            'value_list': ['0', '1']
        },
        {
            'check': 'values within list',
            'columns': ['Verified_Source'],
            'value_list': ['0', '1']
        },
    ],
    # 'emission_profile_company_raw': [
    #     {
    #         'check': 'values occur as expected',
    #         'columns': ['companies_id'],
    #         'expected_count': 18
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['country'],
    #         'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
    #     },
    #     {
    #         'check': 'values sum to 1',
    #         'columns': ['companies_id', 'benchmark'],
    #         'sum_column': 'emission_profile_share'
    #     },
    #     # {
    #     #     'check':'values sum to 1',
    #     #     'columns':['companies_id','benchmark'],
    #     #     'sum_column': 'emission_share_wc'
    #     # },
    #     # {
    #     #     'check':'values sum to 1',
    #     #     'columns':['companies_id','benchmark'],
    #     #     'sum_column': 'emission_share_bc'
    #     # },
    #     {
    #         'check': 'values within list',
    #         'columns': ['benchmark'],
    #         'value_list': ['all', 'isic_4digit', 'tilt_sec', 'unit', 'unit_tilt_sec', 'unit_isic_4digit']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['main_tilt_sector'],
    #     #     'value_list': ['Industry','Land Use','Metals','Construction','Non-metallic Minerals','Transport']
    #     # },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['main_tilt_sub_sector'],
    #     #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
    #     # },
    #     {
    #         'check': 'values are unique',
    #         'columns': ['companies_id', 'benchmark', 'matching_certainty_company_average', 'emission_profile', 'emission_profile_share']
    #     }, 
    # ],
    # 'emission_profile_product_raw': [
    #     {
    #         'check': 'values within list',
    #         'columns': ['emission_profile'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values occur as expected',
    #         'columns': ['companies_id', 'activity_uuid_product_uuid'],
    #         'expected_count': 6
    #     },
    #     {
    #         'check': 'distinct values occur as expected',
    #         'columns': ['companies_id', 'activity_uuid_product_uuid'],
    #         'distinct_columns': ['benchmark'],
    #         'expected_count': 6
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['unit'],
    #         'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['benchmark'],
    #         'value_list': ['all', 'isic_4digit', 'tilt_sec', 'unit', 'unit_tilt_sec', 'unit_isic_4digit']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['tilt_sector'],
    #         'value_list': ['industry', 'land use', 'metals', 'construction', 'non-metallic minerals', 'transport', 'no_match']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['tilt_subsector'],
    #         'value_list': ['other non-metallic minerals', 'agriculture & livestock', 'construction buildings', 'food related products', 'fishing & forestry', 'bioenergy & waste power', 'other industry', 'cement', 'bioenergy & waste energy', 'automotive ldv', 'renewable energy', 'iron & steel', 'aviation', 'oil energy', 'other metals', 'machinery & equipment', 'other transport', 'coal energy', 'construction residential', 'total power', 'chemicals', 'shipping', 'gas energy', 'total energy', 'automotive hdv', 'no_match']
    #     },
    #     {
    #         'check': 'distinct values occur as expected',
    #         'columns': ['activity_uuid_product_uuid'],
    #         'distinct_columns': ['tilt_sector'],
    #         'expected_count': 1
    #     },
    #     {
    #         'check': 'distinct values occur as expected',
    #         'columns': ['activity_uuid_product_uuid'],
    #         'distinct_columns': ['tilt_subsector'],
    #         'expected_count': 1
    #     },
    #     {
    #         'check': 'distinct values occur as expected',
    #         'columns': ['activity_uuid_product_uuid'],
    #         'distinct_columns': ['isic_4digit'],
    #         'expected_count': 1
    #     },
    #     { 
    #         'check': 'values are unique',
    #         'columns': ['companies_id', 'emission_profile', 'benchmark', 'isic_4digit', 'activity_uuid_product_uuid', 'ep_product']
    #     },
    # ],
    'emission_profile_ledger_enriched': [
        {
            'check': 'values within list',
            'columns': ['benchmark_group'],
            'value_list': ['isic_4digit', 'tilt_sector', 'unit', 'all', 'unit_tilt_sector', 'unit_isic_4digit']
        },
        {
            'check': 'values within list',
            'columns': ['risk_category'],
            'value_list': ['low', 'medium', 'high']
        },
    ],
    # 'emission_upstream_profile_company_raw': [
    #     {
    #         'check': 'values occur as expected',
    #         'columns': ['companies_id'],
    #         'expected_count': 18
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['country'],
    #         'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['emission_upstream_profile'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['benchmark'],
    #         'value_list': ['all', 'input_isic_4digit', 'input_tilt_sec', 'input_unit', 'input_unit_tilt_sec', 'input_unit_isic_4digit']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['main_tilt_sector'],
    #     #     'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
    #     # },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['main_tilt_sub_sector'],
    #     #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
    #     # },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
    #     },
    #     {
    #         'check': 'values are unique',
    #         'columns': ['companies_id', 'benchmark', 'emission_upstream_profile', 'matching_certainty_company_average', 'emission_upstream_profile_share']
    #     },
    # ],
    # 'emission_upstream_profile_product_raw': [
    #     {
    #         'check': 'values within list',
    #         'columns': ['country'],
    #         'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['benchmark'],
    #         'value_list': ['all', 'input_isic_4digit', 'input_tilt_sec', 'input_unit', 'input_unit_tilt_sec', 'input_unit_isic_4digit']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['input_unit'],
    #         'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     },
    #     {
    #         'check':'values are unique',
    #         'columns':['companies_id', 'emission_upstream_profile', 'ep_product', 'benchmark', 'matched_activity_name', 'matching_certainty_company_average', 'input_tilt_sector', 'input_tilt_subsector', 'input_isic_4digit', 'activity_uuid_product_uuid', 'reduction_targets', 'main_activity', 'input_name', 'input_unit']
    #     },
    # ],
    'emission_profile_ledger_upstream_enriched': [
        {
            'check': 'values within list',
            'columns': ['benchmark_group'],
            'value_list': ['input_unit', 'input_tilt_sector', 'input_unit_isic_4digit', 'input_unit_tilt_sector', 'input_isic_4digit', 'all']
        },
        {
            'check': 'values within list',
            'columns': ['risk_category'],
            'value_list': ['low', 'medium', 'high']
        },
    ],
    # 'sector_profile_company_raw': [
    #     {
    #         'check': 'values occur as expected',
    #         'columns': ['companies_id', 'scenario'],
    #         'expected_count': 6
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['country'],
    #         'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
    #     },
    #     {
    #         'check': 'values sum to 1',
    #         'columns': ['companies_id', 'scenario', 'year'],
    #         'sum_column': 'sector_profile_share'
    #     },
    #     # {
    #     #     'check':'values sum to 1',
    #     #     'columns':['companies_id','scenario','year'],
    #     #     'sum_column': 'sector_share_bc'
    #     # },
    #     # {
    #     #     'check':'values sum to 1',
    #     #     'columns':['companies_id','scenario','year'],
    #     #     'sum_column': 'sector_share_wc'
    #     # },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns': ['sector_category'],
    #     #     'value_list':['low','medium','high']
    #     # },
    #     {
    #         'check': 'values within list',
    #         'columns': ['scenario'],
    #         'value_list': ['NZ 2050', '1.5C RPS']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['year'],
    #         'value_list': ['2030', '2050']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high', None]
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['sector'],
    #     #     'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
    #     # },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['subsector'],
    #     #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
    #     # },
    #     {
    #         'check': 'values are unique',
    #         'columns': ['companies_id', 'scenario', 'sector_profile', 'matching_certainty_company_average', 'year']
    #     },
    # ],
    # 'sector_profile_product_raw': [
    #     {
    #         'check': 'values within list',
    #         'columns': ['country'],
    #         'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
    #     },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns': ['sector_category'],
    #     #     'value_list':['low','medium','high']
    #     # },
    #     {
    #         'check': 'values occur as expected',
    #         'columns': ['companies_id', 'ep_product', 'scenario'],
    #         'expected_count': 2
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['scenario'],
    #         'value_list': ['NZ 2050', '1.5C RPS']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['year'],
    #         'value_list': ['2030', '2050']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['tilt_sector'],
    #         'value_list': ['industry', 'land use', 'metals', 'no_match', 'construction', 'non-metallic minerals', 'transport']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['tilt_subsector'],
    #         'value_list': ['other non-metallic minerals', 'agriculture & livestock', 'construction buildings', 'food related products', 'fishing & forestry', 'bioenergy & waste power', 'other industry', 'cement', 'bioenergy & waste energy', 'automotive ldv', 'renewable energy', 'iron & steel', 'aviation', 'no_match', 'oil energy', 'other metals', 'machinery & equipment', 'other transport', 'coal energy', 'construction residential', 'total power', 'chemicals', 'shipping', 'gas energy', 'total energy', 'automotive hdv']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty'],
    #         'value_list': ['low', 'medium', 'high', None]
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high', None]
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler','missing']
    #     },
    #     {
    #         'check': 'values are unique',
    #         'columns': ['companies_id', 'sector_profile', 'scenario', 'year', 'ep_product', 'activity_uuid_product_uuid']
    #     },
    # ],
    'sector_profile_ledger_enriched': [
        {
            'check': 'values within list',
            'columns': ['benchmark_group'],
            'value_list': ['weo_nz 2050_2030', 'weo_nz 2050_2050', 'ipr_1.5c rps_2030', 'ipr_1.5c rps_2050']
        },
        {
            'check': 'values within list',
            'columns': ['risk_category'],
            'value_list': ['low', 'medium', 'high']
        },
        {   'check': 'values within list',
            'columns': ['tilt_sector'],
            'value_list': ['Energy', 'Power', 'Industry', 'Land Use', 'Metals', 'Construction', 'Non-metallic Minerals', 'Transport']
        }, 
        {   'check': 'values within list',
            'columns': ['scenario_name'],
            'value_list': ['1.5C RPS', 'NZ 2050']
        },
        {   'check': 'values within list',
            'columns': ['scenario_type'],
            'value_list': ['ipr', 'weo']
        },
        {   'check': 'values within list',
            'columns': ['year'],
            'value_list': ['2030', '2050']
        },
    ],
    # 'sector_upstream_profile_company_raw': [
    #     {
    #         'check': 'values sum to 1',
    #         'columns': ['companies_id', 'scenario', 'year'],
    #         'sum_column': 'sector_profile_upstream_share'
    #     },
    #     # {
    #     #     'check':'values sum to 1',
    #     #     'columns':['companies_id','scenario','year'],
    #     #     'sum_column': 'sector_upstream_share_bc'
    #     # },
    #     # {
    #     #     'check':'values sum to 1',
    #     #     'columns':['companies_id','scenario','year'],
    #     #     'sum_column': 'sector_upstream_share_wc'
    #     # },
    #     {
    #         'check': 'values within list',
    #         'columns': ['sector_profile_upstream'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['scenario'],
    #         'value_list': ['NZ 2050', '1.5C RPS']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['year'],
    #         'value_list': ['2030', '2050']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['matching_certainty_company_average'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['main_tilt_sector'],
    #     #     'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
    #     # },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['main_tilt_sub_sector'],
    #     #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
    #     # },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     },
    #     {
    #         'check':'values are unique',
    #         'columns':['companies_id', 'sector_profile_upstream', 'scenario', 'year']
    #     },
    # ],
    # 'sector_upstream_profile_product_raw': [
    #     {
    #         'check': 'values within list',
    #         'columns': ['country'],
    #         'value_list': ['netherlands', 'germany', 'spain', 'france', 'austria']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['sector_profile_upstream'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['scenario'],
    #         'value_list': ['NZ 2050', '1.5C RPS']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['year'],
    #         'value_list': ['2030', '2050']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['tilt_sector'],
    #         'value_list': ['industry', 'land use', 'metals', 'no_match', 'construction', 'non-metallic minerals', 'transport']
    #     },
    #     # {
    #     #     'check':'values within list',
    #     #     'columns':['tilt_subsector'],
    #     #     'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
    #     # },
    #     {
    #         'check': 'values within list',
    #         'columns': ['main_activity'],
    #         'value_list': ['agent/ representative', 'distributor', 'manufacturer/ producer', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['unit'],
    #         'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['input_unit'],
    #         'value_list': ['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['input_tilt_sector'],
    #         'value_list': ['industry', 'land use', 'metals', 'no_match', 'construction', 'non-metallic minerals', 'transport']
    #     },
    #     {
    #         'check':'values within list',
    #         'columns':['input_tilt_subsector'],
    #         'value_list': ['other non-metallic minerals','agriculture & livestock','construction buildings','food related products','fishing & forestry','bioenergy & waste power','other industry','cement','bioenergy & waste energy','automotive ldv','renewable energy','iron & steel','aviation','no_match','oil energy','other metals','machinery & equipment','other transport','coal energy','construction residential','total power','chemicals','shipping','gas energy','total energy','automotive hdv']
    #     },
    #     { 
    #         'check':'values are unique',
    #         'columns':['companies_id', 'sector_profile_upstream', 'scenario', 'year', 'activity_uuid_product_uuid', 'reduction_targets', 'input_name', 'input_unit', 'ep_product']
    #     },
    # ],
    # 'ep_ei_matcher_raw': [
    #     {
    #         'check': 'values within list',
    #         'columns': ['ep_main_act'],
    #         'value_list': ['manufacturer/ producer', 'distributor', 'wholesaler', 'agent/ representative', 'retailer']
    #     },
    #     {
    #         'check': 'values within list',
    #         'columns': ['completion'],
    #         'value_list': ['low', 'medium', 'high']
    #     },
    #     {
    #         'check': 'values are unique',
    #         'columns': ['group_var']
    #     },
    # ],
    'transition_risk_ledger_enriched': [
        {
            'check': 'values within list',
            'columns': ['benchmark_group'],
            'value_list': ['nz 2050_2050_tilt_sector', 'nz 2050_2030_all', 'nz 2050_2030_unit_isic_4digit', 'nz 2050_2030_isic_4digit', 'nz 2050_2050_unit', 'nz 2050_2050_isic_4digit', 'nz 2050_2050_unit_isic_4digit', '1.5c rps_2050_unit', 'nz 2050_2050_unit_tilt_sector',
                           '1.5c rps_2050_all', '1.5c rps_2030_unit_isic_4digit', 'nz 2050_2030_unit_tilt_sector', '1.5c rps_2050_unit_isic_4digit', 'nz 2050_2030_tilt_sector', '1.5c rps_2050_tilt_sector', '1.5c rps_2030_isic_4digit', 'nz 2050_2030_unit', 
                           'nz 2050_2050_all', '1.5c rps_2050_isic_4digit', '1.5c rps_2050_unit_tilt_sector', '1.5c rps_2030_unit', '1.5c rps_2030_unit_tilt_sector', '1.5c rps_2030_all', '1.5c rps_2030_tilt_sector']
        }
    ],
    'sector_profile_ledger_upstream_enriched': [
        {
            'check': 'values within list',
            'columns': ['benchmark_group'],
            'value_list': ['weo_nz 2050_2030', 'weo_nz 2050_2050', 'ipr_1.5c rps_2030', 'ipr_1.5c rps_2050']
        },
        {
            'check': 'values within list',
            'columns': ['risk_category'],
            'value_list': ['low', 'medium', 'high']
        }, 
        {   'check': 'values within list',
            'columns': ['scenario_name'],
            'value_list': ['1.5C RPS', 'NZ 2050']
        },
        {   'check': 'values within list',
            'columns': ['scenario_type'],
            'value_list': ['ipr', 'weo']
        },
        {   'check': 'values within list',
            'columns': ['year'],
            'value_list': ['2030', '2050']
        },
    ],
    'company_product_indicators_enriched': [
        {
            'check': 'values are unique',
            'columns': ['company_id']
        },
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['IT', 'FR', 'GB', 'NL', 'ES', 'DE', 'AT']
        },
        {
            'check': 'value vithin list',
            'columns': ['benchmark'],
            'value_list': ['input_unit', 'isic_4digit', 'input_tilt_sector', 'weo_nz 2050_2030', 'input_unit_isic_4digit', 'ipr_1.5c rps_2030', 'ipr_1.5c rps_2050', 'input_unit_tilt_sector', 'tilt_sector', 'unit', 'input_isic_4digit', 'unit_tilt_sector', 'all', 'weo_nz 2050_2030', 'unit_isic_4digit']
        },
        {
            'check': 'value vithin list',
            'columns': ['score'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check':'values within list',
            'columns':['tilt_sector'],
            'value_list': ['Energy', 'Power','Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
        },
        {
            'check':'values within list',
            'columns':['tilt_subsector'],
            'value_list': ['Other Non-metallic Minerals','Construction Buildings','Food Related Products','Fishing & Forestry','Other Industry','Cement','Automotive LDV','Aviation','Iron & Steel', 'no_match','Other Metals','Machinery & Equipment','Other Transport','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        },
        {
            'check': 'values within list',
            'columns': ['activity_type'],
            'value_list': ['Transforming Activity', 'Market Activity']
        },
        {
            'check': 'values within list',
            'columns': ['geography'],
            'value_list': ['IT', 'FR', 'GB', 'NL', 'ES', 'DE', 'AT']
        },
        {
            'check': 'values have format',
            'columns': ['CPC_Code'],
            'format': r'^\d{1,6}$'
        },
        {
            'check': 'values have format',
            'columns': ['ISIC_Code'],
            'format': r'^[A-Z]$|^\d{1,4}$'
        },
        {
            'check': 'values within list',
            'columns': ['Indicator'],
            'value_list': ['EPU', 'EP', 'SP', 'SPU']
        },
        {
            'check': 'values in range',
            'columns': ['data_granularity'],
            'range_start': 1,
            'range_end': 5
        },
        {
            'check': 'values in range',
            'columns': ['model_certainty'],
            'range_start': 1,
            'range_end': 5
        },
        {
            'check': 'values in range',
            'columns': ['data_source_reliability'],
            'range_start': 1,
            'range_end': 5
        }
    ],
    'company_indicators_enriched': [
        {
            'check': 'values are unique',
            'columns': ['company_id']
        },
        {
            'check': 'values within list',
            'columns': ['country'],
            'value_list': ['IT', 'FR', 'GB', 'NL', 'ES', 'DE', 'AT']
        },
        {
            'check': 'value vithin list',
            'columns': ['benchmark'],
            'value_list': ['input_unit', 'isic_4digit', 'input_tilt_sector', 'weo_nz 2050_2030', 'input_unit_isic_4digit', 'ipr_1.5c rps_2030', 'ipr_1.5c rps_2050', 'input_unit_tilt_sector', 'tilt_sector', 'unit', 'input_isic_4digit', 'unit_tilt_sector', 'all', 'weo_nz 2050_2030', 'unit_isic_4digit']
        },
        {
            'check': 'value vithin list',
            'columns': ['company_score'],
            'value_list': ['low', 'medium', 'high']
        },
        {
            'check': 'values within list',
            'columns': ['Indicator'],
            'value_list': ['EPU', 'EP', 'SP', 'SPU']
        },
        {
            'check': 'values in range',
            'columns': ['data_granularity'],
            'range_start': 1,
            'range_end': 5
        },
        {
            'check': 'values in range',
            'columns': ['model_certainty'],
            'range_start': 1,
            'range_end': 5
        },
        {
            'check': 'values in range',
            'columns': ['data_source_reliability'],
            'range_start': 1,
            'range_end': 5
        }
    ]
}
