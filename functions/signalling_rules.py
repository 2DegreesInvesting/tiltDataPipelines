signalling_checks_dictionary = {
    'companies_raw': [
        {
            'check': 'values in range',
            'columns':['year_established'],
            'range_start' : 1800,
            'range_end': 2023
        },
        {
            'check': 'values have format',
            'columns':['year_established'],
            'format' : '[0-9]{4}'
        },
        {
            'check': 'values are consistent',
            'columns':['company_name'],
            'compare_table':'companies_raw',
            'join_columns':['companies_id']
        },
        {
            'check': 'values are unique',
            'columns':['company_name']
        }
        ],
    'main_activity_raw' : [{
            'check':'values within list',
            'columns':['main_activity'],
            'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    },{
            'check':'values are unique',
            'columns':['main_activity_id']
    }],
    'undefined_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check':'values within list',
            'columns':['Unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'cut_off_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        },
         {
            'check':'values within list',
            'columns':['Unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
      'en15804_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check':'values within list',
            'columns':['Unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'consequential_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        },
        {
            'check':'values within list',
            'columns':['Unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'activities_transformed' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
     'country_raw' : [
        {
            'check':'values within list',
            'columns':['country'],
            'value_list':['AT', 'DE', 'FR', 'NL', 'ES', 'BE', 'PL', 'LU', 'RO', 'TR']
        }
    ],
     'products_transformed' : [
        {
            'check':'values within list',
            'columns':['Unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
     'impact_categories_raw' : [
        {
            'check':'values within list',
            'columns':['Unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
     'intermediate_exchanges_raw' : [
        {
            'check':'values within list',
            'columns':['Unit Name'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
     'elementary_exchanges_raw' : [
        {
            'check':'values within list',
            'columns':['Unit Name'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        }
    ],
    'ecoinvent-v3.9.1_raw': [{
            'check':'values are unique',
            'columns':['activity_name']
    }],
    'ictr_company_result_raw': [
        {
            'check':'values occur as expected',
            'columns':['companies_id'],
            'expected_count': 18
        },   
        {
            'check':'values sum to 1',
            'columns':['companies_id','benchmark'],
            'sum_column': 'ICTR_share'
        },
        {
            'check':'values within list',
            'columns': ['ICTR_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['country'],
            'value_list':['netherlands','germany','spain','france','austria']
        },
        {
            'check':'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
        {
            'check':'values within list',
            'columns': ['benchmark'],
            'value_list':['all','isic_sec','tilt_sec','unit','unit_isic_sec','unit_tilt_sec']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high']
        },
    ],
    'pctr_company_result_raw': [
        {
            'check':'values occur as expected',
            'columns':['companies_id'],
            'expected_count': 18
        },   
        {
            'check':'values sum to 1',
            'columns':['companies_id','benchmark'],
            'sum_column': 'PCTR_share'
        },
        {
            'check':'values within list',
            'columns': ['PCTR_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['country'],
            'value_list':['netherlands','germany','spain','france','austria']
        },
        {
            'check':'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
        {
            'check':'values within list',
            'columns': ['benchmark'],
            'value_list':['all','isic_sec','tilt_sec','unit','unit_isic_sec','unit_tilt_sec']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high']
        },
    ],
    'istr_company_result_raw':[
        {
            'check':'values occur as expected',
            'columns':['companies_id'],
            'expected_count': 12
        },   
        {
            'check':'values sum to 1',
            'columns':['companies_id','scenario','year'],
            'sum_column': 'ISTR_share'
        },
        {
            'check':'values within list',
            'columns': ['ISTR_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['Scenario'],
            'value_list':['WEO NZ 2050','IPR 1.5c RPS']
        },
        {
            'check':'values within list',
            'columns': ['Scenario'],
            'value_list':['2030','2050']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high',None]
        },
        {
            'check':'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
    ],
    'pstr_company_result_raw':[
        {
            'check':'values occur as expected',
            'columns':['companies_id'],
            'expected_count': 12
        },   
        {
            'check':'values sum to 1',
            'columns':['companies_id','scenario','year'],
            'sum_column': 'PSTR_share'
        },
        {
            'check':'values within list',
            'columns': ['PSTR_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['Scenario'],
            'value_list':['WEO NZ 2050','IPR 1.5c RPS']
        },
        {
            'check':'values within list',
            'columns': ['year'],
            'value_list':['2030','2050']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high',None]
        },
        {
            'check':'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
    ],
    'istr_product_result_raw':[
        {
            'check':'distinct values occur as expected',
            'columns':['input_name'],
            'distinct_columns': ['tilt_sector'],
            'expected_count': 1
        },
        {
            'check':'values within list',
            'columns': ['istr_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['Scenario'],
            'value_list':['WEO NZ 2050','IPR 1.5c RPS']
        },
        {
            'check':'values within list',
            'columns': ['year'],
            'value_list':['2030','2050']
        },
        {
            'check':'values within list',
            'columns':['tilt_sector'],
            'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
        },  
        {
            'check':'values within list',
            'columns':['unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high',None]
        },
    ],
    'pstr_product_result_raw':[
        {
            'check':'distinct values occur as expected',
            'columns':['companies_id'],
            'distinct_columns': ['tilt_sector', 'tilt_subsector'],
            'expected_count': 1
        },
        {
            'check':'values within list',
            'columns': ['pstr_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['Scenario'],
            'value_list':['WEO NZ 2050','IPR 1.5c RPS']
        },
        {
            'check':'values within list',
            'columns': ['year'],
            'value_list':['2030','2050']
        },
        {
            'check':'values within list',
            'columns':['tilt_sector'],
            'value_list': ['Industry','Land Use','Metals','no_match','Construction','Non-metallic Minerals','Transport']
        },   
        {
            'check':'values within list',
            'columns':['tilt_subsector'],
            'value_list': ['Other Non-metallic Minerals','Agriculture & Livestock','Construction Buildings','Food Related Products','Fishing & Forestry','Bioenergy & Waste Power','Other Industry','Cement','Bioenergy & Waste Energy','Automotive LDV','Renewable Energy','Iron & Steel','Aviation','no_match','Oil Energy','Other Metals','Machinery & Equipment','Other Transport','Coal Energy','Construction Residential','Total Power','Chemicals','Shipping','Gas Energy','Total Energy','Automotive HDV']
        },
        {
            'check':'values within list',
            'columns':['unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high',None]
        },
    ],
    'pctr_product_result_raw': [ 
        {
            'check':'values occur as expected',
            'columns':['companies_id','ep_product'],
            'expected_count': 6
        },  
        {
            'check':'distinct values occur as expected',
            'columns':['companies_id'],
            'distinct_columns': ['benchmark'],
            'expected_count': 6
        }, 
        {
            'check':'values within list',
            'columns': ['PCTR_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
        {
            'check':'values within list',
            'columns': ['benchmark'],
            'value_list':['all','isic_sec','tilt_sec','unit','unit_isic_sec','unit_tilt_sec']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high',None]
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty'],
            'value_list':['low','medium','high',None]
        },
        {
            'check':'values within list',
            'columns':['unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
    ],
    'ictr_product_result_raw': [ 
        {
            'check':'values occur as expected',
            'columns':['companies_id','ep_product'],
            'expected_count': 6
        },  
        {
            'check':'distinct values occur as expected',
            'columns':['companies_id'],
            'distinct_columns': ['benchmark'],
            'expected_count': 6
        }, 
        {
            'check':'values within list',
            'columns': ['ICTR_risk_category'],
            'value_list':['low','medium','high']
        },
        {
            'check':'values within list',
            'columns': ['main_activity'],
            'value_list':['agent/ representative','distributor','manufacturer/ producer','multi-category','retailer','service provider','subcontractor','wholesaler']
        },
        {
            'check':'values within list',
            'columns': ['benchmark'],
            'value_list':['all','isic_sec','tilt_sec','unit','unit_isic_sec','unit_tilt_sec']
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty_company_average'],
            'value_list':['low','medium','high',None]
        },
        {
            'check':'values within list',
            'columns': ['matching_certainty'],
            'value_list':['low','medium','high',None]
        },
        {
            'check':'values within list',
            'columns':['unit'],
            'value_list':['l', 'metric ton*km', 'kWh', 'm', 'ha', 'guest night', 'm2*year', 'm3', 'MJ', 'km', 'km*year', 'hour', 'unit', 'kg', 'kg*day', 'person*km', 'm2', 'm*year']
        },
    ],

}


