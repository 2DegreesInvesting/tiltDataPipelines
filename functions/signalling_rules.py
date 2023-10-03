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
    }
    ]

}


