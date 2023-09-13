signalling_checks_dictionary = {
    'companies_raw': [
        {
            'check': 'values in range',
            'columns':['year_established'],
            'range_start' : 1800,
            'range_end': 2023
        }
        ],
    'main_activity_raw' : [{
            'check':'values within list',
            'columns':['main_activity'],
            'value_list':['distributor', 'agent/ representative', 'manufacturer/ producer', 'missing', 'multi-category', 'retailer', 'service provider', 'subcontractor', 'wholesaler']
    }],
    'undefined_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
    'cut_off_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
      'en15804_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
    'consequential_ao_raw' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ],
    'activities_transformed' : [
        {
            'check':'values within list',
            'columns':['Special Activity Type'],
            'value_list':['market group', 'market activity', 'ordinary transforming activity']
        }
    ]


}


