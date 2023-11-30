from pyspark.sql.types import StringType,StructType, StructField, DataType, BooleanType, DoubleType, ShortType, TimestampType, IntegerType, DoubleType, DateType, ByteType

def get_table_definition(table_name: str = '') -> dict:
    """
    Template for a table:

    'table_name_container': {
        'columns' :  StructType([
            StructField('string_column', StringType(), False),
            StructField('integer_column', IntegerType(), True),
            StructField('decimal_column', DoubleType(), True),
            StructField('Date_column', DateType(), True),
        ]  
        ), 
        'container': 'container_name',
        'location': 'location_in_container',
        'type': 'data_type',
        'partition_column' : 'name_of_partition_column',
        'quality_checks': [['unique', ['string_column']],
                            ['format', 'string_column', r"[a-zA-Z\-]"]]
    }
    """

    table_dict = {
        'geographies_landingzone': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('Contained and Overlapping Geographies', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Geographies.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'geographies_raw': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('Contained and Overlapping Geographies', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'geographies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique', ['ID']],
                                ['format', 'Geographical Classification', r"[a-zA-Z\-]"]]
        },
        'geographies_transform': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'geographies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique', ['ID']]]
        },
        'geographies_related': {
            'columns' :  StructType([
                StructField('Shortname', StringType(), True),
                StructField('Shortname_related', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'geographies_related',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'undefined_ao_landingzone': {
            'columns' :  StructType([
                StructField('Activity UUID', StringType(), True),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Product Group', StringType(), True),
                StructField('Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Undefined AO.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'undefined_ao_raw': {
            'columns' :  StructType([
                StructField('Activity UUID', StringType(), False),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), False),
                StructField('Product Group', StringType(), True),
                StructField('Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'undefined_ao',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'cut_off_ao_landingzone': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), True),
                StructField('Activity UUID', StringType(), True),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Cut-Off Classification', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Cut-OFF AO.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'cut_off_ao_raw': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('Activity UUID', StringType(), False),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), False),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Cut-Off Classification', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'cutoff_ao',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'en15804_ao_landingzone': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), True),
                StructField('Activity UUID', StringType(), True),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Cut-Off Classification', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/EN15804 AO.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'en15804_ao_raw': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('Activity UUID', StringType(), False),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), False),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Cut-Off Classification', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'en15804_ao',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'consequential_ao_landingzone': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), True),
                StructField('Activity UUID', StringType(), True),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Technology Level', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Consequential AO.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'consequential_ao_raw': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('Activity UUID', StringType(), False),
                StructField('EcoQuery URL', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Technology Level', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), False),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'consequential_ao',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'products_transformed': {
            'columns' :  StructType([
                StructField('Product UUID', StringType(), False),
                StructField('Product Group', StringType(), True),
                StructField('Product Name', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'products',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'activities_transformed': {
            'columns' :  StructType([
                StructField('Activity UUID', StringType(), False),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time Period', StringType(), True),
                StructField('Special Activity Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC Classification', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'activities',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique', ['Activity UUID']]]
        },
        'products_activities_transformed': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('Activity UUID', StringType(), False),
                StructField('Product UUID', StringType(), False),
                StructField('EcoQuery URL', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'products_activities',
            'type': 'parquet',
            'partition_column' : 'AO Method',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'lcia_methods_landingzone': {
            'columns' :  StructType([
                StructField('Method Name', StringType(), True),
                StructField('Status', StringType(), True),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField('Links to Characterization Factor Successes', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/LCIA Methods.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'lcia_methods_raw': {
            'columns' :  StructType([
                StructField('Method Name', StringType(), False),
                StructField('Status', StringType(), False),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField('Links to Characterization Factor Successes', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'lcia_methods',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'impact_categories_landingzone': {
            'columns' :  StructType([
                StructField('Resources - Emissions - Total', StringType(), True),
                StructField('Main impact/damage category', StringType(), True),
                StructField('Inventory - Midpoint - Endpoint - AoP', StringType(), True),
                StructField('Area of Protection (AoP)', StringType(), True),
                StructField('Used in EN15804', StringType(), True),
                StructField('Method', StringType(), True),
                StructField('Category', StringType(), True),
                StructField('Indicator', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Category name in method', StringType(), True),
                StructField('Indicator name in method', StringType(), True),
                StructField('Unit in method', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Impact Categories.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'impact_categories_raw': {
            'columns' :  StructType([
                StructField('Resources - Emissions - Total', StringType(), True),
                StructField('Main impact/damage category', StringType(), True),
                StructField('Inventory - Midpoint - Endpoint - AoP', StringType(), True),
                StructField('Area of Protection (AoP)', StringType(), True),
                StructField('Used in EN15804', StringType(), True),
                StructField('Method', StringType(), True),
                StructField('Category', StringType(), True),
                StructField('Indicator', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Category name in method', StringType(), True),
                StructField('Indicator name in method', StringType(), True),
                StructField('Unit in method', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'impact_categories',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'intermediate_exchanges_landingzone': {
            'columns' :  StructType([
                StructField('ID', StringType(), True),
                StructField('Name', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('By-product Classification', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('Synonym', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Intermediate Exchanges.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'intermediate_exchanges_raw': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('By-product Classification', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'intermediate_exchanges',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['ID']]]
        },
        'elementary_exchanges_landingzone': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Compartment', StringType(), True),
                StructField('Sub Compartment', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('Formula', StringType(), True),
            ]  
            ), 
            'container': 'landingzone',
            'location': 'ecoInvent/Elementary Exchanges.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'elementary_exchanges_raw': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Compartment', StringType(), True),
                StructField('Sub Compartment', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('Formula', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'elementary_exchanges',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['ID']]]
        },
        'issues_companies_landingzone': {
            'columns' :  StructType([
                StructField('issues_companies_id', StringType(), False),
                StructField('issues_id', StringType(), True),
                StructField('companies_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/issues_companies.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'issues_companies_raw': {
            'columns' :  StructType([
                StructField('issues_companies_id', StringType(), False),
                StructField('issues_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'issues_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['issues_companies_id']]]
        },
        'issues_landingzone': {
            'columns' :  StructType([
                StructField('issues_id', StringType(), False),
                StructField('repo', StringType(), True),
                StructField('issue', StringType(), True),
                StructField('title', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/issues.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'issues_raw': {
            'columns' :  StructType([
                StructField('issues_id', StringType(), False),
                StructField('repo', StringType(), True),
                StructField('issue', StringType(), True),
                StructField('title', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'issues',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['issues_id']]]
        },
        'sea_food_companies_landingzone': {
            'columns' :  StructType([
                StructField('sea_food_companies_id', StringType(), False),
                StructField('sea_food_id', StringType(), True),
                StructField('companies_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sea_food_companies.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'sea_food_companies_raw': {
            'columns' :  StructType([
                StructField('sea_food_companies_id', StringType(), False),
                StructField('sea_food_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sea_food_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['sea_food_companies_id']]]
        },
        'sea_food_landingzone': {
            'columns' :  StructType([
                StructField('sea_food_id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('supply_chain_main_segment', StringType(), True),
                StructField('supply_chain_feed', StringType(), True),
                StructField('supply_chain_fishing', StringType(), True),
                StructField('supply_chain_aquaculture', StringType(), True),
                StructField('supply_chain_processing', StringType(), True),
                StructField('supply_chain_wholesale_distribution', StringType(), True),
                StructField('supply_chain_retail', StringType(), True),
                StructField('supply_chain_foodservice', StringType(), True),
                StructField('supply_chain_fishing_vessels', StringType(), True),
                StructField('supply_chain_fishing_and_aquaculture_gear_equipment', StringType(), True),
                StructField('supply_chain_other', StringType(), True),
                StructField('full_species_disclosure_for_entire_portfolio', StringType(), True),
                StructField('full_species_disclosure_for_at_least_part_of_portfolio', StringType(), True),
                StructField('species_disclosure_text', StringType(), True),
                StructField('seafood_exposure', StringType(), True),
                StructField('reference', StringType(), True),
                StructField('websites', StringType(), True),
                StructField('information', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sourcing_regions_identified', StringType(), True),
                StructField('list_of_species', StringType(), True),
                StructField('reporting_precision_pt_score', StringType(), True),
                StructField('world_benchmarking_alliance_seafood_stewardship_index', StringType(), True),
                StructField('ocean_health_index_score_2012', StringType(), True),
                StructField('ocean_health_index_score_2021', StringType(), True),
                StructField('ocean_health_index_score_percent_change_2021_2012', StringType(), True),
                StructField('fish_source_score_management_quality', StringType(), True),
                StructField('fish_source_score_managers_compliance', StringType(), True),
                StructField('fish_source_score_fishers_compliance', StringType(), True),
                StructField('fish_source_score_current_stock_health', StringType(), True),
                StructField('fish_source_score_future_stock_health', StringType(), True),
                StructField('sea_around_us_unreported_total_catch_percent', StringType(), True),
                StructField('sea_around_us_bottom_trawl_total_catch_percent_35', StringType(), True),
                StructField('sea_around_us_gillnets_total_catch_percent', StringType(), True),
                StructField('global_fishing_index_data_availability_on_stock_sustainability', StringType(), True),
                StructField('global_fishing_index_proportion_of_assessed_fish_stocks_that_is_sustainable', StringType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_sustainable', StringType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_overfished', StringType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_not_assessed', StringType(), True),
                StructField('global_fishing_index_fisheries_governance_score', StringType(), True),
                StructField('global_fishing_index_alignment_with_international_standards_for_protecting_worker_rights_and_safety_in_fisheries_assessment_score', StringType(), True),
                StructField('global_fishing_index_fishery_subsidy_program_assessment_score', StringType(), True),
                StructField('global_fishing_index_knowledge_on_fishing_fleets_assessment_score', StringType(), True),
                StructField('global_fishing_index_compliance_monitoring_and_surveillance_programs_assessment_score', StringType(), True),
                StructField('global_fishing_index_severity_of_fishery_sanctions_assessment_score', StringType(), True),
                StructField('global_fishing_index_access_of_foreign_fishing_fleets_assessment_score', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sea_food.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'sea_food_raw': {
            'columns' :  StructType([
                StructField('sea_food_id', StringType(), False),
                StructField('company_name', StringType(), False),
                StructField('supply_chain_main_segment', StringType(), True),
                StructField('supply_chain_feed', BooleanType(), True),
                StructField('supply_chain_fishing', BooleanType(), True),
                StructField('supply_chain_aquaculture', BooleanType(), True),
                StructField('supply_chain_processing', BooleanType(), True),
                StructField('supply_chain_wholesale_distribution', BooleanType(), True),
                StructField('supply_chain_retail', BooleanType(), True),
                StructField('supply_chain_foodservice', BooleanType(), True),
                StructField('supply_chain_fishing_vessels', BooleanType(), True),
                StructField('supply_chain_fishing_and_aquaculture_gear_equipment', BooleanType(), True),
                StructField('supply_chain_other', BooleanType(), True),
                StructField('full_species_disclosure_for_entire_portfolio', BooleanType(), True),
                StructField('full_species_disclosure_for_at_least_part_of_portfolio', BooleanType(), True),
                StructField('species_disclosure_text', StringType(), True),
                StructField('seafood_exposure', StringType(), True),
                StructField('reference', StringType(), True),
                StructField('websites', StringType(), True),
                StructField('information', StringType(), True),
                StructField('country', StringType(), False),
                StructField('sourcing_regions_identified', StringType(), True),
                StructField('list_of_species', StringType(), True),
                StructField('reporting_precision_pt_score', DoubleType(), True),
                StructField('world_benchmarking_alliance_seafood_stewardship_index', DoubleType(), True),
                StructField('ocean_health_index_score_2012', DoubleType(), True),
                StructField('ocean_health_index_score_2021', DoubleType(), True),
                StructField('ocean_health_index_score_percent_change_2021_2012', DoubleType(), True),
                StructField('fish_source_score_management_quality', DoubleType(), True),
                StructField('fish_source_score_managers_compliance', DoubleType(), True),
                StructField('fish_source_score_fishers_compliance', DoubleType(), True),
                StructField('fish_source_score_current_stock_health', DoubleType(), True),
                StructField('fish_source_score_future_stock_health', DoubleType(), True),
                StructField('sea_around_us_unreported_total_catch_percent', DoubleType(), True),
                StructField('sea_around_us_bottom_trawl_total_catch_percent_35', DoubleType(), True),
                StructField('sea_around_us_gillnets_total_catch_percent', DoubleType(), True),
                StructField('global_fishing_index_data_availability_on_stock_sustainability', DoubleType(), True),
                StructField('global_fishing_index_proportion_of_assessed_fish_stocks_that_is_sustainable', DoubleType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_sustainable', DoubleType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_overfished', DoubleType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_not_assessed', DoubleType(), True),
                StructField('global_fishing_index_fisheries_governance_score', DoubleType(), True),
                StructField('global_fishing_index_alignment_with_international_standards_for_protecting_worker_rights_and_safety_in_fisheries_assessment_score', DoubleType(), True),
                StructField('global_fishing_index_fishery_subsidy_program_assessment_score', DoubleType(), True),
                StructField('global_fishing_index_knowledge_on_fishing_fleets_assessment_score', DoubleType(), True),
                StructField('global_fishing_index_compliance_monitoring_and_surveillance_programs_assessment_score', DoubleType(), True),
                StructField('global_fishing_index_severity_of_fishery_sanctions_assessment_score', DoubleType(), True),
                StructField('global_fishing_index_access_of_foreign_fishing_fleets_assessment_score', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sea_food',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['sea_food_id']]]
        },
        'products_companies_landingzone': {
            'columns' :  StructType([
                StructField('products_companies_id', StringType(), False),
                StructField('products_id', StringType(), True),
                StructField('companies_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/products_companies.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'products_companies_raw': {
            'columns' :  StructType([
                StructField('products_companies_id', StringType(), False),
                StructField('products_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'products_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['products_companies_id']]]
        },
        'companies_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('main_activity_id', StringType(), True),
                StructField('address', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('information', StringType(), True),
                StructField('min_headcount', StringType(), True),
                StructField('max_headcount', StringType(), True),
                StructField('type_of_building_for_registered_address', StringType(), True),
                StructField('verified_by_europages', StringType(), True),
                StructField('year_established', StringType(), True),
                StructField('websites', StringType(), True),
                StructField('download_datetime', StringType(), True),
                StructField('country_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/companies.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'companies_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), False),
                StructField('main_activity_id', StringType(), True),
                StructField('address', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('information', StringType(), True),
                StructField('min_headcount', IntegerType(), True),
                StructField('max_headcount', IntegerType(), True),
                StructField('type_of_building_for_registered_address', StringType(), True),
                StructField('verified_by_europages', BooleanType(), True),
                StructField('year_established', ShortType(), True),
                StructField('websites', StringType(), True),
                StructField('download_datetime', TimestampType(), True),
                StructField('country_id', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['companies_id']],
                                ['in list',['country_id'],['1fe80e506149e4b49ca3560003edd2b8d57e6d9e',
                                                        '23e591e8c36dda987970603ad0fdd031b7dff9f9',
                                                        '9aec35c79b4e91fefb2a310d33bd736a14c961d8',
                                                        '6ace185eedb813fe84c2eca7641f9fa0aa3bfdc3',
                                                        '51288d140528c2d5c0565891be70300a9b0e365f']]]
        },
         'main_activity_landingzone': {
            'columns' :  StructType([
                StructField('main_activity_id', StringType(), False),
                StructField('main_activity', StringType(), False),
                StructField('ecoinvent', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/main_activity.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'main_activity_raw': {
            'columns' :  StructType([
                StructField('main_activity_id', StringType(), False),
                StructField('main_activity', StringType(), False),
                StructField('ecoinvent', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'main_activity',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['main_activity_id']]]
        },
         'geography_landingzone': {
            'columns' :  StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), True),
                StructField('ecoinvent_geography', StringType(), True),
                StructField('priority', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/geography.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'geography_raw': {
            'columns' :  StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), False),
                StructField('ecoinvent_geography', StringType(), True),
                StructField('priority', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'geography',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['geography_id']]]
        },
         'country_landingzone': {
            'columns' :  StructType([
                StructField('country_id', StringType(), False),
                StructField('country', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/country.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'country_raw': {
            'columns' :  StructType([
                StructField('country_id', StringType(), False),
                StructField('country', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'country',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['country_id']]]
        },
         'delimited_products_landingzone': {
            'columns' :  StructType([
                StructField('delimited_products_id', StringType(), False),
                StructField('delimited_id', StringType(), True),
                StructField('products_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/delimited_products.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'delimited_products_raw': {
            'columns' :  StructType([
                StructField('delimited_products_id', StringType(), False),
                StructField('delimited_id', StringType(), True),
                StructField('products_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'delimited_products',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['delimited_products_id']]]
        },
         'products_landingzone': {
            'columns' :  StructType([
                StructField('products_id', StringType(), False),
                StructField('products_and_services', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/products.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'products_raw': {
            'columns' :  StructType([
                StructField('products_id', StringType(), False),
                StructField('products_and_services', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'products',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['products_id']]]
        },
         'categories_companies_landingzone': {
            'columns' :  StructType([
                StructField('categories_companies_id', StringType(), False),
                StructField('categories_id', StringType(), True),
                StructField('companies_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/categories_companies.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'categories_companies_raw': {
            'columns' :  StructType([
                StructField('categories_companies_id', StringType(), False),
                StructField('categories_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'categories_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['categories_companies_id']]]
        },
         'delimited_landingzone': {
            'columns' :  StructType([
                StructField('delimited_id', StringType(), False),
                StructField('delimited', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/delimited.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'delimited_raw': {
            'columns' :  StructType([
                StructField('delimited_id', StringType(), False),
                StructField('delimited', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'delimited',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['delimited_id']]]
        },
         'clustered_delimited_landingzone': {
            'columns' :  StructType([
                StructField('clustered_delimited_id', StringType(), False),
                StructField('clustered_id', StringType(), True),
                StructField('delimited_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/clustered_delimited.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'clustered_delimited_raw': {
            'columns' :  StructType([
                StructField('clustered_delimited_id', StringType(), False),
                StructField('clustered_id', StringType(), True),
                StructField('delimited_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'clustered_delimited',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['clustered_delimited_id']]]
        },
        'clustered_landingzone': {
            'columns' :  StructType([
                StructField('clustered_id', StringType(), False),
                StructField('clustered', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/clustered.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'clustered_raw': {
            'columns' :  StructType([
                StructField('clustered_id', StringType(), False),
                StructField('clustered', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'clustered',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['clustered_id']]]
        },
        'categories_sector_ecoinvent_delimited_landingzone': {
            'columns' :  StructType([
                StructField('categories_sector_ecoinvent_delimited_id', StringType(), False),
                StructField('categories_id', StringType(), True),
                StructField('sector_ecoinvent_delimited_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/categories_sector_ecoinvent_delimited.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'categories_sector_ecoinvent_delimited_raw': {
            'columns' :  StructType([
                StructField('categories_sector_ecoinvent_delimited_id', StringType(), False),
                StructField('categories_id', StringType(), True),
                StructField('sector_ecoinvent_delimited_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'categories_sector_ecoinvent_delimited',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['categories_sector_ecoinvent_delimited_id']]]
        },
        'categories_landingzone': {
            'columns' :  StructType([
                StructField('categories_id', StringType(), False),
                StructField('group', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/categories.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'categories_raw': {
            'columns' :  StructType([
                StructField('categories_id', StringType(), False),
                StructField('group', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'categories',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['categories_id']]]
        },
        'sector_ecoinvent_delimited_sector_ecoinvent_landingzone': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent_delimited_id', StringType(), True),
                StructField('sector_ecoinvent_id', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sector_ecoinvent_delimited_sector_ecoinvent.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_ecoinvent_delimited_sector_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent_delimited_id', StringType(), True),
                StructField('sector_ecoinvent_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_ecoinvent_delimited_sector_ecoinvent',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['sector_ecoinvent_delimited_sector_ecoinvent_id']]]
        },
         'sector_ecoinvent_delimited_landingzone': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_id', StringType(), False),
                StructField('sector_ecoinvent_delimited', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sector_ecoinvent_delimited.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_ecoinvent_delimited_raw': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_id', StringType(), False),
                StructField('sector_ecoinvent_delimited', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_ecoinvent_delimited',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['sector_ecoinvent_delimited_id']]]
        },
        'sector_ecoinvent_landingzone': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sector_ecoinvent.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_ecoinvent',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['sector_ecoinvent_id']]]
        },
        'product_matching_complete_all_cases_landingzone': {
            'columns' :  StructType([
                StructField('group_var', StringType(), False),
                StructField('ep_id', StringType(), False),
                StructField('ep_country', StringType(), True),
                StructField('ep_main_act', StringType(), True),
                StructField('ep_prod', StringType(), True),
                StructField('lca_id', StringType(), False),
                StructField('lca_prod', StringType(), True),
                StructField('lca_act_name', StringType(), True),
                StructField('lca_act_type', StringType(), True),
                StructField('lca_geo', StringType(), True),
                StructField('meta_mappable_main_activity', StringType(), True),
                StructField('meta_matching_main_activity', StringType(), True),
                StructField('score', StringType(), True),
                StructField('priority', StringType(), True),
                StructField('n_candidates', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('cos_sim_bert_mini', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'Matching/product_matching_complete_all_cases.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'product_matching_complete_all_cases_raw': {
            'columns' :  StructType([
                StructField('group_var', StringType(), False),
                StructField('ep_id', StringType(), False),
                StructField('lca_id', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'product_matching_complete_all_cases',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['group_var','ep_id','lca_id']]]
        },
        'labelled_activity_v1.0_landingzone': {
            'columns' :  StructType([
                StructField('', StringType(), False),
                StructField('index', StringType(), False),
                StructField('ep_act_id', StringType(), False),
                StructField('ep_country', StringType(), False),
                StructField('ep_main_act', StringType(), True),
                StructField('ep_act', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('Activity UUID', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('Special Activity Type', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('bert_activities', StringType(), True),
                StructField('completion', StringType(), True),
                StructField('logprob', StringType(), True),
                StructField('bert_epact_lcaprod', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'Matching/labelled_activity_v1.0.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'labelled_activity_v1.0_raw': {
            'columns' :  StructType([
                StructField('index', StringType(), False),
                StructField('ep_act_id', StringType(), False),
                StructField('ep_country', StringType(), False),
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'labelled_activity_v1.0',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['index','ep_act_id', 'ep_country', 'Activity UUID & Product UUID']]]
        },
        'ep_companies_NL_postcode_landingzone': {
            'columns' :  StructType([
                StructField('id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('postcode', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/ep_companies_NL_postcode.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'ep_companies_NL_postcode_raw': {
            'columns' :  StructType([
                StructField('id', StringType(), False),
                StructField('company_name', StringType(), False),
                StructField('postcode', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'ep_companies_NL_postcode',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': [['unique',['id']]]
        },
        'ep_ei_matcher_landingzone': {
            'columns' :  StructType([
                StructField('group_var', StringType(), False),
                StructField('ep_id', StringType(), True),
                StructField('ep_country', StringType(), True),
                StructField('ep_main_act', StringType(), True),
                StructField('ep_clustered', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('completion', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/ep_ei_matcher.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'ep_ei_matcher_raw': {
            'columns' :  StructType([
                StructField('group_var', StringType(), False),
                StructField('ep_id', StringType(), True),
                StructField('ep_country', StringType(), False),
                StructField('ep_main_act', StringType(), True),
                StructField('ep_clustered', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('completion', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'ep_ei_matcher',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'scenario_targets_IPR_NEW_landingzone': {
            'columns' :  StructType([
                StructField('Scenario', StringType(), False),
                StructField('Region', StringType(), True),
                StructField('Variable Class', StringType(), True),
                StructField('Sub Variable Class', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('Sub Sector', StringType(), True),
                StructField('Units', StringType(), True),
                StructField('Year', StringType(), True),
                StructField('Value', StringType(), True),
                StructField('Reductions', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/scenario_targets_IPR_NEW.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'scenario_targets_IPR_NEW_raw': {
            'columns' :  StructType([
                StructField('Scenario', StringType(), False),
                StructField('Region', StringType(), True),
                StructField('Variable Class', StringType(), True),
                StructField('Sub Variable Class', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('Sub Sector', StringType(), True),
                StructField('Units', StringType(), True),
                StructField('Year', ShortType(), True),
                StructField('Value', DoubleType(), True),
                StructField('Reductions', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'scenario_targets_IPR_NEW',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'scenario_targets_WEO_NEW_landingzone': {
            'columns' :  StructType([
                StructField('PUBLICATION', StringType(), False),
                StructField('SCENARIO', StringType(), True),
                StructField('CATEGORY', StringType(), True),
                StructField('PRODUCT', StringType(), True),
                StructField('FLOW', StringType(), True),
                StructField('UNIT', StringType(), True),
                StructField('REGION', StringType(), True),
                StructField('YEAR', StringType(), True),
                StructField('VALUE', StringType(), True),
                StructField('REDUCTIONS', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/scenario_targets_WEO_NEW.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'scenario_targets_WEO_NEW_raw': {
            'columns' :  StructType([
                StructField('PUBLICATION', StringType(), False),
                StructField('SCENARIO', StringType(), True),
                StructField('CATEGORY', StringType(), True),
                StructField('PRODUCT', StringType(), True),
                StructField('FLOW', StringType(), True),
                StructField('UNIT', StringType(), True),
                StructField('REGION', StringType(), True),
                StructField('YEAR', ShortType(), True),
                StructField('VALUE', DoubleType(), True),
                StructField('REDUCTIONS', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'scenario_targets_WEO_NEW',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'scenario_tilt_mapper_2023-07-20_landingzone': {
            'columns' :  StructType([
                StructField('tilt_sector', StringType(), False),
                StructField('tilt_subsector', StringType(), True),
                StructField('weo_product', StringType(), True),
                StructField('weo_flow', StringType(), True),
                StructField('ipr_sector', StringType(), True),
                StructField('ipr_subsector', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/scenario_tilt_mapper_2023-07-20.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'scenario_tilt_mapper_2023-07-20_raw': {
            'columns' :  StructType([
                StructField('tilt_sector', StringType(), False),
                StructField('tilt_subsector', StringType(), True),
                StructField('weo_product', StringType(), True),
                StructField('weo_flow', StringType(), True),
                StructField('ipr_sector', StringType(), True),
                StructField('ipr_subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'scenario_tilt_mapper_2023-07-20',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_resolve_without_tiltsector_landingzone': {
            'columns' :  StructType([
                StructField('main_activity', StringType(), True),
                StructField('clustered', StringType(), False),
                StructField('tilt_subsector', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/sector_resolve_without_tiltsector.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_resolve_without_tiltsector_raw': {
            'columns' :  StructType([
                StructField('main_activity', StringType(), True),
                StructField('clustered', StringType(), False),
                StructField('tilt_subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_resolve_without_tiltsector',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'tilt_isic_mapper_2023-07-20_landingzone': {
            'columns' :  StructType([
                StructField('tilt_sector', StringType(), False),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_4digit_name_ecoinvent', StringType(), True),
                StructField('isic_section', StringType(), True),
                StructField('Comments', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/tilt_isic_mapper_2023-07-20.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'tilt_isic_mapper_2023-07-20_raw': {
            'columns' :  StructType([
                StructField('tilt_sector', StringType(), False),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_4digit_name_ecoinvent', StringType(), True),
                StructField('isic_section', StringType(), True),
                StructField('Comments', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'tilt_isic_mapper_2023-07-20',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
         },
        'ecoinvent-v3.9.1_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('activity_uuid', StringType(), True),
                StructField('activity_name', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('special_activity_type', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('product_uuid', StringType(), True),
                StructField('reference_product_name', StringType(), True),
                StructField('cpc_classification', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('method_category_indicator_product_information', StringType(), True),
                StructField('product_inputs_tbc', StringType(), True),
                StructField('ipcc_2021_climate_change_global_warming_potential_gwp100_kg_co2_eq', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/ecoinvent-v3.9.1.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'ecoinvent-v3.9.1_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('activity_uuid', StringType(), True),
                StructField('activity_name', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('special_activity_type', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('product_uuid', StringType(), True),
                StructField('reference_product_name', StringType(), True),
                StructField('cpc_classification', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('method_category_indicator_product_information', StringType(), True),
                StructField('product_inputs_tbc', StringType(), True),
                StructField('ipcc_2021_climate_change_global_warming_potential_gwp100_kg_co2_eq', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'ecoinvent-v3.9.1',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
         },
        'ecoinvent_inputs_overview_raw_landingzone': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('By-product Classification', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('Synonym', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/ecoinvent_inputs_overview_raw.csv',
            'type': 'ecoInvent',
            'partition_column' : '',
            'quality_checks': []
        },
        'ecoinvent_inputs_overview_raw_raw': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('By-product Classification', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'ecoinvent_inputs_overview_raw',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
         },
        'ecoinvent_input_data_relevant_columns_landingzone': {
            'columns' :  StructType([
                StructField('activityId', StringType(), False),
                StructField('activityName', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('reference product', StringType(), True),
                StructField('group', StringType(), True),
                StructField('exchange name', StringType(), True),
                StructField('activityLinkId', StringType(), True),
                StructField('activityLink_activityName', StringType(), True),
                StructField('activityLink_geography', StringType(), True),
                StructField('exchange unitName', StringType(), True),
                StructField('exchange amount', StringType(), True),
                StructField('CPC_classificationValue', StringType(), True),
                StructField('By-product classification_classificationValue', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/ecoinvent_input_data_relevant_columns.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'ecoinvent_input_data_relevant_columns_raw': {
            'columns' :  StructType([
                StructField('activityId', StringType(), False),
                StructField('activityName', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('reference product', StringType(), True),
                StructField('group', StringType(), True),
                StructField('exchange name', StringType(), True),
                StructField('activityLinkId', StringType(), True),
                StructField('activityLink_activityName', StringType(), True),
                StructField('activityLink_geography', StringType(), True),
                StructField('exchange unitName', StringType(), True),
                StructField('exchange amount', DoubleType(), True),
                StructField('CPC_classificationValue', StringType(), True),
                StructField('By-product classification_classificationValue', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'ecoinvent_input_data_relevant_columns_raw',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
         },
        'tilt_sector_classification_landingzone': {
            'columns' :  StructType([
                StructField('tilt_sector', StringType(), False),
                StructField('tilt_subsector', StringType(), False)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/tilt_sector_classification.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'tilt_sector_classification_raw': {
            'columns' :  StructType([
                StructField('tilt_sector', StringType(), False),
                StructField('tilt_subsector', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'tilt_sector_classification',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_upstream_products_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('input_activity_uuid_product_uuid', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_co2_footprint', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('input_isic_4digit', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/emissions_profile_upstream_products.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_upstream_products_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('input_activity_uuid_product_uuid', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_co2_footprint', DoubleType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('input_isic_4digit', StringType(), True),               
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'emissions_profile_upstream_products',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_upstream_products_ecoinvent_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('ei_geography', StringType(), True),
                StructField('input_activity_uuid_product_uuid', StringType(), True),
                StructField('input_co2_footprint', StringType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('input_reference_product_name', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('grouped_by', StringType(), True),
                StructField('profile_ranking', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/emissions_profile_upstream_products_ecoinvent.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_upstream_products_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('ei_geography', StringType(), True),
                StructField('input_activity_uuid_product_uuid', StringType(), True),
                StructField('input_co2_footprint', DoubleType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('input_reference_product_name', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('grouped_by', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'emissions_profile_upstream_products_ecoinvent',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_upstream_companies_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('clustered', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/sector_profile_upstream_companies.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_upstream_companies_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('clustered', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_profile_upstream_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_upstream_products_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('input_activity_uuid_product_uuid', StringType(), True),
                StructField('input_reference_product_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('type', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/sector_profile_upstream_products.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_upstream_products_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('input_activity_uuid_product_uuid', StringType(), True),
                StructField('input_reference_product_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('type', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_profile_upstream_products',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_products_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('co2_footprint', StringType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('unit', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/emissions_profile_products.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_products_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('co2_footprint', DoubleType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'emissions_profile_products',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_products_ecoinvent_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('co2_footprint', StringType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('ei_geography', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('grouped_by', StringType(), True),
                StructField('profile_ranking', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/emissions_profile_products_ecoinvent.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_products_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('co2_footprint', DoubleType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('ei_geography', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('grouped_by', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'emissions_profile_products_ecoinvent',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_companies_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('clustered', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('type', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/sector_profile_companies.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_companies_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('clustered', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('type', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_profile_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_any_scenarios_landingzone': {
            'columns' :  StructType([
                StructField('scenario', StringType(), True),
                StructField('region', StringType(), True),
                StructField('sector', StringType(), False),
                StructField('subsector', StringType(), True),
                StructField('year', StringType(), True),
                StructField('value', StringType(), False),
                StructField('reductions', StringType(), False),
                StructField('type', StringType(), False)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/sector_profile_any_scenarios.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'sector_profile_any_scenarios_raw': {
            'columns' :  StructType([
                StructField('scenario', StringType(), True),
                StructField('region', StringType(), True),
                StructField('sector', StringType(), False),
                StructField('subsector', StringType(), True),
                StructField('year', IntegerType(), True),
                StructField('value', DoubleType(), False),
                StructField('reductions', DoubleType(), False),
                StructField('type', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_profile_any_scenarios',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_any_companies_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('clustered', StringType(), False),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('unit', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/emissions_profile_any_companies.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_any_companies_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('clustered', StringType(), False),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'emissions_profile_any_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_any_companies_ecoinvent_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('clustered', StringType(), False),
                StructField('companies_id', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('unit', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicator/emissions_profile_any_companies_ecoinvent.csv/',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'emissions_profile_any_companies_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('clustered', StringType(), False),
                StructField('companies_id', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ), 
            'container': 'raw',
            'location': 'emissions_profile_any_companies_ecoinvent',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'mapper_ep_ei_landingzone': {
            'columns' :  StructType([
                StructField('country', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('clustered', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), False), 
                StructField('multi_match', StringType(), True), 
                StructField('completion', StringType(), True)
            ]
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorAfter/mapper_ep_ei.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'mapper_ep_ei_raw': {
            'columns' :  StructType([
                StructField('country', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('clustered', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), False), 
                StructField('multi_match', BooleanType(), True), 
                StructField('completion', StringType(), True), 
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
            'container': 'raw',
            'location': 'mapper_ep_ei',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'ep_companies_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('clustered', StringType(), True)
            ]
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorAfter/ep_companies.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'ep_companies_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('clustered', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
            'container': 'raw',
            'location': 'ep_companies',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'ei_input_data_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False), 
                StructField('activity_name', StringType(), True), 
                StructField('product_geography', StringType(), True), 
                StructField('input_activity_uuid_product_uuid', StringType(), True), 
                StructField('exchange_name', StringType(), True), 
                StructField('input_geography', StringType(), True), 
                StructField('exchange_unit_name', StringType(), True), 
                StructField('input_priotiry', StringType(), True), 
                StructField('ecoinvent_geography', StringType(), True)
            ]
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorAfter/ei_input_data.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'ei_input_data_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False), 
                StructField('activity_name', StringType(), True), 
                StructField('product_geography', StringType(), True), 
                StructField('input_activity_uuid_product_uuid', StringType(), True), 
                StructField('exchange_name', StringType(), True), 
                StructField('input_geography', StringType(), True), 
                StructField('exchange_unit_name', StringType(), True), 
                StructField('input_priotiry', ByteType(), True), 
                StructField('ecoinvent_geography', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
            'container': 'raw',
            'location': 'ei_input_data',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'ei_activities_overview_landingzone': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False), 
                StructField('activity_name', StringType(), True), 
                StructField('geography', StringType(), True), 
                StructField('reference_product_name', StringType(), True), 
                StructField('unit', StringType(), True)
            ]
            ), 
            'container': 'landingzone',
            'location': 'tiltIndicatorAfter/ei_activities_overview.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'ei_activities_overview_raw': {
            'columns' :  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False), 
                StructField('activity_name', StringType(), True), 
                StructField('geography', StringType(), True), 
                StructField('reference_product_name', StringType(), True), 
                StructField('unit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
            'container': 'raw',
            'location': 'ei_activities_overview',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
         'dummy_quality_check': {
            'columns' :  StructType([
                StructField('signalling_id', StringType(), False),
                StructField('check_id', StringType(), False),
                StructField('column_name', StringType(), True),
                StructField('check_name', StringType(), True),
                StructField('total_count', IntegerType(), True),
                StructField('valid_count', IntegerType(), True)
            ]  
            ),
            'container': 'monitoring',
            'location': 'dummy_quality_check/dummy_check.csv',
            'type': 'csv',
            'partition_column' : '',
            'quality_checks': []
        },
        'monitoring_values': {
            'columns' :  StructType([
                StructField('signalling_id', IntegerType(), False),
                StructField('check_id', StringType(), False),
                StructField('column_name', StringType(), True),
                StructField('check_name', StringType(), True),
                StructField('total_count', IntegerType(), True),
                StructField('valid_count', IntegerType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ),
            'container': 'monitoring',
            'location': 'monitoring_values',
            'type': 'parquet',
            'partition_column' : 'table_name',
            'quality_checks': []
        },
        'ictr_company_result_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ICTR_share', StringType(), True),
                StructField('ICTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]  
            ),
            'container': 'landingzone',
            'location': 'tiltProfiles/ictr_company_result.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'ictr_company_result_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ICTR_share', DoubleType(), True),
                StructField('ICTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]  
            ),
            'container': 'raw',
            'location': 'ictr_company_result',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'ictr_product_result_landingzone': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ICTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'tiltProfiles/ictr_product_result.csv',
            'type': 'tiltData',
            'partition_column' : '',
            'quality_checks': []
        },
        'ictr_product_result_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ICTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'ictr_product_result',
            'type': 'parquet',
            'partition_column' : '',
            'quality_checks': []
        },
        'istr_company_result_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ISTR_share', StringType(), True),
                StructField('ISTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/istr_company_result.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
        },
        'istr_company_result_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ISTR_share', DoubleType(), True),
                StructField('ISTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'istr_company_result',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'istr_product_result_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ISTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True)
        ]
        )        , 
        'container': 'landingzone',
        'location': 'tiltProfiles/istr_product_result.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
        },
        'istr_product_result_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('ISTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', IntegerType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'istr_product_result',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'pctr_company_result_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PCTR_share', StringType(), True),
                StructField('PCTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/pctr_company_result.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
        },
        'pctr_company_result_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PCTR_share', DoubleType(), True),
                StructField('PCTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'pctr_company_result',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'pctr_product_result_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PCTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True), StructField('postcode', StringType(), True),
                StructField('address', StringType(), True), StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/pctr_product_result.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
        },
        'pctr_product_result_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PCTR_risk_category', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True), StructField('postcode', StringType(), True),
                StructField('address', StringType(), True), StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'pctr_product_result',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'pstr_company_result_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PSTR_share', StringType(), True),
                StructField('PSTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/pstr_company_result.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
        },
        'pstr_company_result_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PSTR_share', DoubleType(), True),
                StructField('PSTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', IntegerType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'pstr_company_result',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'pstr_product_result_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PSTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('multi_match', StringType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/pstr_product_result.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
        },
        'pstr_product_result_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('PSTR_risk_category', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', IntegerType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'pstr_product_result',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_profile_company_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_share_ew', StringType(), True), 
                StructField('emission_share_bc', StringType(), True), 
                StructField('emission_share_wc', StringType(), True), 
                StructField('emission_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('Co2e_upper', StringType(), True), 
                StructField('Co2e_lower', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Emission Profile company.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_profile_company_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_share_ew', DoubleType(), True), 
                StructField('emission_share_bc', DoubleType(), True), 
                StructField('emission_share_wc', DoubleType(), True), 
                StructField('emission_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('Co2e_upper', DoubleType(), True), 
                StructField('Co2e_lower', DoubleType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'emission_profile_company',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_profile_product_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('Co2e_lower', StringType(), True), 
                StructField('Co2e_upper', StringType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('multi_match', StringType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('isic_categorisation', StringType(), True), 
                StructField('isic_4digit', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Emission Profile product.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_profile_product_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('Co2e_lower', DoubleType(), True), 
                StructField('Co2e_upper', DoubleType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('multi_match', BooleanType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('isic_categorisation', StringType(), True), 
                StructField('isic_4digit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'emission_profile_product',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_upstream_profile_company_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_upstream_share_ew', StringType(), True), 
                StructField('emission_upstream_share_bc', StringType(), True), 
                StructField('emission_upstream_share_wc', StringType(), True), 
                StructField('Co2e_input_lower', StringType(), True), 
                StructField('Co2e_input_upper', StringType(), True), 
                StructField('emission_upstream_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Emission Upstream Profile compa.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_upstream_profile_company_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_upstream_share_ew', DoubleType(), True), 
                StructField('emission_upstream_share_bc', DoubleType(), True), 
                StructField('emission_upstream_share_wc', DoubleType(), True), 
                StructField('Co2e_input_lower', DoubleType(), True), 
                StructField('Co2e_input_upper', DoubleType(), True), 
                StructField('emission_upstream_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'emission_upstream_profile_company',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_upstream_profile_product_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_upstream_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('isic_categorisation', StringType(), True), 
                StructField('isic_4digit', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('multi_match', StringType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('input_name', StringType(), True), 
                StructField('Co2e_input_lower', StringType(), True), 
                StructField('Co2e_input_upper', StringType(), True), 
                StructField('tilt_sector_input', StringType(), True), 
                StructField('tilt_subsector_input', StringType(), True), 
                StructField('isic_categorisation_input', StringType(), True), 
                StructField('isic_4digit_input', StringType(), True), 
                StructField('input_unit', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Emission Upstream Profile produ.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'emission_upstream_profile_product_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('emission_upstream_category', StringType(), True), 
                StructField('benchmark', StringType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('isic_categorisation', StringType(), True), 
                StructField('isic_4digit', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('multi_match', BooleanType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('input_name', StringType(), True), 
                StructField('Co2e_input_lower', DoubleType(), True), 
                StructField('Co2e_input_upper', DoubleType(), True), 
                StructField('tilt_sector_input', StringType(), True), 
                StructField('tilt_subsector_input', StringType(), True), 
                StructField('isic_categorisation_input', StringType(), True), 
                StructField('isic_4digit_input', StringType(), True), 
                StructField('input_unit', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'emission_upstream_profile_product',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_profile_company_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_share_ew', StringType(), True), 
                StructField('sector_share_bc', StringType(), True), 
                StructField('sector_share_wc', StringType(), True), 
                StructField('sector_category', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('year', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True)
                ]
                ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Sector Profile company.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_profile_company_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_share_ew', DoubleType(), True), 
                StructField('sector_share_bc', DoubleType(), True), 
                StructField('sector_share_wc', DoubleType(), True), 
                StructField('sector_category', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('year', IntegerType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'sector_profile_company',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_profile_product_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_category', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('year', StringType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('multi_match', StringType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Sector Profile product.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_profile_product_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_category', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('year', IntegerType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('multi_match', BooleanType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'sector_profile_product',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_upstream_profile_company_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_upstream_share_ew', StringType(), True), 
                StructField('sector_upstream_share_bc', StringType(), True), 
                StructField('sector_upstream_share_wc', StringType(), True), 
                StructField('sector_upstream_category', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('year', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Sector Upstream Profile company.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_upstream_profile_company_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_upstream_share_ew', DoubleType(), True), 
                StructField('sector_upstream_share_bc', DoubleType(), True), 
                StructField('sector_upstream_share_wc', DoubleType(), True), 
                StructField('sector_upstream_category', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('year', IntegerType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('main_tilt_sector', StringType(), True), 
                StructField('main_tilt_sub_sector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'sector_upstream_profile_company',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_upstream_profile_product_landingzone': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_upstream_category', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('year', StringType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('multi_match', StringType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('input_name', StringType(), True), 
                StructField('input_unit', StringType(), True), 
                StructField('input_tilt_sector', StringType(), True), 
                StructField('input_tilt_subsector', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True)
            ]
            ), 
        'container': 'landingzone',
        'location': 'tiltProfiles/Sector Upstream Profile product.csv',
        'type': 'tiltData',
        'partition_column' : '',
        'quality_checks': []
    },
        'sector_upstream_profile_product_raw': {
        'columns' :  StructType([
                StructField('companies_id', StringType(), True), 
                StructField('company_name', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('sector_upstream_category', StringType(), True), 
                StructField('SERT', StringType(), True), 
                StructField('scenario', StringType(), True), 
                StructField('year', IntegerType(), True), 
                StructField('ep_product', StringType(), True), 
                StructField('matched_activity_name', StringType(), True), 
                StructField('matched_reference_product', StringType(), True), 
                StructField('unit', StringType(), True), 
                StructField('tilt_sector', StringType(), True), 
                StructField('tilt_subsector', StringType(), True), 
                StructField('multi_match', BooleanType(), True), 
                StructField('matching_certainty', StringType(), True), 
                StructField('matching_certainty_company_average', StringType(), True), 
                StructField('input_name', StringType(), True), 
                StructField('input_unit', StringType(), True), 
                StructField('input_tilt_sector', StringType(), True), 
                StructField('input_tilt_subsector', StringType(), True), 
                StructField('company_city', StringType(), True), 
                StructField('postcode', StringType(), True), 
                StructField('address', StringType(), True), 
                StructField('main_activity', StringType(), True), 
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ), 
        'container': 'raw',
        'location': 'sector_upstream_profile_product',
        'type': 'parquet',
        'partition_column' : '',
        'quality_checks': []
    }
        

    }

    if not table_name:
        return table_dict
    else:
        return table_dict[table_name]