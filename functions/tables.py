from pyspark.sql.types import StringType,StructType, StructField, DataType, BooleanType, FloatType, ShortType, TimestampType, IntegerType, DateType

def create_location_path():

    return None

def get_table_definition(table_name: str) -> dict:

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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'geographies_raw': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('Contained and Overlapping Geographies', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'geographies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'ID'],
                                ['format', 'Geographical Classification', r"[a-zA-Z\-]"]]
        },
        'geographies_transform': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'geographies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'ID']]
        },
        'geographies_related': {
            'columns' :  StructType([
                StructField('Shortname', StringType(), True),
                StructField('Shortname_related', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'geographies_related',
            'type': 'parquet',
            'partition_by' : '',
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
            'type': 'csv',
            'partition_by' : '',
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'undefined_ao',
            'type': 'parquet',
            'partition_by' : '',
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
            'type': 'csv',
            'partition_by' : '',
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'cutoff_ao',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'Activity UUID & Product UUID']]
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
            'type': 'csv',
            'partition_by' : '',
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'en15804_ao',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'Activity UUID & Product UUID']]
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
            'type': 'csv',
            'partition_by' : '',
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'consequential_ao',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'Activity UUID & Product UUID']]
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'products',
            'type': 'parquet',
            'partition_by' : '',
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'activities',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'Activity UUID']]
        },
        'products_activities_transformed': {
            'columns' :  StructType([
                StructField('Activity UUID & Product UUID', StringType(), False),
                StructField('Activity UUID', StringType(), False),
                StructField('Product UUID', StringType(), False),
                StructField('EcoQuery URL', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'transform',
            'location': 'products_activities',
            'type': 'parquet',
            'partition_by' : 'AO Method',
            'quality_checks': [['unique', 'Activity UUID & Product UUID']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'lcia_methods_raw': {
            'columns' :  StructType([
                StructField('Method Name', StringType(), False),
                StructField('Status', StringType(), False),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField('Links to Characterization Factor Successes', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'lcia_methods',
            'type': 'parquet',
            'partition_by' : '',
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
            'type': 'csv',
            'partition_by' : '',
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'impact_categories',
            'type': 'parquet',
            'partition_by' : '',
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'intermediate_exchanges_raw': {
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
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'intermediate_exchanges',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'elementary_exchanges_landingzone': {
            'columns' :  StructType([
                StructField('ID', StringType(), True),
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'elementary_exchanges_raw': {
            'columns' :  StructType([
                StructField('ID', StringType(), True),
                StructField('Name', StringType(), True),
                StructField('Compartment', StringType(), True),
                StructField('Sub Compartment', StringType(), True),
                StructField('Unit Name', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('Formula', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'elementary_exchanges',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'issues_companies_raw': {
            'columns' :  StructType([
                StructField('issues_companies_id', StringType(), False),
                StructField('issues_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'issues_companies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','issues_companies_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'issues_raw': {
            'columns' :  StructType([
                StructField('issues_id', StringType(), False),
                StructField('repo', StringType(), True),
                StructField('issue', StringType(), True),
                StructField('title', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'issues',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','issues_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'sea_food_companies_raw': {
            'columns' :  StructType([
                StructField('sea_food_companies_id', StringType(), False),
                StructField('sea_food_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sea_food_companies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','sea_food_companies_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'sea_food_raw': {
            'columns' :  StructType([
                StructField('sea_food_id', StringType(), False),
                StructField('company_name', StringType(), True),
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
                StructField('country', StringType(), True),
                StructField('sourcing_regions_identified', StringType(), True),
                StructField('list_of_species', StringType(), True),
                StructField('reporting_precision_pt_score', FloatType(), True),
                StructField('world_benchmarking_alliance_seafood_stewardship_index', FloatType(), True),
                StructField('ocean_health_index_score_2012', FloatType(), True),
                StructField('ocean_health_index_score_2021', FloatType(), True),
                StructField('ocean_health_index_score_percent_change_2021_2012', FloatType(), True),
                StructField('fish_source_score_management_quality', FloatType(), True),
                StructField('fish_source_score_managers_compliance', FloatType(), True),
                StructField('fish_source_score_fishers_compliance', FloatType(), True),
                StructField('fish_source_score_current_stock_health', FloatType(), True),
                StructField('fish_source_score_future_stock_health', FloatType(), True),
                StructField('sea_around_us_unreported_total_catch_percent', FloatType(), True),
                StructField('sea_around_us_bottom_trawl_total_catch_percent_35', FloatType(), True),
                StructField('sea_around_us_gillnets_total_catch_percent', FloatType(), True),
                StructField('global_fishing_index_data_availability_on_stock_sustainability', FloatType(), True),
                StructField('global_fishing_index_proportion_of_assessed_fish_stocks_that_is_sustainable', FloatType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_sustainable', FloatType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_overfished', FloatType(), True),
                StructField('global_fishing_index_proportion_of_1990_2018_catches_that_is_not_assessed', FloatType(), True),
                StructField('global_fishing_index_fisheries_governance_score', FloatType(), True),
                StructField('global_fishing_index_alignment_with_international_standards_for_protecting_worker_rights_and_safety_in_fisheries_assessment_score', FloatType(), True),
                StructField('global_fishing_index_fishery_subsidy_program_assessment_score', FloatType(), True),
                StructField('global_fishing_index_knowledge_on_fishing_fleets_assessment_score', FloatType(), True),
                StructField('global_fishing_index_compliance_monitoring_and_surveillance_programs_assessment_score', FloatType(), True),
                StructField('global_fishing_index_severity_of_fishery_sanctions_assessment_score', FloatType(), True),
                StructField('global_fishing_index_access_of_foreign_fishing_fleets_assessment_score', FloatType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sea_food',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','sea_food_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'products_companies_raw': {
            'columns' :  StructType([
                StructField('products_companies_id', StringType(), False),
                StructField('products_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'products_companies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','products_companies_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'companies_raw': {
            'columns' :  StructType([
                StructField('companies_id', StringType(), False),
                StructField('company_name', StringType(), True),
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
                StructField('country_id', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'companies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','companies_id']]
        },
         'main_activity_landingzone': {
            'columns' :  StructType([
                StructField('main_activity_id', StringType(), False),
                StructField('main_activity', StringType(), True),
                StructField('ecoinvent', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/main_activity.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'main_activity_raw': {
            'columns' :  StructType([
                StructField('main_activity_id', StringType(), False),
                StructField('main_activity', StringType(), True),
                StructField('ecoinvent', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'main_activity',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','main_activity_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'geography_raw': {
            'columns' :  StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), True),
                StructField('ecoinvent_geography', StringType(), True),
                StructField('priority', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'geography',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','geography_id']]
        },
         'country_landingzone': {
            'columns' :  StructType([
                StructField('country_id', StringType(), False),
                StructField('country', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/country.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'country_raw': {
            'columns' :  StructType([
                StructField('country_id', StringType(), False),
                StructField('country', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'country',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','country_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'delimited_products_raw': {
            'columns' :  StructType([
                StructField('delimited_products_id', StringType(), False),
                StructField('delimited_id', StringType(), True),
                StructField('products_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'delimited_products',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','delimited_products_id']]
        },
         'products_landingzone': {
            'columns' :  StructType([
                StructField('products_id', StringType(), False),
                StructField('products_and_services', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/products.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'products_raw': {
            'columns' :  StructType([
                StructField('products_id', StringType(), False),
                StructField('products_and_services', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'products',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','products_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'categories_companies_raw': {
            'columns' :  StructType([
                StructField('categories_companies_id', StringType(), False),
                StructField('categories_id', StringType(), True),
                StructField('companies_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'categories_companies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','categories_companies_id']]
        },
         'delimited_landingzone': {
            'columns' :  StructType([
                StructField('delimited_id', StringType(), False),
                StructField('delimited', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/delimited.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'delimited_raw': {
            'columns' :  StructType([
                StructField('delimited_id', StringType(), False),
                StructField('delimited', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'delimited',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','delimited_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'clustered_delimited_raw': {
            'columns' :  StructType([
                StructField('clustered_delimited_id', StringType(), False),
                StructField('clustered_id', StringType(), True),
                StructField('delimited_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'clustered_delimited',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','clustered_delimited_id']]
        },
        'clustered_landingzone': {
            'columns' :  StructType([
                StructField('clustered_id', StringType(), False),
                StructField('clustered', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/clustered.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'clustered_raw': {
            'columns' :  StructType([
                StructField('clustered_id', StringType(), False),
                StructField('clustered', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'clustered',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','clustered_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'categories_sector_ecoinvent_delimited_raw': {
            'columns' :  StructType([
                StructField('categories_sector_ecoinvent_delimited_id', StringType(), False),
                StructField('categories_id', StringType(), True),
                StructField('sector_ecoinvent_delimited_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'categories_sector_ecoinvent_delimited',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','categories_sector_ecoinvent_delimited_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'categories_raw': {
            'columns' :  StructType([
                StructField('categories_id', StringType(), False),
                StructField('group', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'categories',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','categories_id']]
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
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'sector_ecoinvent_delimited_sector_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent_delimited_id', StringType(), True),
                StructField('sector_ecoinvent_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_ecoinvent_delimited_sector_ecoinvent',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','sector_ecoinvent_delimited_sector_ecoinvent_id']]
        },
         'sector_ecoinvent_delimited_landingzone': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_id', StringType(), False),
                StructField('sector_ecoinvent_delimited', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sector_ecoinvent_delimited.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'sector_ecoinvent_delimited_raw': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_delimited_id', StringType(), False),
                StructField('sector_ecoinvent_delimited', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_ecoinvent_delimited',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','sector_ecoinvent_delimited_id']]
        },
        'sector_ecoinvent_landingzone': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent', StringType(), True)
            ]  
            ), 
            'container': 'landingzone',
            'location': 'tiltData/sector_ecoinvent.csv',
            'type': 'csv',
            'partition_by' : '',
            'quality_checks': []
        },
        'sector_ecoinvent_raw': {
            'columns' :  StructType([
                StructField('sector_ecoinvent_id', StringType(), False),
                StructField('sector_ecoinvent', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'sector_ecoinvent',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique','sector_ecoinvent_id']]
        },
        'product_matching_complete_all_cases_landingzone': {
            'columns' :  StructType([
                StructField('group_var', StringType(), False),
                StructField('ep_id', StringType(), True),
                StructField('ep_country', StringType(), True),
                StructField('ep_main_act', StringType(), True),
                StructField('ep_prod', StringType(), True),
                StructField('lca_id', StringType(), True),
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
            'partition_by' : '',
            'quality_checks': []
        },
        'product_matching_complete_all_cases_raw': {
            'columns' :  StructType([
                StructField('group_var', StringType(), False),
                StructField('ep_id', StringType(), True),
                StructField('lca_id', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'product_matching_complete_all_cases',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'labelled_activity_v1.0_landingzone': {
            'columns' :  StructType([
                StructField('', StringType(), False),
                StructField('index', StringType(), True),
                StructField('ep_act_id', StringType(), True),
                StructField('ep_country', StringType(), True),
                StructField('ep_main_act', StringType(), True),
                StructField('ep_act', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('Activity UUID', StringType(), True),
                StructField('Activity Name', StringType(), True),
                StructField('Activity UUID & Product UUID', StringType(), True),
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
            'partition_by' : '',
            'quality_checks': []
        },
        'labelled_activity_v1.0_raw': {
            'columns' :  StructType([
                StructField('ep_act_id', StringType(), False),
                StructField('ep_country', StringType(), True),
                StructField('Activity UUID & Product UUID', StringType(), True),
                StructField('tiltRecordID', StringType(), False),
            ]  
            ), 
            'container': 'raw',
            'location': 'labelled_activity_v1.0',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        }
        

    }

    return table_dict[table_name]