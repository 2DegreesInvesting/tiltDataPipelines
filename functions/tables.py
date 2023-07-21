from pyspark.sql.types import StringType,StructType, StructField
from pyspark.sql.types import DataType

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
                StructField('Contained and Overlapping Geographies', StringType(), True)
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
            ]  
            ), 
            'container': 'transform',
            'location': 'geographies_related',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'geographies_transform': {
            'columns' :  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
            ]  
            ), 
            'container': 'transform',
            'location': 'geographies',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': [['unique', 'ID']]
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
            ]  
            ), 
            'container': 'raw',
            'location': 'lcia_methods',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'lcia_methods_transform': {
            'columns' :  StructType([
                StructField('Method Name', StringType(), False),
                StructField('Status', StringType(), False),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField('Links to Characterization Factor Successes', StringType(), True),
            ]  
            ), 
            'container': 'transform',
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
            ]  
            ), 
            'container': 'raw',
            'location': 'impact_categories',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'impact_categories_transform': {
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
            'container': 'transform',
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
            ]  
            ), 
            'container': 'raw',
            'location': 'intermediate_exchanges',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'intermediate_exchanges_transform': {
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
            'container': 'transform',
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
            ]  
            ), 
            'container': 'raw',
            'location': 'elementary_exchanges',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
        'elementary_exchanges_transform': {
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
            'container': 'transform',
            'location': 'elementary_exchanges',
            'type': 'parquet',
            'partition_by' : '',
            'quality_checks': []
        },
    }

    return table_dict[table_name]