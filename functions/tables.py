"""
This module contains the definition of various tables used in the datahub.

Each table is defined as a dictionary with the following keys:
- 'columns': A StructType object defining the schema of the table.
- 'container': The name of the container where the table is stored.
- 'location': The location of the table within the container.
- 'type': The data type of the table.
- 'partition_column': The name of the partition column in the table.
- 'quality_checks': A list of quality checks to be performed on the table.

The `get_table_definition` function is used to retrieve the definition of a specific table.

Example:
    'table_name_container': {
        'columns' :  StructType([
            StructField('string_column', StringType(), False),
            StructField('integer_column', IntegerType(), True),
            StructField('decimal_column', DoubleType(), True),
            StructField('Date_column', DateType(), True),
        ]), 
        'container': 'container_name',
        'location': 'location_in_container',
        'type': ['tiltData','ecoInvent','csv','parquet'],
        'partition_column' : 'name_of_partition_column',
        'quality_checks': [['unique', ['string_column']],
                           ['format', 'string_column', r"[a-zA-Z\-]"]]
    }
"""
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType, ShortType, IntegerType, DateType


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
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('Contained and Overlapping Geographies',
                            StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Geographies.csv',
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'geographies_raw': {
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical Classification', StringType(), True),
                StructField('Contained and Overlapping Geographies',
                            StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]
            ),
            'container': 'raw',
            'location': 'geographies',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': [['unique', ['ID']],
                               ['format', 'Geographical Classification', r"[a-zA-Z\-]"]]
        },
        'geographies_transform': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': [['unique', ['ID']]]
        },
        'geographies_related': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'undefined_ao_landingzone': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'undefined_ao_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'cut_off_ao_landingzone': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), True),
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
            'partition_column': '',
            'quality_checks': []
        },
        'cut_off_ao_raw': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), False),
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
            'partition_column': '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'en15804_ao_landingzone': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), True),
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
            'partition_column': '',
            'quality_checks': []
        },
        'en15804_ao_raw': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), False),
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
            'partition_column': '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'consequential_ao_landingzone': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), True),
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
            'partition_column': '',
            'quality_checks': []
        },
        'consequential_ao_raw': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), False),
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
            'partition_column': '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'products_transformed': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'activities_transformed': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': [['unique', ['Activity UUID']]]
        },
        'products_activities_transformed': {
            'columns':  StructType([
                StructField('Activity UUID & Product UUID',
                            StringType(), False),
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
            'partition_column': 'AO Method',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'lcia_methods_landingzone': {
            'columns':  StructType([
                StructField('Method Name', StringType(), True),
                StructField('Status', StringType(), True),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField(
                    'Links to Characterization Factor Successes', StringType(), True),
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/LCIA Methods.csv',
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'lcia_methods_raw': {
            'columns':  StructType([
                StructField('Method Name', StringType(), False),
                StructField('Status', StringType(), False),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField(
                    'Links to Characterization Factor Successes', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]
            ),
            'container': 'raw',
            'location': 'lcia_methods',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'impact_categories_landingzone': {
            'columns':  StructType([
                StructField('Resources - Emissions - Total',
                            StringType(), True),
                StructField('Main impact/damage category', StringType(), True),
                StructField('Inventory - Midpoint - Endpoint - AoP',
                            StringType(), True),
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
            'partition_column': '',
            'quality_checks': []
        },
        'impact_categories_raw': {
            'columns':  StructType([
                StructField('Resources - Emissions - Total',
                            StringType(), True),
                StructField('Main impact/damage category', StringType(), True),
                StructField('Inventory - Midpoint - Endpoint - AoP',
                            StringType(), True),
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
            'partition_column': '',
            'quality_checks': []
        },
        'intermediate_exchanges_landingzone': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'intermediate_exchanges_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': [['unique', ['ID']]]
        },
        'elementary_exchanges_landingzone': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'elementary_exchanges_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': [['unique', ['ID']]]
        },
        'ep_ei_matcher_landingzone': {
            'columns':  StructType([
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
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'ep_ei_matcher_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'ep_ei_matcher_eurocaps_landingzone': {
            'columns':  StructType([
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
            'location': 'tiltIndicatorBefore/20231213_eurocaps_mapper_ep_ei.csv',
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'ep_ei_matcher_eurocaps_raw': {
            'columns':  StructType([
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
            'location': 'ep_ei_matcher_eurocaps',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_IPR_NEW_landingzone': {
            'columns':  StructType([
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
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_IPR_NEW_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_WEO_NEW_landingzone': {
            'columns':  StructType([
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
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_WEO_NEW_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_tilt_mapper_2023-07-20_landingzone': {
            'columns':  StructType([
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
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_tilt_mapper_2023-07-20_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_isic_mapper_2023-07-20_landingzone': {
            'columns':  StructType([
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
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_isic_mapper_2023-07-20_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'geography_mapper_landingzone': {
            'columns':  StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), True),
                StructField('lca_geo', StringType(), True),
                StructField('priority', StringType(), True),
                StructField('input_priority', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'tiltIndicatorBefore/geography_mapper.csv',
            'type': 'ecoInvent',
            'partition_column': '',
            'quality_checks': []
        },
        'geography_mapper_raw': {
            'columns':  StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), True),
                StructField('lca_geo', StringType(), True),
                StructField('priority', StringType(), True),
                StructField('input_priority', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False),
            ]
            ),
            'container': 'raw',
            'location': 'geography_mapper',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'mapper_ep_ei_landingzone': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'mapper_ep_ei_raw': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'dummy_quality_check': {
            'columns':  StructType([
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
            'partition_column': '',
            'quality_checks': []
        },
        'monitoring_values': {
            'columns':  StructType([
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
            'partition_column': 'table_name',
            'quality_checks': []
        },
        'emission_profile_company_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_profile_share', DoubleType(), True),
                StructField('emission_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/emission_profile_company',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'emission_profile_company_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_profile_share', DoubleType(), True),
                StructField('emission_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
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
            'location': 'emission_profile_company',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'emission_profile_product_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_4digit_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('geography', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/emission_profile_product',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'emission_profile_product_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_4digit_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('geography', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'emission_profile_product',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'emission_upstream_profile_company_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_upstream_profile_share',
                            DoubleType(), True),
                StructField('emission_upstream_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/emissions_profile_upstream_at_company_level',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'emission_upstream_profile_company_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_upstream_profile_share',
                            DoubleType(), True),
                StructField('emission_upstream_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'emission_upstream_profile_company',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'emission_upstream_profile_product_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_upstream_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('input_isic_4digit_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/emissions_profile_upstream_at_product_level',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'emission_upstream_profile_product_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_upstream_profile', StringType(), True),
                StructField('benchmark', StringType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('input_isic_4digit_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'emission_upstream_profile_product',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'sector_profile_company_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile_share', DoubleType(), True),
                StructField('sector_profile', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/sector_profile_at_company_level',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'sector_profile_company_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile_share', DoubleType(), True),
                StructField('sector_profile', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
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
            'location': 'sector_profile_company',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'sector_profile_product_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile', StringType(), True),
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
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('isic_4digit_name', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/sector_profile_at_product_level',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'sector_profile_product_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile', StringType(), True),
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
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('isic_4digit_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'sector_profile_product',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'sector_upstream_profile_company_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile_upstream_share',
                            DoubleType(), True),
                StructField('sector_profile_upstream', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/sector_profile_upstream_at_company_level',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'sector_upstream_profile_company_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile_upstream_share',
                            DoubleType(), True),
                StructField('sector_profile_upstream', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
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
            'location': 'sector_upstream_profile_company',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        },
        'sector_upstream_profile_product_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile_upstream', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', IntegerType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('input_isic_4digit_name', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'wrapperOutput/sector_profile_upstream_at_product_level',
            'type': 'parquet',
            'partition_column': 'batch',
            'quality_checks': []
        },
        'sector_upstream_profile_product_raw': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('sector_profile_upstream', StringType(), True),
                StructField('scenario', StringType(), True),
                StructField('year', IntegerType(), True),
                StructField('ep_product', StringType(), True),
                StructField('matched_activity_name', StringType(), True),
                StructField('matched_reference_product', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('multi_match', BooleanType(), True),
                StructField('matching_certainty', StringType(), True),
                StructField('matching_certainty_company_average',
                            StringType(), True),
                StructField('input_name', StringType(), True),
                StructField('input_unit', StringType(), True),
                StructField('input_tilt_sector', StringType(), True),
                StructField('input_tilt_subsector', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('address', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('activity_uuid_product_uuid', StringType(), True),
                StructField('profile_ranking', DoubleType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('input_isic_4digit_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'sector_upstream_profile_product',
            'type': 'parquet',
            'partition_column': '',
            'quality_checks': []
        }


    }

    if not table_name:
        return table_dict

    return table_dict[table_name]
