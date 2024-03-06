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
        'type': ['multiline','csv','delta','parquet'],
        'partition_column' : 'name_of_partition_column',
        'quality_checks': [['unique', ['string_column']],
                           ['format', 'string_column', r"[a-zA-Z\-]"]]
    }
"""
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType, ShortType, IntegerType, DateType, ByteType, TimestampType, DecimalType


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
        'test_table_landingzone': {
            'columns': StructType([
                StructField('test_string_column', StringType(), False),
                StructField('test_integer_column', IntegerType(), False),
                StructField('test_decimal_column', DoubleType(), True),
                StructField('test_date_column', DateType(), True),
            ]
            ),
            'container': 'landingzone',
            'location': 'test/test_table.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'test_table_raw': {
            'columns': StructType([
                StructField('test_string_column', StringType(), False),
                StructField('test_integer_column', IntegerType(), False),
                StructField('test_decimal_column', DoubleType(), True),
                StructField('test_date_column', DateType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'test',
            'location': 'test_table',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'companies_europages_landingzone': {
            'columns': StructType([
                StructField('company_name', StringType(), False),
                StructField('group', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('address', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('country', StringType(), False),
                StructField('products_and_services', StringType(), True),
                StructField('information', StringType(), True),
                StructField('min_headcount', StringType(), True),
                StructField('max_headcount', StringType(), True),
                StructField('type_of_building_for_registered_address',
                            StringType(), True),
                StructField('verified_by_europages', StringType(), True),
                StructField('year_established', StringType(), True),
                StructField('websites', StringType(), True),
                StructField('download_datetime', StringType(), True),
                StructField('id', StringType(), False),
                StructField('filename', StringType(), False)
            ]
            ),
            'container': 'landingzone',
            'location': 'tiltEP/',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': [['unique', ['id']]]
        },
        'companies_europages_raw': {
            'columns': StructType([
                StructField('company_name', StringType(), False),
                StructField('group', StringType(), True),
                StructField('sector', StringType(), True),
                StructField('subsector', StringType(), True),
                StructField('main_activity', StringType(), True),
                StructField('address', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('country', StringType(), False),
                StructField('products_and_services', StringType(), True),
                StructField('information', StringType(), True),
                StructField('min_headcount', IntegerType(), True),
                StructField('max_headcount', IntegerType(), True),
                StructField('type_of_building_for_registered_address',
                            StringType(), True),
                StructField('verified_by_europages', BooleanType(), True),
                StructField('year_established', IntegerType(), True),
                StructField('websites', StringType(), True),
                StructField('download_datetime', DateType(), True),
                StructField('id', StringType(), False),
                StructField('filename', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'companies_europages',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['id']]]
        },
        'companies_datamodel': {
            'columns': StructType([
                StructField('company_id', StringType(), False),
                StructField('country_un', StringType(), False),
                StructField('source_id', StringType(), False),
                StructField('company_name', StringType(), False),
                StructField('address', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('postcode', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'companies',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'companies_products_datamodel': {
            'columns': StructType([
                StructField('company_id', StringType(), False),
                StructField('product_id', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'companies_products',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'products_datamodel': {
            'columns': StructType([
                StructField('product_id', StringType(), False),
                StructField('product_name', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'products',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'country_landingzone': {
            'columns': StructType([
                StructField('country_id', StringType(), False),
                StructField('country', StringType(), False)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/country.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'country_raw': {
            'columns': StructType([
                StructField('country_id', StringType(), False),
                StructField('country', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'country',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'main_activity_ecoinvent_mapper_landingzone': {
            'columns': StructType([
                StructField('main_activity_id', StringType(), False),
                StructField('main_activity', StringType(), True),
                StructField('ei_activity_name', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/main_activity_ecoinvent_mapper.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'main_activity_ecoinvent_mapper_raw': {
            'columns': StructType([
                StructField('main_activity_id', StringType(), False),
                StructField('main_activity', StringType(), True),
                StructField('ei_activity_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'main_activity_ecoinvent_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'sources_mapper_landingzone': {
            'columns': StructType([
                StructField('source_id', StringType(), False),
                StructField('source_name', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/sources_mapper.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'sources_mapper_raw': {
            'columns': StructType([
                StructField('source_id', StringType(), False),
                StructField('source_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'sources_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'sources_mapper_datamodel': {
            'columns': StructType([
                StructField('source_id', StringType(), False),
                StructField('source_name', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'sources_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'countries_mapper_landingzone': {
            'columns': StructType([
                StructField('country_un', StringType(), False),
                StructField('country', StringType(), False)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/countries_mapper.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'countries_mapper_raw': {
            'columns': StructType([
                StructField('country_un', StringType(), False),
                StructField('country', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'countries_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'countries_mapper_datamodel': {
            'columns': StructType([
                StructField('country_un', StringType(), False),
                StructField('country', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'countries_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'geography_mapper_landingzone': {
            'columns': StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), False),
                StructField('lca_geo', StringType(), False),
                StructField('priority', StringType(), False),
                StructField('input_priority', StringType(), False)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/geography_mapper.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'geography_ecoinvent_mapper_raw': {
            'columns': StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_id', StringType(), False),
                StructField('lca_geo', StringType(), False),
                StructField('priority', ByteType(), False),
                StructField('input_priority', ByteType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'geography_ecoinvent_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'geography_ecoinvent_mapper_datamodel': {
            'columns': StructType([
                StructField('geography_id', StringType(), False),
                StructField('country_un', StringType(), False),
                StructField('ecoinvent_geography', StringType(), False),
                StructField('priority', ByteType(), False),
                StructField('input_priority', ByteType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'geography_ecoinvent_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'EP_tilt_sector_mapper_landingzone': {
            'columns':  StructType([
                StructField('categories_id', StringType(), False),
                StructField('group', StringType(), True),
                StructField('ep_sector', StringType(), False),
                StructField('ep_subsector', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/EP_tilt_sector_mapper.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'EP_tilt_sector_unmatched_mapper_raw': {
            'columns':  StructType([
                StructField('categories_id', StringType(), False),
                StructField('group', StringType(), True),
                StructField('ep_sector', StringType(), False),
                StructField('ep_subsector', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'EP_tilt_sector_unmatched_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'EP_tilt_sector_unmatched_mapper_datamodel': {
            'columns':  StructType([
                StructField('categories_id', StringType(), False),
                StructField('group', StringType(), True),
                StructField('ep_sector', StringType(), False),
                StructField('ep_subsector', StringType(), True),
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'EP_tilt_sector_unmatched_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_isic_mapper_2023-07-20_landingzone': {
            'columns': StructType([
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_section', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/tilt_isic_mapper_2023-07-20.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_sector_isic_mapper_raw': {
            'columns': StructType([
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_section', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'tilt_sector_isic_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_sector_isic_mapper_datamodel': {
            'columns': StructType([
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('isic_section', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'tilt_sector_isic_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_tilt_mapper_2023-07-20_landingzone': {
            'columns': StructType([
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('weo_product', StringType(), True),
                StructField('weo_flow', StringType(), True),
                StructField('ipr_sector', StringType(), True),
                StructField('ipr_subsector', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'mappers/scenario_tilt_mapper_2023-07-20.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_sector_scenario_mapper_raw': {
            'columns': StructType([
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('weo_product', StringType(), True),
                StructField('weo_flow', StringType(), True),
                StructField('ipr_sector', StringType(), True),
                StructField('ipr_subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'tilt_sector_scenario_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'tilt_sector_scenario_mapper_datamodel': {
            'columns': StructType([
                StructField('tilt_sector', StringType(), True),
                StructField('tilt_subsector', StringType(), True),
                StructField('scenario_type', StringType(), True),
                StructField('scenario_sector', StringType(), True),
                StructField('scenario_subsector', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'tilt_sector_scenario_mapper',
            'type': 'delta',
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
                StructField('Year', StringType(), False),
                StructField('Value', StringType(), False),
                StructField('Reductions', StringType(), False)
            ]
            ),
            'container': 'landingzone',
            'location': 'scenario/scenario_targets_IPR_NEW.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_IPR_raw': {
            'columns':  StructType([
                StructField('Scenario', StringType(), False),
                StructField('Region', StringType(), True),
                StructField('Variable_Class', StringType(), True),
                StructField('Sub_Variable_Class', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('Sub_Sector', StringType(), True),
                StructField('Units', StringType(), True),
                StructField('Year', ShortType(), False),
                StructField('Value', DoubleType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'scenario_targets_IPR',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_IPR_datamodel': {
            'columns':  StructType([
                StructField('scenario_targets_ipr_id', StringType(), False),
                StructField('scenario', StringType(), True),
                StructField('region', StringType(), True),
                StructField('ipr_sector', StringType(), True),
                StructField('ipr_subsector', StringType(), True),
                StructField('year', ShortType(), False),
                StructField('value', DoubleType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'scenario_targets_IPR',
            'type': 'delta',
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
                StructField('VALUE', StringType(), False),
                StructField('REDUCTIONS', StringType(), False)
            ]
            ),
            'container': 'landingzone',
            'location': 'scenario/scenario_targets_WEO_NEW.csv',
            'type': 'csv',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_WEO_raw': {
            'columns':  StructType([
                StructField('PUBLICATION', StringType(), False),
                StructField('SCENARIO', StringType(), True),
                StructField('CATEGORY', StringType(), True),
                StructField('PRODUCT', StringType(), True),
                StructField('FLOW', StringType(), True),
                StructField('UNIT', StringType(), True),
                StructField('REGION', StringType(), True),
                StructField('YEAR', ShortType(), True),
                StructField('VALUE', DoubleType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'scenario_targets_WEO',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'scenario_targets_WEO_datamodel': {
            'columns':  StructType([
                StructField('scenario_targets_weo_id', StringType(), False),
                StructField('scenario', StringType(), True),
                StructField('region', StringType(), True),
                StructField('weo_sector', StringType(), True),
                StructField('weo_subsector', StringType(), True),
                StructField('year', ShortType(), True),
                StructField('value', DoubleType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'scenario_targets_WEO',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'geographies_landingzone': {
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical_Classification', StringType(), True),
                StructField('Contained_and_Overlapping_Geographies',
                            StringType(), True),
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Geographies.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'geographies_raw': {
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical_Classification', StringType(), True),
                StructField('Contained_and_Overlapping_Geographies',
                            StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'geographies',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['ID']],
                               ['format', 'Geographical Classification', r"[a-zA-Z\-]"]]
        },
        'geographies_transform': {
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Shortname', StringType(), True),
                StructField('Geographical_Classification', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'transform',
            'location': 'geographies',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['ID']]]
        },
        'geographies_related': {
            'columns':  StructType([
                StructField('Shortname', StringType(), True),
                StructField('Shortname_related', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'transform',
            'location': 'geographies_related',
            'type': 'delta',
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
                StructField('CAS Number', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Undefined AO.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'undefined_ao_raw': {
            'columns':  StructType([
                StructField('Activity_UUID', StringType(), False),
                StructField('EcoQuery_URL', StringType(), True),
                StructField('Activity_Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time_Period', StringType(), True),
                StructField('Special_Activity_Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC_Classification', StringType(), True),
                StructField('ISIC_Section', StringType(), True),
                StructField('Product_UUID', StringType(), False),
                StructField('Product_Group', StringType(), True),
                StructField('Product_Name', StringType(), True),
                StructField('CPC_Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product_Information', StringType(), True),
                StructField('CAS_Number', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'undefined_ao',
            'type': 'delta',
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
                StructField('c', StringType(), True),
                StructField('ISIC Section', StringType(), True),
                StructField('Product UUID', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('CPC Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product Information', StringType(), True),
                StructField('CAS Number', StringType(), True),
                StructField('Cut-Off Classification', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Cut-OFF AO.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'cut_off_ao_raw': {
            'columns':  StructType([
                StructField('Activity_UUID_&_Product_UUID',
                            StringType(), False),
                StructField('Activity_UUID', StringType(), False),
                StructField('EcoQuery_URL', StringType(), True),
                StructField('Activity_Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time_Period', StringType(), True),
                StructField('Special_Activity_Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC_Classification', StringType(), True),
                StructField('ISIC_Section', StringType(), True),
                StructField('Product_UUID', StringType(), False),
                StructField('Reference_Product_Name', StringType(), True),
                StructField('CPC_Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product_Information', StringType(), True),
                StructField('CAS_Number', StringType(), True),
                StructField('Cut_Off_Classification', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'cutoff_ao',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'ecoinvent_cut_off_datamodel': {
            'columns':  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('activity_uuid', StringType(), False),
                StructField('product_uuid', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'ecoinvent_cut_off',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['activity_uuid_product_uuid']]]
        },
        'ecoinvent_product_datamodel': {
            'columns':  StructType([
                StructField('product_uuid', StringType(), False),
                StructField('reference_product_name', StringType(), True),
                StructField('unit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'ecoinvent_product',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['product_uuid']]]
        },
        'ecoinvent_activity_datamodel': {
            'columns':  StructType([
                StructField('activity_uuid', StringType(), False),
                StructField('activity_name', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'ecoinvent_activity',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['activity_uuid']]]
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
                StructField('Cut-Off Classification', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/EN15804 AO.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'en15804_ao_raw': {
            'columns':  StructType([
                StructField('Activity_UUID_&_Product_UUID',
                            StringType(), False),
                StructField('Activity_UUID', StringType(), False),
                StructField('EcoQuery_URL', StringType(), True),
                StructField('Activity_Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time_Period', StringType(), True),
                StructField('Special_Activity_Type', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC_Classification', StringType(), True),
                StructField('ISIC_Section', StringType(), True),
                StructField('Product_UUID', StringType(), False),
                StructField('Reference_Product_Name', StringType(), True),
                StructField('CPC_Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product_Information', StringType(), True),
                StructField('CAS_Number', StringType(), True),
                StructField('Cut-Off_Classification', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'en15804_ao',
            'type': 'delta',
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
                StructField('CAS Number', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Consequential AO.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'consequential_ao_raw': {
            'columns':  StructType([
                StructField('Activity_UUID_&_Product_UUID',
                            StringType(), False),
                StructField('Activity_UUID', StringType(), False),
                StructField('EcoQuery_URL', StringType(), True),
                StructField('Activity_Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Time_Period', StringType(), True),
                StructField('Special_Activity_Type', StringType(), True),
                StructField('Technology_Level', StringType(), True),
                StructField('Sector', StringType(), True),
                StructField('ISIC_Classification', StringType(), True),
                StructField('ISIC_Section', StringType(), True),
                StructField('Product_UUID', StringType(), False),
                StructField('Reference_Product_Name', StringType(), True),
                StructField('CPC_Classification', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Product_Information', StringType(), True),
                StructField('CAS_Number', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'consequential_ao',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['Activity UUID & Product UUID']]]
        },
        'lcia_methods_landingzone': {
            'columns':  StructType([
                StructField('Method Name', StringType(), True),
                StructField('Status', StringType(), True),
                StructField('Method Version', StringType(), True),
                StructField('Further Documentation', StringType(), True),
                StructField(
                    'Links to Characterization Factor Successes', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/LCIA Methods.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'lcia_methods_raw': {
            'columns':  StructType([
                StructField('Method_Name', StringType(), False),
                StructField('Status', StringType(), False),
                StructField('Method_Version', StringType(), True),
                StructField('Further_Documentation', StringType(), True),
                StructField(
                    'Links_to_Characterization_Factor_Successes', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'lcia_methods',
            'type': 'delta',
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
                StructField('Unit in method', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Impact Categories.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'impact_categories_raw': {
            'columns':  StructType([
                StructField('Resources_Emissions_Total',
                            StringType(), True),
                StructField('Main_impact_damage_category', StringType(), True),
                StructField('Inventory_Midpoint_Endpoint_AoP',
                            StringType(), True),
                StructField('Area_of_Protection_AoP', StringType(), True),
                StructField('Used_in_EN15804', StringType(), True),
                StructField('Method', StringType(), True),
                StructField('Category', StringType(), True),
                StructField('Indicator', StringType(), True),
                StructField('Unit', StringType(), True),
                StructField('Category_name_in_method', StringType(), True),
                StructField('Indicator_name_in_method', StringType(), True),
                StructField('Unit_in_method', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'impact_categories',
            'type': 'delta',
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
                StructField('Synonym', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Intermediate Exchanges.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'intermediate_exchanges_raw': {
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Unit_Name', StringType(), True),
                StructField('CAS_Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('By_product_Classification', StringType(), True),
                StructField('CPC_Classification', StringType(), True),
                StructField('Product_Information', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'intermediate_exchanges',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['ID']]]
        },
        'intermediate_exchanges_datamodel': {
            'columns':  StructType([
                StructField('exchange_id', StringType(), False),
                StructField('exchange_name', StringType(), True),
                StructField('unit_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'intermediate_exchanges',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['exchange_id']]]
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
                StructField('Formula', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            'location': 'ecoInvent/Elementary Exchanges.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'elementary_exchanges_raw': {
            'columns':  StructType([
                StructField('ID', StringType(), False),
                StructField('Name', StringType(), True),
                StructField('Compartment', StringType(), True),
                StructField('Sub_Compartment', StringType(), True),
                StructField('Unit_Name', StringType(), True),
                StructField('CAS_Number', StringType(), True),
                StructField('Comment', StringType(), True),
                StructField('Synonym', StringType(), True),
                StructField('Formula', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'elementary_exchanges',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': [['unique', ['ID']]]
        },
        'cut-off_cumulative_LCIA_v3.9.1_landingzone': {
            'columns':  StructType([
                StructField('Activity UUID_Product UUID', StringType(), False),
                StructField('Activity Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Reference Product Name', StringType(), True),
                StructField('Reference Product Unit', StringType(), True),
                StructField(
                    'IPCC 2021 climate change global warming potential (GWP100) kg CO2-Eq', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            # file that is created by extracting certain columns from the licensed data
            'location': 'ecoInvent/cut-off_cumulative_LCIA_v3.9.1.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'ecoinvent_co2_raw': {
            'columns':  StructType([
                StructField('Activity_UUID_Product_UUID', StringType(), False),
                StructField('Activity_Name', StringType(), True),
                StructField('Geography', StringType(), True),
                StructField('Reference_Product_Name', StringType(), True),
                StructField('Reference_Product_Unit', StringType(), True),
                StructField(
                    'IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq', DecimalType(15, 10), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'ecoinvent_co2',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'ecoinvent_co2_datamodel': {
            'columns':  StructType([
                StructField('activity_uuid_product_uuid', StringType(), False),
                StructField('co2_footprint', DecimalType(15, 10), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'ecoinvent_co2',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'ecoinvent_input_data_relevant_columns_landingzone': {
            'columns':  StructType([
                StructField('activityId', StringType(), True),
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
                StructField(
                    'By-product classification_classificationValue', StringType(), True)
            ]
            ),
            'container': 'landingzone',
            # extract from Ecoinvent portal (licensed)
            'location': 'ecoInvent/ecoinvent_input_data_relevant_columns.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'ecoinvent_input_data_raw': {
            'columns':  StructType([
                StructField('activityId', StringType(), True),
                StructField('activityName', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('reference_product', StringType(), True),
                StructField('group', StringType(), True),
                StructField('exchange_name', StringType(), True),
                StructField('activityLinkId', StringType(), True),
                StructField('activityLink_activityName', StringType(), True),
                StructField('activityLink_geography', StringType(), True),
                StructField('exchange_unitName', StringType(), True),
                StructField('exchange_amount', DecimalType(25, 10), True),
                StructField('CPC_classificationValue', StringType(), True),
                StructField(
                    'By_product_classification_classificationValue', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'ecoinvent_input_data',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'ecoinvent_input_data_datamodel': {
            'columns':  StructType([
                StructField('activityId', StringType(), True),
                StructField('activityName', StringType(), True),
                StructField('geography', StringType(), True),
                StructField('reference_product', StringType(), True),
                StructField('group', StringType(), True),
                StructField('exchange_name', StringType(), True),
                StructField('activityLinkId', StringType(), True),
                StructField('activityLink_activityName', StringType(), True),
                StructField('activityLink_geography', StringType(), True),
                StructField('exchange_unitName', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'ecoinvent_input_data',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
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
            'type': 'multiline',
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
            'location': 'scenario_tilt_mapper_2023-07-20',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        # 'geography_mapper_landingzone': {
        #     'columns':  StructType([
        #         StructField('geography_id', StringType(), False),
        #         StructField('country_id', StringType(), True),
        #         StructField('lca_geo', StringType(), True),
        #         StructField('priority', StringType(), True),
        #         StructField('input_priority', StringType(), True)
        #     ]
        #     ),
        #     'container': 'landingzone',
        #     'location': 'tiltIndicatorBefore/geography_mapper.csv',
        #     'type': 'multiline',
        #     'partition_column': '',
        #     'quality_checks': []
        # },
        # 'geography_mapper_raw': {
        #     'columns':  StructType([
        #         StructField('geography_id', StringType(), False),
        #         StructField('country_id', StringType(), True),
        #         StructField('lca_geo', StringType(), True),
        #         StructField('priority', StringType(), True),
        #         StructField('input_priority', StringType(), True),
        #         StructField('from_date', DateType(), False),
        #         StructField('to_date', DateType(), False),
        #         StructField('tiltRecordID', StringType(), False),
        #     ]
        #     ),
        #     'container': 'raw',
        #     'location': 'geography_mapper',
        #     'type': 'delta',
        #     'partition_column': '',
        #     'quality_checks': []
        # },
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
            'type': 'delta',
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
            'type': 'delta',
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
            'type': 'delta',
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
                StructField('reduction_targets', DoubleType(), True),
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
                StructField('reduction_targets', DoubleType(), True),
                StructField('geography', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'emission_profile_product',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'emission_upstream_profile_company_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('company_city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_usptream_profile_share',
                            DoubleType(), True),
                StructField('emission_usptream_profile', StringType(), True),
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
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'emission_upstream_profile_product_landingzone': {
            'columns':  StructType([
                StructField('companies_id', StringType(), True),
                StructField('company_name', StringType(), True),
                StructField('country', StringType(), True),
                StructField('emission_usptream_profile', StringType(), True),
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
                StructField('reduction_targets', DoubleType(), True)
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
                StructField('reduction_targets', DoubleType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'emission_upstream_profile_product',
            'type': 'delta',
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
            'type': 'delta',
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
                StructField('reduction_targets', DoubleType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('sector_scenario', StringType(), True),
                StructField('subsector_scenario', StringType(), True),
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
                StructField('reduction_targets', DoubleType(), True),
                StructField('isic_4digit', StringType(), True),
                StructField('sector_scenario', StringType(), True),
                StructField('subsector_scenario', StringType(), True),
                StructField('isic_4digit_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'sector_profile_product',
            'type': 'delta',
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
            'type': 'delta',
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
                StructField('reduction_targets', DoubleType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('sector_scenario', StringType(), True),
                StructField('subsector_scenario', StringType(), True),
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
                StructField('reduction_targets', DoubleType(), True),
                StructField('input_isic_4digit', StringType(), True),
                StructField('sector_scenario', StringType(), True),
                StructField('subsector_scenario', StringType(), True),
                StructField('input_isic_4digit_name', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'sector_upstream_profile_product',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'isic_4_digit_codes_landingzone': {
            'columns':  StructType([
                StructField('ISIC Rev 4 label', StringType(), True),
                StructField('Code', StringType(), True),
                StructField('Section (1-digit)', StringType(), True),
                StructField('Division (2-digit)', StringType(), True),
                StructField('Group (3-digit)', StringType(), True),
                StructField('Inclusions', IntegerType(), True),
                StructField('Exclusions', StringType(), True),
            ]
            ),
            'container': 'landingzone',
            'location': 'activityCodes/ISIC4DigitCodes.csv',
            'type': 'multiline',
            'partition_column': '',
            'quality_checks': []
        },
        'isic_mapper_raw': {
            'columns':  StructType([
                StructField('ISIC_Rev_4_label', StringType(), False),
                StructField('Code', StringType(), False),
                StructField('Section_1_digit', StringType(), False),
                StructField('Division_2_digit', StringType(), True),
                StructField('Group_3_digit', StringType(), True),
                StructField('Inclusions', IntegerType(), True),
                StructField('Exclusions', StringType(), True),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'raw',
            'location': 'isic_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        },
        'isic_mapper_datamodel': {
            'columns':  StructType([
                StructField('isic_4digit', StringType(), False),
                StructField('isic_4digit_name', StringType(), False),
                StructField('from_date', DateType(), False),
                StructField('to_date', DateType(), False),
                StructField('tiltRecordID', StringType(), False)
            ]
            ),
            'container': 'datamodel',
            'location': 'isic_mapper',
            'type': 'delta',
            'partition_column': '',
            'quality_checks': []
        }


    }

    if not table_name:
        return table_dict

    return table_dict[table_name]
