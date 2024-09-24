"""
This module contains the definition of the tables used in the datahub from the datamodel layer.

"""

from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    DoubleType,
    ShortType,
    DateType,
    ByteType,
    DecimalType,
    IntegerType
)

datamodel_schema = {
    "companies_datamodel": {
        "columns": StructType(
            [
                StructField("company_id", StringType(), False),
                StructField("country_un", StringType(), False),
                StructField("source_id", StringType(), False),
                StructField("company_name", StringType(), False),
                StructField("company_description", StringType(), True),
                StructField("address", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "companies",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [{"check": "values are unique", "columns": ["company_id"]}],
    },
    "companies_match_result_datamodel": {
        "columns": StructType(
            [
                StructField("europages_company_id", StringType(), False),
                StructField("companyinfo_company_id", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "companies_match_result",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "companies_EP_products_datamodel": {
        "columns": StructType(
            [
                StructField("company_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "companies_EP_products",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "EP_products_datamodel": {
        "columns": StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "EP_products",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "SBI_activities_datamodel": {
        "columns": StructType(
            [
                StructField("sbi_code", StringType(), False),
                StructField("sbi_code_description", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "SBI_activities",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "companies_SBI_activities_datamodel": {
        "columns": StructType(
            [
                StructField("company_id", StringType(), False),
                StructField("sbi_code", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "companies_SBI_activities",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "sources_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("source_id", StringType(), False),
                StructField("source_name", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "sources_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "countries_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("country_un", StringType(), False),
                StructField("country", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "countries_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "geography_ecoinvent_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("geography_id", StringType(), False),
                StructField("country_un", StringType(), False),
                StructField("ecoinvent_geography", StringType(), False),
                StructField("priority", ByteType(), False),
                StructField("input_priority", ByteType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "geography_ecoinvent_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "EP_tilt_sector_unmatched_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("categories_id", StringType(), False),
                StructField("group", StringType(), True),
                StructField("ep_sector", StringType(), False),
                StructField("ep_subsector", StringType(), True),
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "EP_tilt_sector_unmatched_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tilt_sector_isic_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("isic_4digit", StringType(), True),
                StructField("isic_section", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "tilt_sector_isic_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tilt_sector_scenario_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("scenario_type", StringType(), True),
                StructField("scenario_sector", StringType(), True),
                StructField("scenario_subsector", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "tilt_sector_scenario_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_targets_IPR_datamodel": {
        "columns": StructType(
            [
                StructField("scenario_targets_ipr_id", StringType(), False),
                StructField("scenario", StringType(), True),
                StructField("region", StringType(), True),
                StructField("ipr_sector", StringType(), True),
                StructField("ipr_subsector", StringType(), True),
                StructField("year", ShortType(), False),
                StructField("value", DoubleType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "scenario_targets_IPR",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_targets_WEO_datamodel": {
        "columns": StructType(
            [
                StructField("scenario_targets_weo_id", StringType(), False),
                StructField("scenario", StringType(), True),
                StructField("region", StringType(), True),
                StructField("weo_sector", StringType(), True),
                StructField("weo_subsector", StringType(), True),
                StructField("year", ShortType(), True),
                StructField("value", DoubleType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "scenario_targets_WEO",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "ecoinvent_cut_off_datamodel": {
        "columns": StructType(
            [
                StructField("activity_uuid_product_uuid", StringType(), False),
                StructField("activity_uuid", StringType(), False),
                StructField("product_uuid", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "ecoinvent_cut_off",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": [
                "activity_uuid_product_uuid"]},
        ],
    },
    'ecoinvent_product_datamodel': {
        'columns':  StructType([
            StructField('product_uuid', StringType(), False),
            StructField('reference_product_name', StringType(), True),
            StructField('unit', StringType(), True),
            StructField('cpc_code', StringType(), True),
            StructField('cpc_name', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        "container": "datamodel",
        "location": "ecoinvent_product",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": ["product_uuid"]},
        ],
    },
    'ecoinvent_activity_datamodel': {
        'columns':  StructType([
            StructField('activity_uuid', StringType(), False),
            StructField('activity_name', StringType(), True),
            StructField('activity_type', StringType(), True),
            StructField('geography', StringType(), True),
            StructField('isic_4digit', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        "container": "datamodel",
        "location": "ecoinvent_activity",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": ["activity_uuid"]},
        ],
    },
    "intermediate_exchanges_datamodel": {
        "columns": StructType(
            [
                StructField("exchange_id", StringType(), False),
                StructField("exchange_name", StringType(), True),
                StructField("unit_name", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "intermediate_exchanges",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": ["exchange_id"]},
        ],
    },
    "ecoinvent_co2_datamodel": {
        "columns": StructType(
            [
                StructField("activity_uuid_product_uuid", StringType(), False),
                StructField("co2_footprint", DecimalType(15, 10), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "ecoinvent_co2",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "ecoinvent_input_data_datamodel": {
        "columns": StructType(
            [
                StructField("Activity_UUID_Product_UUID", StringType(), True),
                StructField("Input_Activity_UUID_Product_UUID",
                            StringType(), True),
                StructField("Product_Name", StringType(), True),
                StructField("Amount", DoubleType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "ecoinvent_input_data",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "isic_mapper_datamodel": {
        "columns": StructType(
            [
                StructField("isic_4digit", StringType(), False),
                StructField("isic_4digit_name", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "isic_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tiltLedger_datamodel": {
        "columns": StructType(
            [
                StructField("tiltLedger_id", StringType(), False),
                StructField("CPC_Code", StringType(), True),
                StructField("CPC_Name", StringType(), True),
                StructField("ISIC_Code", StringType(), True),
                StructField("ISIC_Name", StringType(), True),
                StructField("Activity_Type", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Distance", IntegerType(), True),
                StructField("Manual_Review", IntegerType(), True),
                StructField("Verified_Source", IntegerType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "datamodel",
        "location": "tiltLedger",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    'ledger_ecoinvent_mapping_datamodel': {
        'columns':  StructType([
            StructField('tiltledger_id', StringType(), True),
            StructField('activity_uuid_product_uuid', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'datamodel',
        'location': 'ledger_ecoinvent_mapping',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'main_activity_ecoinvent_mapper_datamodel': {
        'columns': StructType([
            StructField('main_activity_id', StringType(), False),
            StructField('main_activity', StringType(), True),
            StructField('ecoinvent_activity', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'datamodel',
        'location': 'main_activity_ecoinvent_mapper',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'scope_1_emissions_datamodel': {
        'columns': StructType([
            StructField('activity_uuid_product_uuid', StringType(), False),
            StructField('emission', StringType(), True),
            StructField('amount', StringType(), True),
            StructField('emissions_unit', StringType(), True),
            StructField('carbon_allocation', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'datamodel',
        'location': 'scope_1_emissions',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    }
}
