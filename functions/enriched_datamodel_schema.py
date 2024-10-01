"""
This module contains the definition of the tables used in the datahub from the datamodel layer.

"""
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, ShortType, DateType, ByteType, DecimalType

enriched_datamodel_schema = {
    'emission_profile_ledger_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('average_profile_ranking', StringType(), False),
            StructField('product_name', StringType(), False),
            StructField('average_co2_footprint', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'enriched',
        'location': 'emission_profile_ledger',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'emission_profile_ledger_upstream_enriched': {
        'columns': StructType([
            StructField('input_activity_uuid_product_uuid', StringType(), False),
            StructField('tiltledger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('profile_ranking', StringType(), False),
            StructField('input_product_name', StringType(), False),
            StructField('input_co2_footprint', StringType(), False),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'enriched',
        'location': 'emission_profile_ledger_upstream',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'sector_profile_ledger_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('profile_ranking', StringType(), False),
            StructField('product_name', StringType(), False),
            StructField('tilt_sector', StringType(), True),
            StructField('scenario_name', StringType(), True),
            StructField('scenario_type', StringType(), True),
            StructField('year', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'enriched',
        'location': 'sector_profile_ledger',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'sector_profile_ledger_upstream_enriched': {
        'columns': StructType([
            StructField('input_tiltledger_id', StringType(), False),
            StructField('tiltledger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('profile_ranking', StringType(), False),
            StructField('input_product_name', StringType(), False),
            StructField('input_tilt_sector', StringType(), False),
            StructField('input_scenario_name', StringType(), False),
            StructField('input_scenario_type', StringType(), False),
            StructField('input_year', StringType(), False),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'enriched',
        'location': 'sector_profile_ledger_upstream',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'transition_risk_ledger_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('average_profile_ranking', StringType(), False),
            StructField('product_name', StringType(), False),
            StructField('reductions', StringType(), False),
            StructField('transition_risk_score', StringType(), False),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
    'container': 'enriched',
    'location': 'transition_risk_ledger',
    'type': 'delta',
    'partition_column': '',
    'quality_checks': []
    }
    ,
    'scope_2_indicator_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('total_scope_2_emission_per_ledger_id', DoubleType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
    'container': 'enriched',
    'location': 'scope_2_indicator_enriched',
    'type': 'delta',
    'partition_column': '',
    'quality_checks': []
    }
}