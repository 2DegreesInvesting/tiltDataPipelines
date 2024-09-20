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
            StructField('input_activity_uuid_product_uuid',
                        StringType(), False),
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
    },
    'company_product_indicators_enriched': {
        'columns': StructType([
            StructField('company_id', StringType(), False),
            StructField('source_id', StringType(), False),
            StructField('country', StringType(), False),
            StructField('tilt_sector', StringType(), False),
            StructField('tilt_subsector', StringType(), False),
            StructField('tiltledger_id', StringType(), False),
            StructField('activity_type', StringType(), False),
            StructField('geography', StringType(), False),
            StructField('CPC_Code', StringType(), False),
            StructField('CPC_Name', StringType(), False),
            StructField('ISIC_Code', StringType(), False),
            StructField('ISIC_Name', StringType(), False),
            StructField('companies_ledger_matches', StringType(), False),
            StructField('model_certainty', StringType(), False),
            StructField('data_source_reliability', StringType(), False),
            StructField('data_granularity', StringType(), False),
            StructField('Indicator', StringType(), False),
            StructField('benchmark', StringType(), False),
            StructField('score', StringType(), False),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'enriched',
        'location': 'company_product_indicators',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    },
    'company_indicators_enriched': {
        'columns': StructType([
            StructField('company_id', StringType(), False),
            StructField('source_id', StringType(), False),
            StructField('country', StringType(), False),
            StructField('Indicator', StringType(), True),
            StructField('benchmark', StringType(), True),
            StructField('average_ranking', DoubleType(), True),
            StructField('company_score', StringType(), True),
            StructField('amount_low', DoubleType(), True),
            StructField('amount_medium', DoubleType(), True),
            StructField('amount_high', DoubleType(), True),
            StructField('model_certainty', StringType(), True),
            StructField('data_source_reliability', StringType(), True),
            StructField('data_granularity', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'enriched',
        'location': 'company_indicators',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': []
    }
}
