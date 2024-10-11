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
            StructField('tiltledger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('average_input_profile_rank', DoubleType(), False),
            StructField('average_input_co2_footprint', DoubleType(), False),
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
            StructField('tiltLedger_id', StringType(), False),
            StructField('benchmark_group', StringType(), False),
            StructField('scenario_type', StringType(), False),
            StructField('scenario_name', StringType(), False),
            StructField('year', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('average_profile_ranking', DoubleType(), False),
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
    'scope_1_indicator_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('avg_sum_carbon_per_product', DoubleType(), False),
            StructField('avg_sum_carbon_per_product_amount', DoubleType(), False),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
    'container': 'enriched',
    'location': 'scope_1_indicator_ledger',
    'type': 'delta',
    'partition_column': '',
    'quality_checks': []
    },
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
    },
    'scope_3_indicator_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('total_scope_3_electricity_emission_per_ledger_id', DoubleType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
    'container': 'enriched',
    'location': 'scope_3_indicator',
    'type': 'delta',
    'partition_column': '',
    'quality_checks': []
    },
    'company_product_indicators_enriched': {
        'columns': StructType([
            StructField('company_id', StringType(), False),
            StructField('source_id', StringType(), False),
            StructField('country', StringType(), False),
            StructField('tilt_sector', StringType(), True),
            StructField('tilt_subsector', StringType(), True),
            StructField('tiltledger_id', StringType(), True),
            StructField('activity_type', StringType(), True),
            StructField('geography', StringType(), True),
            StructField('CPC_Code', StringType(), True),
            StructField('CPC_Name', StringType(), True),
            StructField('ISIC_Code', StringType(), True),
            StructField('ISIC_Name', StringType(), True),
            StructField('companies_ledger_matches', StringType(), True),
            StructField('model_certainty', StringType(), True),
            StructField('data_source_reliability', StringType(), True),
            StructField('data_granularity', StringType(), True),
            StructField('Indicator', StringType(), True),
            StructField('benchmark', StringType(), True),
            StructField('score', StringType(), True),
            StructField('profile_ranking', StringType(), True),
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
