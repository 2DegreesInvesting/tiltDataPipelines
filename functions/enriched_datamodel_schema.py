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
            StructField('activity_name', StringType(), True),
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
    'sector_profile_ledger_enriched': {
        'columns': StructType([
            StructField('tiltledger_id', StringType(), False),
            StructField('risk_category', StringType(), False),
            StructField('benchmark_group', StringType(), False),
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
    }
}
