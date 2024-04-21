from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType, ShortType, IntegerType, DateType, ByteType, TimestampType, DecimalType

raw_schema = {
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
    }
}