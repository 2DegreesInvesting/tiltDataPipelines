from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType, ShortType, IntegerType, DateType, ByteType, TimestampType, DecimalType

monitoring_schema = {
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
    }
}