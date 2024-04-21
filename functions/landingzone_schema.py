from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType, ShortType, IntegerType, DateType, ByteType, TimestampType, DecimalType

landingzone_schema = {
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
    }
}