import re
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame


def create_map_column(spark_session: SparkSession, dataframe: DataFrame, dataframe_name: str) -> DataFrame:
    dataframe = dataframe.withColumn(
        f'map_{dataframe_name}', F.create_map(
            F.lit(dataframe_name), F.col('tiltRecordID'))
    )
    return dataframe


def create_sha_values(spark_session: SparkSession, data_frame: DataFrame, col_list: list) -> DataFrame:

    data_frame = data_frame.withColumn(
        'shaValue', F.sha2(F.concat_ws('|', *col_list), 256))

    return data_frame


def create_table_path(environment: str, schema: dict, partition_name: str = '') -> str:
    print(schema['container'])
    if schema['container'] == 'landingzone':
        if partition_name:
            return f"abfss://{schema['container']}@storagetilt{environment}.dfs.core.windows.net/{schema['location']}/{schema['partition_column']}={partition_name}"

        return f"abfss://{schema['container']}@storagetilt{environment}.dfs.core.windows.net/{schema['location']}/"
    return ''


def create_table_name(environment: str, container: str, location: str) -> str:
    return f"`{environment}`.`{container}`.`{location.replace('.','')}`"


def clean_column_names(data_frame: DataFrame) -> DataFrame:
    for col in data_frame.columns:
        new_col_name = re.sub(r"[-\\\/]", ' ', col)
        new_col_name = re.sub(r'[\(\)]', '', new_col_name)
        new_col_name = re.sub(r'\s+', '_', new_col_name)
        data_frame = data_frame.withColumnRenamed(col, new_col_name)
    return data_frame
