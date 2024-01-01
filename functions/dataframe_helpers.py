import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame


def create_map_column(spark_session: SparkSession, dataframe: DataFrame, dataframe_name: str) -> DataFrame:
    dataframe = dataframe.withColumn(
        f'map_{dataframe_name}', F.create_map(F.lit(dataframe_name), F.col('tiltRecordID'))
    )
    return dataframe


def create_sha_values(spark_session: SparkSession, data_frame: DataFrame, col_list: list) -> DataFrame:

    data_frame = data_frame.withColumn('shaValue',F.sha2(F.concat_ws('|',*col_list),256))

    return data_frame
