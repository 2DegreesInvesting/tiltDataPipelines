from pyspark.sql import SparkSession, DataFrame
from functions.spark_session import read_table, get_table_definition, build_table_path
from pyspark.sql import functions as F


def create_map_column(spark_session: SparkSession, dataframe: DataFrame, dataframe_name: str) -> DataFrame:
    dataframe = dataframe.withColumn(
        f'map_{dataframe_name}', F.create_map(F.lit(dataframe_name), F.col('tiltRecordID'))
    )
    return dataframe


class CustomDF:
    def __init__(self, table_name: str, spark_session: SparkSession):
        self._name = table_name
        self._spark_session = spark_session
        self._schema = get_table_definition(self._name)
        self._df = read_table(spark_session, table_name)
        self._df = create_map_column(self._spark_session, self._df, self._name)

    def read_table(self, partition_name: str = '', history: str = 'recent'):

        
        if history not in ['recent','complete']:
            raise ValueError(f"Value {history} is not in valid arguments [recent,complete] for history argument")

        if partition_name != '':
            partition_column = self._schema['partition_column']
            partition_path = f'{partition_column}={partition_name}'
        else:
            partition_path = partition_name

        table_location = build_table_path(self._schema['container'], self._schema['location'], partition_path)

        try:
            if self._schema['type'] == 'csv':
                df = self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '"').option("multiline", 'True').load(table_location)
            if self._schema['type'] in ['ecoInvent','tiltData']:
                df = self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option("quote", '~').option('delimiter',';').option("multiline", 'True').load(table_location)
            else:
                df = self._spark_session.read.format(self._schema['type']).schema(self._schema['columns']).option('header', True).load(table_location)
            # Force to load first record of the data to check if it throws an error
            df.head()
        # Try to catch the specific exception where the table to be read does not exist
        except Exception as e:
            # If the table does not exist yet, return an empty data frame
            if "Path does not exist:" in str(e):
                df = self._spark_session.createDataFrame([], self._schema['columns'])
            # If we encounter any other error, raise as the error
            else:
                raise(e)

        if partition_name != '':
            df = df.withColumn(partition_column, F.lit(partition_name))

        if history == 'recent' and 'to_date' in df.columns:
            df = df.filter(F.col('to_date')=='2099-12-31')

        # Replace empty values with None/null
        replacement_dict = {'NA': None, 'nan': None}
        df = df.replace(replacement_dict, subset=df.columns)
    
    def custom_join(self, custom_other: 'CustomDF', custom_on: str = None, custom_how: str = None):
        copy_df = custom_other._df
        self._df = self._df.join(copy_df, custom_on, custom_how)
        self._df = self._df.withColumn(
            f'map_{self._name}', F.map_concat(F.col(f'map_{self._name}'), F.col(f'map_{custom_other._name}'))
        )
        self._df = self._df.drop(F.col(f'map_{custom_other._name}'))