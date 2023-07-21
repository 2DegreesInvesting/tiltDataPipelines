from functions.spark_session import read_table, write_table, create_spark_session
import pyspark.sql.functions as F


def generate_table(table_name: str) -> None:

    spark_generate = create_spark_session()

    if table_name == 'geographies_raw':

        df = read_table(spark_generate, 'geographies_landingzone')

        df = df.filter(~F.isnull(F.col('ID')))

        write_table(spark_generate, df,'geographies_raw')
    
    elif table_name == 'geographies_transform':

        df = read_table(spark_generate, 'geographies_raw')

        geographies_df = df.select('ID', 'Name', 'Shortname', 'Geographical Classification')

        geographies_related_df = df.select('Shortname', 'Contained and Overlapping Geographies')

        geographies_related_df = geographies_related_df.withColumn('Shortname_related', F.explode(F.split('Contained and Overlapping Geographies', ';')))

        geographies_related_df = geographies_related_df.select('Shortname', 'Shortname_related')

        write_table(spark_generate, geographies_df, 'geographies_transform')

        write_table(spark_generate, geographies_related_df, 'geographies_related')

    elif table_name == 'undefined_ao_raw':

        df = read_table(spark_generate, 'undefined_ao_landingzone')

        write_table(spark_generate, df, 'undefined_ao_raw')

    elif table_name == 'cut_off_ao_raw':

        df = read_table(spark_generate, 'cut_off_ao_landingzone')

        write_table(spark_generate, df, 'cut_off_ao_raw')

    elif table_name == 'en15804_ao_raw':

        df = read_table(spark_generate, 'en15804_ao_landingzone')

        write_table(spark_generate, df, 'en15804_ao_raw')

    elif table_name == 'consequential_ao_raw':

        df = read_table(spark_generate, 'consequential_ao_landingzone')

        write_table(spark_generate, df, 'consequential_ao_raw')

    elif table_name == 'products_activities_transformed':

        undefined_ao_df = read_table(spark_generate, 'undefined_ao_raw')
        cutoff_ao_df = read_table(spark_generate, 'cut_off_ao_raw')
        en15804_ao_df = read_table(spark_generate, 'en15804_ao_raw')
        consequential_ao_df = read_table(spark_generate, 'consequential_ao_raw')

        undefined_ao_df = undefined_ao_df.withColumn('Reference Product Name', F.lit(None))

        cutoff_ao_df = cutoff_ao_df.withColumn('Product Group', F.lit(None))
        cutoff_ao_df = cutoff_ao_df.withColumn('Product Name', F.lit(None))
        cutoff_ao_df = cutoff_ao_df.withColumn('AO Method', F.lit('CutOff'))
        en15804_ao_df = en15804_ao_df.withColumn('Product Group', F.lit(None))
        en15804_ao_df = en15804_ao_df.withColumn('Product Name', F.lit(None))
        en15804_ao_df = en15804_ao_df.withColumn('AO Method', F.lit('EN15804'))
        consequential_ao_df = consequential_ao_df.withColumn('Product Group', F.lit(None))
        consequential_ao_df = consequential_ao_df.withColumn('Product Name', F.lit(None))
        consequential_ao_df = consequential_ao_df.withColumn('AO Method', F.lit('Consequential'))

        product_list = ['Product UUID','Product Group',
                        'Product Name','Reference Product Name',
                        'CPC Classification','Unit',
                        'Product Information','CAS Number']

        activity_list = ['Activity UUID','Activity Name',
                        'Geography','Time Period','Special Activity Type',
                        'Sector','ISIC Classification','ISIC Section']

        relational_list = ['Activity UUID & Product UUID', 'Activity UUID', 'Product UUID','EcoQuery URL','AO Method']

        cutoff_products = cutoff_ao_df.select(product_list)
        cutoff_activities = cutoff_ao_df.select(activity_list)
        cutoff_relations = cutoff_ao_df.select(relational_list)
        
        undefined_products = undefined_ao_df.select(product_list)
        undefined_activities = undefined_ao_df.select(activity_list)

        en15804_products = en15804_ao_df.select(product_list)
        en15804_activities = en15804_ao_df.select(activity_list)
        en15804_relations = en15804_ao_df.select(relational_list)

        consequential_products = consequential_ao_df.select(product_list)
        consequential_activities = consequential_ao_df.select(activity_list)
        consequential_relations = consequential_ao_df.select(relational_list)

        products_df = cutoff_products.union(undefined_products)\
                        .union(en15804_products).union(consequential_products).distinct()

        activities_df = cutoff_activities.union(undefined_activities)\
                        .union(en15804_activities).union(consequential_activities).distinct()

        relational_df = cutoff_relations.union(en15804_relations)\
                        .union(consequential_relations).distinct()

        write_table(spark_generate, products_df, 'products_transformed')
        write_table(spark_generate, activities_df, 'activities_transformed')
        write_table(spark_generate, relational_df, 'products_activities_transformed','AO Method')

    elif table_name == 'lcia_methods_raw':

        df = read_table(spark_generate, 'lcia_methods_landingzone')

        write_table(spark_generate, df, 'lcia_methods_raw')

    elif table_name == 'lcia_methods_transform':

        df = read_table(spark_generate, 'lcia_methods_raw')

        write_table(spark_generate, df, 'lcia_methods_transform')

    elif table_name == 'impact_categories_raw':

        df = read_table(spark_generate, 'impact_categories_landingzone')

        write_table(spark_generate, df, 'impact_categories_raw')

    elif table_name == 'impact_categories_transform':

        df = read_table(spark_generate, 'impact_categories_raw')

        write_table(spark_generate, df, 'impact_categories_transform')

    elif table_name == 'intermediate_exchanges_raw':

        df = read_table(spark_generate, 'intermediate_exchanges_landingzone')

        write_table(spark_generate, df, 'intermediate_exchanges_raw')

    elif table_name == 'intermediate_exchanges_transform':

        df = read_table(spark_generate, 'intermediate_exchanges_raw')

        write_table(spark_generate, df, 'intermediate_exchanges_transform')

    elif table_name == 'elementary_exchanges_raw':

        df = read_table(spark_generate, 'elementary_exchanges_landingzone')

        write_table(spark_generate, df, 'elementary_exchanges_raw')

    elif table_name == 'elementary_exchanges_transform':

        df = read_table(spark_generate, 'elementary_exchanges_raw')

        write_table(spark_generate, df, 'elementary_exchanges_transform')

    spark_generate.stop()
