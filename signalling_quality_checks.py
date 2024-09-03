from functions.spark_session import create_spark_session
from functions.custom_dataframes import CustomDF

if __name__ == '__main__':

    signalling_spark_session = create_spark_session()

    table_name_list = ['emission_profile_ledger_enriched', 'emission_profile_ledger_upstream_enriched',
                       'sector_profile_ledger_enriched', 'sector_profile_ledger_upstream_enriched']

    # Apply the checks from dictionary
    for table_name in table_name_list:
        table_df = CustomDF(table_name, signalling_spark_session)
        table_df.check_signalling_issues()
