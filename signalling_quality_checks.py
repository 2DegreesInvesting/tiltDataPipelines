from functions.spark_session import create_spark_session
from functions.custom_dataframes import CustomDF

if __name__ == '__main__':

    signalling_spark_session = create_spark_session()

    table_name_list = ['undefined_ao_raw', 'cut_off_ao_raw', 'en15804_ao_raw', 'consequential_ao_raw', 'impact_categories_raw', 'intermediate_exchanges_raw', 'elementary_exchanges_raw',  
                       'ecoinvent_product_datamodel', 'companies_datamodel', 'ecoinvent_product_datamodel',  'tilt_sector_isic_mapper_datamodel', 'tilt_sector_scenario_mapper_datamodel', 'tiltLedger_datamodel', 'SBI_activities_datamodel', 'companies_SBI_activities_datamodel', 
                       'emission_profile_ledger_enriched', 'emission_profile_ledger_upstream_enriched', 'sector_profile_ledger_enriched', 'transition_risk_ledger_enriched', 'company_product_indicators_enriched', 'sector_profile_ledger_upstream_enriched', 'company_indicators_enriched']
    
# Removed tables because old: 'isic_mapper_datamodel', 'inputProducts_raw'

    # Apply the checks from dictionary
    for table_name in table_name_list:
        table_df = CustomDF(table_name, signalling_spark_session)
        table_df.check_signalling_issues()
