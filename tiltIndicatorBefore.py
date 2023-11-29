from functions.processing import generate_table

generate_table("ep_companies_NL_postcode_raw")
generate_table("ep_ei_matcher_raw") # output of matching
generate_table("scenario_targets_IPR_NEW_raw")
generate_table("scenario_targets_WEO_NEW_raw")
generate_table("scenario_tilt_mapper_2023-07-20_raw")
generate_table("sector_resolve_raw") # output of matching
generate_table("tilt_isic_mapper_2023-07-20_raw")
generate_table("ecoinvent-v3.9.1_raw")
generate_table("ecoinvent_inputs_overview_raw_raw")
generate_table("ecoinvent_input_data_relevant_columns_raw")
generate_table("tilt_sector_classification_raw")

