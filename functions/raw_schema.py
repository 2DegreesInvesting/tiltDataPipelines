"""
This module contains the definition of tables used in the datahub from the raw layer.

"""

from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    BooleanType,
    DoubleType,
    ShortType,
    IntegerType,
    DateType,
    ByteType,
    DecimalType,
)

raw_schema = {
    "test_table_raw": {
        "columns": StructType(
            [
                StructField("test_string_column", StringType(), False),
                StructField("test_integer_column", IntegerType(), False),
                StructField("test_decimal_column", DoubleType(), True),
                StructField("test_date_column", DateType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "test",
        "location": "test_table",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "companies_europages_raw": {
        "columns": StructType(
            [
                StructField("company_name", StringType(), False),
                StructField("group", StringType(), True),
                StructField("sector", StringType(), True),
                StructField("subsector", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("address", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("country", StringType(), False),
                StructField("products_and_services", StringType(), True),
                StructField("information", StringType(), True),
                StructField("min_headcount", IntegerType(), True),
                StructField("max_headcount", IntegerType(), True),
                StructField(
                    "type_of_building_for_registered_address", StringType(), True
                ),
                StructField("verified_by_europages", BooleanType(), True),
                StructField("year_established", IntegerType(), True),
                StructField("websites", StringType(), True),
                StructField("download_datetime", DateType(), True),
                StructField("id", StringType(), False),
                StructField("filename", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "companies_europages",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": [
                "id", "sector", "subsector"]}
        ],
    },
    "companies_companyinfo_raw": {
        "columns": StructType(
            [
                StructField("Sizokey", StringType(), False),
                StructField("Kamer_van_Koophandel_nummer",
                            StringType(), False),
                StructField(
                    "Kamer_van_Koophandel_nummer_12_cijferig", StringType(), False
                ),
                StructField("RSIN_nummer", StringType(), True),
                StructField("Vestigingsnummer", StringType(), True),
                StructField("Instellingsnaam", StringType(), False),
                StructField("Statutaire_naam", StringType(), True),
                StructField("Handelsnaam_1", StringType(), True),
                StructField("Handelsnaam_2", StringType(), True),
                StructField("Handelsnaam_3", StringType(), True),
                StructField("Bedrijfsomschrijving", StringType(), True),
                StructField("Vestigingsadres", StringType(), False),
                StructField("Vestigingsadres_straat", StringType(), False),
                StructField("Vestigingsadres_huisnummer", StringType(), False),
                StructField("Vestigingsadres_toevoeging", StringType(), True),
                StructField("Vestigingsadres_postcode", StringType(), False),
                StructField("Vestigingsadres_plaats", StringType(), False),
                StructField("Plan_Naam_1", StringType(), True),
                StructField("Kern_Naam_1", StringType(), True),
                StructField("KIX", StringType(), False),
                StructField("4_cijferige_Postcode", StringType(), False),
                StructField("Pand_ID", StringType(), True),
                StructField("Rechtsvorm_Omschrijving", StringType(), False),
                StructField("Aantal_medewerkers", StringType(), False),
                StructField("Klasse_aantal_medewerkers", StringType(), False),
                StructField(
                    "Klasse_aantal_medewerkers_Omschrijving", StringType(), False
                ),
                StructField(
                    "Aantal_medewerkers_van_concern_op_locatie", StringType(), False
                ),
                StructField("Klasse_aantal_medewerkers_locatie",
                            StringType(), False),
                StructField(
                    "Klasse_aantal_medewerkers_locatie_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField("SBI_code", StringType(), False),
                StructField("SBI_code_Omschrijving", StringType(), False),
                StructField("SBI_code_2_cijferig", StringType(), False),
                StructField("SBI_code_2_cijferig_Omschrijving",
                            StringType(), False),
                StructField("SBI_code_segment", StringType(), False),
                StructField("SBI_code_segment_Omschrijving",
                            StringType(), False),
                StructField("NACE_code", StringType(), False),
                StructField("NACE_code_Omschrijving", StringType(), False),
                StructField("SBI_code_locatie", StringType(), False),
                StructField("SBI_code_locatie_Omschrijving",
                            StringType(), False),
                StructField("SBI_code_2_cijferig_locatie",
                            StringType(), False),
                StructField(
                    "SBI_code_2_cijferig_locatie_Omschrijving", StringType(), False
                ),
                StructField("SBI_code_segment_locatie", StringType(), False),
                StructField(
                    "SBI_code_segment_locatie_Omschrijving", StringType(), False
                ),
                StructField("Concernrelatie_Omschrijving",
                            StringType(), False),
                StructField("Sizokey_ultimate_parent", StringType(), False),
                StructField("Instellingsnaam_ultimate_parent",
                            StringType(), False),
                StructField("Aantal_instellingen_in_concern",
                            StringType(), False),
                StructField("Aantal_locaties_van_concern",
                            StringType(), False),
                StructField("Klasse_aantal_locaties_van_concern",
                            StringType(), False),
                StructField(
                    "Klasse_aantal_locaties_van_concern_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField("Aantal_medewerkers_concern", StringType(), False),
                StructField("Klasse_aantal_medewerkers_concern",
                            StringType(), False),
                StructField(
                    "Klasse_aantal_medewerkers_concern_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField("SBI_code_concern", StringType(), False),
                StructField("SBI_code_concern_Omschrijving",
                            StringType(), False),
                StructField("SBI_code_2_cijferig_concern",
                            StringType(), False),
                StructField(
                    "SBI_code_2_cijferig_concern_Omschrijving", StringType(), False
                ),
                StructField("SBI_code_segment_concern", StringType(), False),
                StructField(
                    "SBI_code_segment_concern_Omschrijving", StringType(), False
                ),
                StructField("Subsidie_bedrag_concern", StringType(), False),
                StructField("Klasse_subsidie_bedrag_concern",
                            StringType(), False),
                StructField(
                    "Klasse_subsidie_bedrag_concern_Omschrijving", StringType(), False
                ),
                StructField(
                    "Grootverbruik_of_kleinverbruik_Omschrijving", StringType(), True
                ),
                StructField("Bouwjaar_verblijfsobject", StringType(), True),
                StructField("Oppervlakte_verblijfsobject", StringType(), True),
                StructField(
                    "Klasse_oppervlakte_verblijfsobject_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Hoofd_gebruiksdoel_verblijfsobject_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Energielabel_verblijfsobject_Omschrijving", StringType(), False
                ),
                StructField(
                    "Energielabel_status_verblijfsobject_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Type_bedrijfsverzamelpand_Omschrijving", StringType(), True
                ),
                StructField("Jaar_laatste_jaarverslag", StringType(), True),
                StructField("Geconsolideerd_resultaat", StringType(), True),
                StructField("Klasse_geconsolideerd_resultaat",
                            StringType(), True),
                StructField(
                    "Klasse_geconsolideerd_resultaat_Omschrijving", StringType(), True
                ),
                StructField("Omzet_geconsolideerd", StringType(), True),
                StructField("Klasse_geconsolideerde_omzet",
                            StringType(), True),
                StructField(
                    "Klasse_geconsolideerde_omzet_Omschrijving", StringType(), True
                ),
                StructField(
                    "Totaal_aantal_personenwagens_locatie", StringType(), False
                ),
                StructField(
                    "Totaal_aantal_lichte_bedrijfswagens_locatie", StringType(), False
                ),
                StructField("Totaal_aantal_vrachtwagens_locatie",
                            StringType(), False),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_personenwagens_locatie",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_lichte_bedrijfswagens_locatie",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_vrachtwagens_locatie",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_personenwagens_locatie_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_lichte_bedrijfswagens_locatie_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_vrachtwagens_locatie_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Percentage_benzine_personenwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_diesel_personenwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_hybride_personenwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_elektrische_personenwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_overig_brandstof_personenwagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_benzine_lichte_bedrijfswagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_diesel_lichte_bedrijfswagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_hybride_lichte_bedrijfswagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_elektrische_lichte_bedrijfswagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_overig_brandstof_lichte_bedrijfswagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_benzine_vrachtwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_diesel_vrachtwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_hybride_vrachtwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_elektrische_vrachtwagens_locatie", StringType(), True
                ),
                StructField(
                    "Percentage_overig_brandstof_vrachtwagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Totaal_aantal_personenwagens_concern", StringType(), False
                ),
                StructField(
                    "Totaal_aantal_lichte_bedrijfswagens_concern", StringType(), False
                ),
                StructField("Totaal_aantal_vrachtwagens_concern",
                            StringType(), False),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_personenwagens_concern",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_lichte_bedrijfswagens_concern",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_vrachtwagens_concern",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_personenwagens_concern_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_lichte_bedrijfswagens_concern_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_vrachtwagens_concern_Omschrijving",
                    StringType(),
                    False,
                ),
                StructField(
                    "Percentage_benzine_personenwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_diesel_personenwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_hybride_personenwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_elektrische_personenwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_overig_brandstof_personenwagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_benzine_lichte_bedrijfswagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_diesel_lichte_bedrijfswagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_hybride_lichte_bedrijfswagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_elektrische_lichte_bedrijfswagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_overig_brandstof_lichte_bedrijfswagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Percentage_benzine_vrachtwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_diesel_vrachtwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_hybride_vrachtwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_elektrische_vrachtwagens_concern", StringType(), True
                ),
                StructField(
                    "Percentage_overig_brandstof_vrachtwagens_concern",
                    StringType(),
                    True,
                ),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "companies_companyinfo",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "multi_SBI_companyinfo_raw": {
        "columns": StructType(
            [
                StructField("SIZOKEY", StringType(), False),
                StructField("SIZOSBI", StringType(), False),
                StructField("SBI_Code", StringType(), False),
                StructField("SIZRVOLGNR", StringType(), False),
                StructField("Soort_SBI_code_Kamer_van_Koophandel", StringType(), False),
                StructField("Bron_ID", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "multi_SBI_companyinfo",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "country_raw": {
        "columns": StructType(
            [
                StructField("country_id", StringType(), False),
                StructField("country", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "country",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "main_activity_ecoinvent_mapper_raw": {
        "columns": StructType(
            [
                StructField("main_activity_id", StringType(), False),
                StructField("main_activity", StringType(), True),
                StructField("ei_activity_name", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "main_activity_ecoinvent_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "sources_mapper_raw": {
        "columns": StructType(
            [
                StructField("source_id", StringType(), False),
                StructField("source_name", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "sources_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "countries_mapper_raw": {
        "columns": StructType(
            [
                StructField("country_un", StringType(), False),
                StructField("country", StringType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "countries_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "geography_ecoinvent_mapper_raw": {
        "columns": StructType(
            [
                StructField("geography_id", StringType(), False),
                StructField("country_id", StringType(), False),
                StructField("lca_geo", StringType(), False),
                StructField("priority", ByteType(), False),
                StructField("input_priority", ByteType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "geography_ecoinvent_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "EP_tilt_sector_unmatched_mapper_raw": {
        "columns": StructType(
            [
                StructField("categories_id", StringType(), False),
                StructField("group", StringType(), True),
                StructField("ep_sector", StringType(), False),
                StructField("ep_subsector", StringType(), True),
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "EP_tilt_sector_unmatched_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tilt_sector_isic_mapper_raw": {
        "columns": StructType(
            [
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("isic_4digit", StringType(), True),
                StructField("isic_section", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "tilt_sector_isic_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tilt_sector_scenario_mapper_raw": {
        "columns": StructType(
            [
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("weo_product", StringType(), True),
                StructField("weo_flow", StringType(), True),
                StructField("ipr_sector", StringType(), True),
                StructField("ipr_subsector", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "tilt_sector_scenario_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_targets_IPR_raw": {
        "columns": StructType(
            [
                StructField("Scenario", StringType(), False),
                StructField("Region", StringType(), True),
                StructField("Variable_Class", StringType(), True),
                StructField("Sub_Variable_Class", StringType(), True),
                StructField("Sector", StringType(), True),
                StructField("Sub_Sector", StringType(), True),
                StructField("Units", StringType(), True),
                StructField("Year", ShortType(), False),
                StructField("Value", DoubleType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "scenario_targets_IPR",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_targets_WEO_raw": {
        "columns": StructType(
            [
                StructField("PUBLICATION", StringType(), False),
                StructField("SCENARIO", StringType(), True),
                StructField("CATEGORY", StringType(), True),
                StructField("PRODUCT", StringType(), True),
                StructField("FLOW", StringType(), True),
                StructField("UNIT", StringType(), True),
                StructField("REGION", StringType(), True),
                StructField("YEAR", ShortType(), True),
                StructField("VALUE", DoubleType(), False),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "scenario_targets_WEO",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    'geographies_raw': {
        'columns':  StructType([
            StructField('ID', StringType(), False),
            StructField('Name', StringType(), True),
            StructField('Shortname', StringType(), True),
            StructField('Latitude', DoubleType(), True),
            StructField('Longitude', DoubleType(), True),
            StructField('Geographical_Classification', StringType(), True),
            StructField('Contained_and_Overlapping_Geographies',
                        StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        "container": "raw",
        "location": "geographies",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": ["ID"]},
            {
                "check": "values have format",
                "columns": ["Geographical_Classification"],
                "format": r"[a-zA-Z\-]",
            },
        ],
    },
    "undefined_ao_raw": {
        "columns": StructType(
            [
                StructField("Activity_UUID", StringType(), False),
                StructField("EcoQuery_URL", StringType(), True),
                StructField("Activity_Name", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Time_Period", StringType(), True),
                StructField("Special_Activity_Type", StringType(), True),
                StructField("Sector", StringType(), True),
                StructField("ISIC_Classification", StringType(), True),
                StructField("ISIC_Section", StringType(), True),
                StructField("Product_UUID", StringType(), False),
                StructField("Product_Group", StringType(), True),
                StructField("Product_Name", StringType(), True),
                StructField("CPC_Classification", StringType(), True),
                StructField("Unit", StringType(), True),
                StructField("Product_Information", StringType(), True),
                StructField("CAS_Number", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "undefined_ao",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    'cut_off_ao_raw': {
        'columns':  StructType([
            StructField('Activity_UUID_&_Product_UUID',
                        StringType(), False),
            StructField('Activity_UUID', StringType(), False),
            StructField('EcoQuery_URL', StringType(), True),
            StructField('Activity_Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time_Period', StringType(), True),
            StructField('Special_Activity_Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC_Classification', StringType(), True),
            StructField('ISIC_Section', StringType(), True),
            StructField('Product_UUID', StringType(), False),
            StructField('Reference_Product_Name', StringType(), True),
            StructField('CPC_Classification', StringType(), True),
            StructField('HS2017_Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product_Information', StringType(), True),
            StructField('CAS_Number', StringType(), True),
            StructField('Cut_Off_Classification', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        "container": "raw",
        "location": "cutoff_ao",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": [
                "Activity_UUID_&_Product_UUID"]},
        ],
    },
    'en15804_ao_raw': {
        'columns':  StructType([
            StructField('Activity_UUID_&_Product_UUID',
                        StringType(), False),
            StructField('Activity_UUID', StringType(), False),
            StructField('EcoQuery_URL', StringType(), True),
            StructField('Activity_Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time_Period', StringType(), True),
            StructField('Special_Activity_Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC_Classification', StringType(), True),
            StructField('ISIC_Section', StringType(), True),
            StructField('Product_UUID', StringType(), False),
            StructField('Reference_Product_Name', StringType(), True),
            StructField('CPC_Classification', StringType(), True),
            StructField('HS2017_Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product_Information', StringType(), True),
            StructField('CAS_Number', StringType(), True),
            StructField('Cut_Off_Classification', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        "container": "raw",
        "location": "en15804_ao",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": [
                "Activity_UUID_&_Product_UUID"]},
        ],
    },
    'apos_ao_raw': {
        'columns':  StructType([
            StructField('Activity_UUID_&_Product_UUID',
                        StringType(), False),
            StructField('Activity_UUID', StringType(), False),
            StructField('EcoQuery_URL', StringType(), True),
            StructField('Activity_Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time_Period', StringType(), True),
            StructField('Special_Activity_Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC_Classification', StringType(), True),
            StructField('ISIC_Section', StringType(), True),
            StructField('Product_UUID', StringType(), False),
            StructField('Reference_Product_Name', StringType(), True),
            StructField('CPC_Classification', StringType(), True),
            StructField('HS2017_Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product_Information', StringType(), True),
            StructField('CAS_Number', StringType(), True),
            StructField('APOS_Classification', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        'container': 'raw',
        'location': 'apos_ao',
        'type': 'delta',
        'partition_column': '',
        'quality_checks': [{
                'check': 'values are unique',
            'columns': ['Activity_UUID_&_Product_UUID']
        },]
    },
    'consequential_ao_raw': {
        'columns':  StructType([
            StructField('Activity_UUID_&_Product_UUID',
                        StringType(), False),
            StructField('Activity_UUID', StringType(), False),
            StructField('EcoQuery_URL', StringType(), True),
            StructField('Activity_Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time_Period', StringType(), True),
            StructField('Special_Activity_Type', StringType(), True),
            StructField('Technology_Level', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC_Classification', StringType(), True),
            StructField('ISIC_Section', StringType(), True),
            StructField('Product_UUID', StringType(), False),
            StructField('Reference_Product_Name', StringType(), True),
            StructField('CPC_Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product_Information', StringType(), True),
            StructField('CAS_Number', StringType(), True),
            StructField('from_date', DateType(), False),
            StructField('to_date', DateType(), False),
            StructField('tiltRecordID', StringType(), False)
        ]
        ),
        "container": "raw",
        "location": "consequential_ao",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": [
                "Activity_UUID_&_Product_UUID"]},
        ],
    },
    "lcia_methods_raw": {
        "columns": StructType(
            [
                StructField("Method_Name", StringType(), False),
                StructField("Status", StringType(), False),
                StructField("Method_Version", StringType(), True),
                StructField("Further_Documentation", StringType(), True),
                StructField(
                    "Links_to_Characterization_Factor_Successes", StringType(), True
                ),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "lcia_methods",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "impact_categories_raw": {
        "columns": StructType(
            [
                StructField("Resources_Emissions_Total", StringType(), True),
                StructField("Main_impact_damage_category", StringType(), True),
                StructField("Inventory_Midpoint_Endpoint_AoP",
                            StringType(), True),
                StructField("Area_of_Protection_AoP", StringType(), True),
                StructField("Used_in_EN15804", StringType(), True),
                StructField("Method", StringType(), True),
                StructField("Category", StringType(), True),
                StructField("Indicator", StringType(), True),
                StructField("Unit", StringType(), True),
                StructField("Category_name_in_method", StringType(), True),
                StructField("Indicator_name_in_method", StringType(), True),
                StructField("Unit_in_method", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "impact_categories",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "intermediate_exchanges_raw": {
        "columns": StructType(
            [
                StructField("ID", StringType(), False),
                StructField("Name", StringType(), True),
                StructField("Unit_Name", StringType(), True),
                StructField("CAS_Number", StringType(), True),
                StructField("Comment", StringType(), True),
                StructField("By_product_Classification", StringType(), True),
                StructField("CPC_Classification", StringType(), True),
                StructField("Product_Information", StringType(), True),
                StructField("Synonym", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "intermediate_exchanges",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": ["ID"]},
        ],
    },
    "elementary_exchanges_raw": {
        "columns": StructType(
            [
                StructField("ID", StringType(), False),
                StructField("Name", StringType(), True),
                StructField("Compartment", StringType(), True),
                StructField("Sub_Compartment", StringType(), True),
                StructField("Unit_Name", StringType(), True),
                StructField("CAS_Number", StringType(), True),
                StructField("Comment", StringType(), True),
                StructField("Synonym", StringType(), True),
                StructField("Formula", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "elementary_exchanges",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [
            {"check": "values are unique", "columns": ["ID"]},
        ],
    },
    "ecoinvent_co2_raw": {
        "columns": StructType(
            [
                StructField("Activity_UUID_Product_UUID", StringType(), False),
                StructField("Activity_Name", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Reference_Product_Name", StringType(), True),
                StructField("Reference_Product_Unit", StringType(), True),
                StructField(
                    "IPCC_2021_climate_change_global_warming_potential_GWP100_kg_CO2_Eq",
                    DecimalType(15, 10),
                    True,
                ),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "ecoinvent_co2",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "ep_ei_matcher_raw": {
        "columns": StructType(
            [
                StructField("group_var", StringType(), False),
                StructField("ep_id", StringType(), True),
                StructField("ep_country", StringType(), False),
                StructField("ep_main_act", StringType(), True),
                StructField("ep_clustered", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), True),
                StructField("multi_match", BooleanType(), True),
                StructField("completion", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "scenario_tilt_mapper_2023-07-20",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    # 'geography_mapper_raw': {
    #     'columns':  StructType([
    #         StructField('geography_id', StringType(), False),
    #         StructField('country_id', StringType(), True),
    #         StructField('lca_geo', StringType(), True),
    #         StructField('priority', StringType(), True),
    #         StructField('input_priority', StringType(), True),
    #         StructField('from_date', DateType(), False),
    #         StructField('to_date', DateType(), False),
    #         StructField('tiltRecordID', StringType(), False),
    #     ]
    #     ),
    #     'container': 'raw',
    #     'location': 'geography_mapper',
    #     'type': 'delta',
    #     'partition_column': '',
    #     'quality_checks': []
    # },
    "mapper_ep_ei_raw": {
        "columns": StructType(
            [
                StructField("country", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("clustered", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), False),
                StructField("multi_match", BooleanType(), True),
                StructField("completion", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "mapper_ep_ei",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "emission_profile_company_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("emission_profile_share", DoubleType(), True),
                StructField("emission_profile", StringType(), True),
                StructField("benchmark", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "emission_profile_company",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "emission_profile_product_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("emission_profile", StringType(), True),
                StructField("benchmark", StringType(), True),
                StructField("ep_product", StringType(), True),
                StructField("matched_activity_name", StringType(), True),
                StructField("matched_reference_product", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("multi_match", BooleanType(), True),
                StructField("matching_certainty", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("isic_4digit", StringType(), True),
                StructField("isic_4digit_name", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), True),
                StructField("reduction_targets", DoubleType(), True),
                StructField("geography", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "emission_profile_product",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "emission_upstream_profile_company_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("emission_upstream_profile_share",
                            DoubleType(), True),
                StructField("emission_upstream_profile", StringType(), True),
                StructField("benchmark", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "emission_upstream_profile_company",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "emission_upstream_profile_product_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("emission_upstream_profile", StringType(), True),
                StructField("benchmark", StringType(), True),
                StructField("ep_product", StringType(), True),
                StructField("matched_activity_name", StringType(), True),
                StructField("matched_reference_product", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("multi_match", BooleanType(), True),
                StructField("matching_certainty", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("input_name", StringType(), True),
                StructField("input_unit", StringType(), True),
                StructField("input_tilt_sector", StringType(), True),
                StructField("input_tilt_subsector", StringType(), True),
                StructField("input_isic_4digit", StringType(), True),
                StructField("input_isic_4digit_name", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), True),
                StructField("reduction_targets", DoubleType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "emission_upstream_profile_product",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "sector_profile_company_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("sector_profile_share", DoubleType(), True),
                StructField("sector_profile", StringType(), True),
                StructField("scenario", StringType(), True),
                StructField("year", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "sector_profile_company",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "sector_profile_product_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("sector_profile", StringType(), True),
                StructField("scenario", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("ep_product", StringType(), True),
                StructField("matched_activity_name", StringType(), True),
                StructField("matched_reference_product", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("multi_match", BooleanType(), True),
                StructField("matching_certainty", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), True),
                StructField("reduction_targets", DoubleType(), True),
                StructField("isic_4digit", StringType(), True),
                StructField("sector_scenario", StringType(), True),
                StructField("subsector_scenario", StringType(), True),
                StructField("isic_4digit_name", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "sector_profile_product",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "sector_upstream_profile_company_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("sector_profile_upstream_share",
                            DoubleType(), True),
                StructField("sector_profile_upstream", StringType(), True),
                StructField("scenario", StringType(), True),
                StructField("year", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "sector_upstream_profile_company",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "sector_upstream_profile_product_raw": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("sector_profile_upstream", StringType(), True),
                StructField("scenario", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("ep_product", StringType(), True),
                StructField("matched_activity_name", StringType(), True),
                StructField("matched_reference_product", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("tilt_sector", StringType(), True),
                StructField("multi_match", BooleanType(), True),
                StructField("matching_certainty", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("input_name", StringType(), True),
                StructField("input_unit", StringType(), True),
                StructField("input_tilt_sector", StringType(), True),
                StructField("input_tilt_subsector", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), True),
                StructField("reduction_targets", DoubleType(), True),
                StructField("input_isic_4digit", StringType(), True),
                StructField("sector_scenario", StringType(), True),
                StructField("subsector_scenario", StringType(), True),
                StructField("input_isic_4digit_name", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "sector_upstream_profile_product",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "isic_mapper_raw": {
        "columns": StructType(
            [
                StructField("ISIC_Rev_4_label", StringType(), False),
                StructField("Code", StringType(), False),
                StructField("Section_1_digit", StringType(), False),
                StructField("Division_2_digit", StringType(), True),
                StructField("Group_3_digit", StringType(), True),
                StructField("Inclusions", IntegerType(), True),
                StructField("Exclusions", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "isic_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "SBI_activities_raw": {
        "columns": StructType(
            [
                StructField("SBI", StringType(), False),
                StructField("Omschrijving", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "SBI_activities",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tiltLedger_raw": {
        "columns": StructType(
            [
                StructField("CPC_Code", StringType(), True),
                StructField("CPC_Name", StringType(), True),
                StructField("ISIC_Code", StringType(), True),
                StructField("ISIC_Name", StringType(), True),
                StructField("Activity_Type", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Distance", IntegerType(), True),
                StructField("Manual_Review", IntegerType(), True),
                StructField("Verified_Source", IntegerType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "tiltLedger",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "inputProducts_raw": {
        "columns": StructType(
            [
                StructField("Activity_UUID_Product_UUID", StringType(), True),
                StructField("Input_Activity_UUID_Product_UUID",
                            StringType(), True),
                StructField("Product_Name", StringType(), True),
                StructField("Amount", DoubleType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "inputProducts",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "emissionData_raw": {
        "columns": StructType(
            [
                StructField("Activity_UUID_Product_UUID", StringType(), True),
                StructField("elementaryExchangeId", StringType(), True),
                StructField("output_unit", StringType(), True),
                StructField("emission", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("emissions_unit", StringType(), True),
                StructField("carbon_allocation", DoubleType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "emissionData",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "tiltLedger_mapping_raw": {
        "columns": StructType(
            [
                StructField("tiltLedger_id", StringType(), True),
                StructField("company_id", StringType(), True),
                StructField("distance_isic", DoubleType(), True),
                StructField("distance_cpc", DoubleType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "tiltLedger_mapping",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "markus_companies_raw": {
        "columns": StructType(
            [
                StructField('ID_Nummer', StringType(), True),
                StructField('Firmenname', StringType(), True),
                StructField('Adresse', StringType(), True),
                StructField('PLZ', StringType(), True),
                StructField('Stadt', StringType(), True),
                StructField('Land', StringType(), True),
                StructField('Tatigkeitsbeschreibung', StringType(), True),
                StructField('1_Zusatzliche_Beschreibung', StringType(), True),
                StructField('2_Zusatzliche_Beschreibung', StringType(), True),
                StructField('3_Zusatzliche_Beschreibung', StringType(), True),
                StructField('4_Zusatzliche_Beschreibung', StringType(), True),
                StructField('ONACE_Haupttatigkeit_Code', StringType(), True),
                StructField('ONACE_Haupttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('1_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('1_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('2_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('2_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('3_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('3_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('4_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('4_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('5_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('5_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('6_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('6_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('7_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('7_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('8_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('8_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('9_ONACE_Nebenttatigkeit_Code',
                            StringType(), True),
                StructField('9_ONACE_Nebenttatigkeit_Beschreibung',
                            StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "companies_markus",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "markus_matches_raw": {
        "columns": StructType(
            [
                StructField('ID_Nummer', StringType(), True),
                StructField('Firmenname', StringType(), True),
                StructField('Tatigkeitsbeschreibung', StringType(), True),
                StructField('onace_klasse', StringType(), True),
                StructField('ONACE_Haupttatigkeit_Code', StringType(), True),
                StructField('ONACE_Haupttatigkeit_Beschreibung',
                            StringType(), True),
                StructField('ONACE_Numbers', StringType(), True),
                StructField('tiltLedger_id', StringType(), True),
                StructField('CPC_Code', StringType(), True),
                StructField('CPC_Name', StringType(), True),
                StructField('ISIC_Code', StringType(), True),
                StructField('ISIC_Name', StringType(), True),
                StructField('Activity_Type', StringType(), True),
                StructField('Manual_Review', StringType(), True),
                StructField('Verified_Source', StringType(), True),
                StructField('Present_in_EI', StringType(), True),
                StructField('which_activity_type', StringType(), True),
                StructField('match_Jule', StringType(), True),
                StructField('match_Anne', StringType(), True),
                StructField('Tilman', StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "markus_matches",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
    "nace2_isic4_mapper_raw": {
        "columns": StructType(
            [
                StructField("NACE2code", StringType(), True),
                StructField("NACE2part", StringType(), True),
                StructField("ISIC4code", StringType(), True),
                StructField("ISIC4part", StringType(), True),
                StructField("from_date", DateType(), False),
                StructField("to_date", DateType(), False),
                StructField("tiltRecordID", StringType(), False),
            ]
        ),
        "container": "raw",
        "location": "nace2_isic4_mapper",
        "type": "delta",
        "partition_column": "",
        "quality_checks": [],
    },
}
