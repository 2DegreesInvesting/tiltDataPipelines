"""
This module contains the definition of the tables used in the datahub from the landingzone layer.

Each table is defined as a dictionary with the following keys:
- 'columns': A StructType object defining the schema of the table.
- 'container': The name of the container where the table is stored.
- 'location': The location of the table within the container.
- 'type': The data type of the table.
- 'partition_column': The name of the partition column in the table.
- 'quality_checks': A list of quality checks to be performed on the table.

The `get_table_definition` function is used to retrieve the definition of a specific table.

Example:
    'table_name_container': {
        'columns' :  StructType([
            StructField('string_column', StringType(), False),
            StructField('integer_column', IntegerType(), True),
            StructField('decimal_column', DoubleType(), True),
            StructField('Date_column', DateType(), True),
        ]), 
        'container': 'container_name',
        'location': 'location_in_container',
        'type': ['multiline','csv','delta','parquet'],
        'partition_column' : 'name_of_partition_column',
        'quality_checks': [{
                                'check': 'values are unique',
                                'columns': ['string_columns']
            },{
                                'check': 'values have format',
                                'columns': ['string_column'],
                                'format': r"[a-zA-Z\-]"
            }]
    }
"""

from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    BooleanType,
    DoubleType,
    IntegerType,
    DateType,
)

landingzone_schema = {
    """
        Template for a table:

        'table_name_container': {
            'columns' :  StructType([
                StructField('string_column', StringType(), False),
                StructField('integer_column', IntegerType(), True),
                StructField('decimal_column', DoubleType(), True),
                StructField('Date_column', DateType(), True),
            ]  
            ), 
            'container': 'container_name',
            'location': 'location_in_container',
            'type': 'data_type',
            'partition_column' : 'name_of_partition_column',
            'quality_checks': [{
                                    'check': 'values are unique',
                                    'columns': ['string_columns']
                },{
                                    'check': 'values have format',
                                    'columns': ['string_column'],
                                    'format': r"[a-zA-Z\-]"
                }]
        }
        """
    "test_table_landingzone": {
        "columns": StructType(
            [
                StructField("test_string_column", StringType(), False),
                StructField("test_integer_column", IntegerType(), False),
                StructField("test_decimal_column", DoubleType(), True),
                StructField("test_date_column", DateType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "test/test_table.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "companies_europages_landingzone": {
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
                StructField("min_headcount", StringType(), True),
                StructField("max_headcount", StringType(), True),
                StructField(
                    "type_of_building_for_registered_address", StringType(), True
                ),
                StructField("verified_by_europages", StringType(), True),
                StructField("year_established", StringType(), True),
                StructField("websites", StringType(), True),
                StructField("download_datetime", StringType(), True),
                StructField("id", StringType(), False),
                StructField("filename", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "tiltEP/",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [{"check": "values are unique", "columns": ["id"]}],
    },
    "companies_companyinfo_landingzone": {
        "columns": StructType(
            [
                StructField("Sizokey", StringType(), False),
                StructField("Kamer_van_Koophandel_nummer",
                            StringType(), False),
                StructField(
                    "Kamer_van_Koophandel_nummer_12-cijferig", StringType(), False
                ),
                StructField("RSIN-nummer", StringType(), True),
                StructField("Vestigingsnummer", StringType(), True),
                StructField("Instellingsnaam", StringType(), True),
                StructField("Statutaire_naam", StringType(), True),
                StructField("Handelsnaam_1", StringType(), True),
                StructField("Handelsnaam_2", StringType(), True),
                StructField("Handelsnaam_3", StringType(), True),
                StructField("Bedrijfsomschrijving", StringType(), True),
                StructField("Vestigingsadres", StringType(), True),
                StructField("Vestigingsadres_straat", StringType(), True),
                StructField("Vestigingsadres_huisnummer", StringType(), True),
                StructField("Vestigingsadres_toevoeging", StringType(), True),
                StructField("Vestigingsadres_postcode", StringType(), True),
                StructField("Vestigingsadres_plaats", StringType(), True),
                StructField("Plan_Naam_1", StringType(), True),
                StructField("Kern_Naam_1", StringType(), True),
                StructField("KIX", StringType(), True),
                StructField("4-cijferige_Postcode", StringType(), True),
                StructField("Pand_ID", StringType(), True),
                StructField("Rechtsvorm_Omschrijving", StringType(), True),
                StructField("Aantal_medewerkers", StringType(), True),
                StructField("Klasse_aantal_medewerkers", StringType(), True),
                StructField(
                    "Klasse_aantal_medewerkers_Omschrijving", StringType(), True
                ),
                StructField(
                    "Aantal_medewerkers_van_concern_op_locatie", StringType(), True
                ),
                StructField("Klasse_aantal_medewerkers_locatie",
                            StringType(), True),
                StructField(
                    "Klasse_aantal_medewerkers_locatie_Omschrijving", StringType(), True
                ),
                StructField("SBI-code", StringType(), True),
                StructField("SBI-code_Omschrijving", StringType(), True),
                StructField("SBI-code_2-cijferig", StringType(), True),
                StructField("SBI-code_2-cijferig_Omschrijving",
                            StringType(), True),
                StructField("SBI-code_segment", StringType(), True),
                StructField("SBI-code_segment_Omschrijving",
                            StringType(), True),
                StructField("NACE-code", StringType(), True),
                StructField("NACE-code_Omschrijving", StringType(), True),
                StructField("SBI-code_locatie", StringType(), True),
                StructField("SBI-code_locatie_Omschrijving",
                            StringType(), True),
                StructField("SBI-code_2-cijferig_locatie", StringType(), True),
                StructField(
                    "SBI-code_2-cijferig_locatie_Omschrijving", StringType(), True
                ),
                StructField("SBI-code_segment_locatie", StringType(), True),
                StructField(
                    "SBI-code_segment_locatie_Omschrijving", StringType(), True
                ),
                StructField("Concernrelatie_Omschrijving", StringType(), True),
                StructField("Sizokey_ultimate_parent", StringType(), True),
                StructField("Instellingsnaam_ultimate_parent",
                            StringType(), True),
                StructField("Aantal_instellingen_in_concern",
                            StringType(), True),
                StructField("Aantal_locaties_van_concern", StringType(), True),
                StructField("Klasse_aantal_locaties_van_concern",
                            StringType(), True),
                StructField(
                    "Klasse_aantal_locaties_van_concern_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField("Aantal_medewerkers_concern", StringType(), True),
                StructField("Klasse_aantal_medewerkers_concern",
                            StringType(), True),
                StructField(
                    "Klasse_aantal_medewerkers_concern_Omschrijving", StringType(), True
                ),
                StructField("SBI-code_concern", StringType(), True),
                StructField("SBI-code_concern_Omschrijving",
                            StringType(), True),
                StructField("SBI-code_2-cijferig_concern", StringType(), True),
                StructField(
                    "SBI-code_2-cijferig_concern_Omschrijving", StringType(), True
                ),
                StructField("SBI-code_segment_concern", StringType(), True),
                StructField(
                    "SBI-code_segment_concern_Omschrijving", StringType(), True
                ),
                StructField("Subsidie_bedrag_concern", StringType(), True),
                StructField("Klasse_subsidie_bedrag_concern",
                            StringType(), True),
                StructField(
                    "Klasse_subsidie_bedrag_concern_Omschrijving", StringType(), True
                ),
                StructField(
                    "Grootverbruik_of_kleinverbruik_Omschrijving", StringType(), True
                ),
                StructField("Bouwjaar_verblijfsobject", StringType(), True),
                StructField("Oppervlakte_verblijfsobject", StringType(), True),
                StructField(
                    "Klasse_oppervlakte_verblijfsobject_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField(
                    "Hoofd_gebruiksdoel_verblijfsobject_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField(
                    "Energielabel_verblijfsobject_Omschrijving", StringType(), True
                ),
                StructField(
                    "Energielabel_status_verblijfsobject_Omschrijving",
                    StringType(),
                    True,
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
                StructField("Totaal_aantal_personenwagens_locatie",
                            StringType(), True),
                StructField(
                    "Totaal_aantal_lichte_bedrijfswagens_locatie", StringType(), True
                ),
                StructField("Totaal_aantal_vrachtwagens_locatie",
                            StringType(), True),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_personenwagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_lichte_bedrijfswagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_vrachtwagens_locatie",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_personenwagens_locatie_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_lichte_bedrijfswagens_locatie_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_vrachtwagens_locatie_Omschrijving",
                    StringType(),
                    True,
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
                StructField("Totaal_aantal_personenwagens_concern",
                            StringType(), True),
                StructField(
                    "Totaal_aantal_lichte_bedrijfswagens_concern", StringType(), True
                ),
                StructField("Totaal_aantal_vrachtwagens_concern",
                            StringType(), True),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_personenwagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_lichte_bedrijfswagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_waarde_totaal_aantal_vrachtwagens_concern",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_personenwagens_concern_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_lichte_bedrijfswagens_concern_Omschrijving",
                    StringType(),
                    True,
                ),
                StructField(
                    "Voorspelling_klasse_totaal_aantal_vrachtwagens_concern_Omschrijving",
                    StringType(),
                    True,
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
            ]
        ),
        "container": "landingzone",
        "location": "CompanyInfo/company_info_20240426.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "multi_SBI_companyinfo_landingzone": {
        "columns": StructType(
            [
                StructField("SIZOKEY", StringType(), False),
                StructField("SIZOSBI", StringType(), False),
                StructField("SBI-Code", StringType(), False),
                StructField("SIZRVOLGNR", StringType(), False),
                StructField("Soort_SBI-code_Kamer_van_Koophandel", StringType(), False),
                StructField("Bron_ID", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "CompanyInfo/multi_sbi_20240718.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "country_landingzone": {
        "columns": StructType(
            [
                StructField("country_id", StringType(), False),
                StructField("country", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/country.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    # 'geography_mapper_landingzone': {
    #     'columns':  StructType([
    #         StructField('geography_id', StringType(), False),
    #         StructField('country_id', StringType(), True),
    #         StructField('lca_geo', StringType(), True),
    #         StructField('priority', StringType(), True),
    #         StructField('input_priority', StringType(), True)
    #     ]
    #     ),
    #     'container': 'landingzone',
    #     'location': 'tiltIndicatorBefore/geography_mapper.csv',
    #     'type': 'multiline',
    #     'partition_column': '',
    #     'quality_checks': []
    # },
    "main_activity_ecoinvent_mapper_landingzone": {
        "columns": StructType(
            [
                StructField("main_activity_id", StringType(), False),
                StructField("main_activity", StringType(), True),
                StructField("ei_activity_name", StringType(), True),
            ]
        ),
        'container': 'landingzone',
        'location': 'mappers/main_activity_ecoinvent_mapper.csv',
        'type': 'multiline',
        'partition_column': '',
        'quality_checks': []
    },
    "sources_mapper_landingzone": {
        "columns": StructType(
            [
                StructField("source_id", StringType(), False),
                StructField("source_name", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/sources_mapper.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "countries_mapper_landingzone": {
        "columns": StructType(
            [
                StructField("country_un", StringType(), False),
                StructField("country", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/countries_mapper.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "geography_mapper_landingzone": {
        "columns": StructType(
            [
                StructField("geography_id", StringType(), False),
                StructField("country_id", StringType(), False),
                StructField("lca_geo", StringType(), False),
                StructField("priority", StringType(), False),
                StructField("input_priority", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/geography_mapper.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },

    'EP_tilt_sector_mapper_landingzone': {
        'columns':  StructType([
            StructField('categories_id', StringType(), False),
            StructField('group', StringType(), True),
            StructField('ep_sector', StringType(), False),
            StructField('ep_subsector', StringType(), True),
            StructField('tilt_sector', StringType(), True),
            StructField('tilt_subsector', StringType(), True)
        ]
        ),
        "container": "landingzone",
        "location": "mappers/EP_tilt_sector_mapper.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "tilt_isic_mapper_landingzone": {
        "columns": StructType(
            [
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("isic_4digit", StringType(), True),
                StructField("isic_section", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/tilt_sectors_isic_mapper_20240826.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_tilt_mapper_2023-07-20_landingzone": {
        "columns": StructType(
            [
                StructField("tilt_sector", StringType(), True),
                StructField("tilt_subsector", StringType(), True),
                StructField("weo_product", StringType(), True),
                StructField("weo_flow", StringType(), True),
                StructField("ipr_sector", StringType(), True),
                StructField("ipr_subsector", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/scenario_tilt_mapper_2023-07-20.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_targets_IPR_NEW_landingzone": {
        "columns": StructType(
            [
                StructField("Scenario", StringType(), False),
                StructField("Region", StringType(), True),
                StructField("Variable Class", StringType(), True),
                StructField("Sub Variable Class", StringType(), True),
                StructField("Sector", StringType(), True),
                StructField("Sub Sector", StringType(), True),
                StructField("Units", StringType(), True),
                StructField("Year", StringType(), False),
                StructField("Value", StringType(), False),
                StructField("Reductions", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "scenario/scenario_targets_IPR_NEW.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "scenario_targets_WEO_NEW_landingzone": {
        "columns": StructType(
            [
                StructField("PUBLICATION", StringType(), False),
                StructField("SCENARIO", StringType(), True),
                StructField("CATEGORY", StringType(), True),
                StructField("PRODUCT", StringType(), True),
                StructField("FLOW", StringType(), True),
                StructField("UNIT", StringType(), True),
                StructField("REGION", StringType(), True),
                StructField("YEAR", StringType(), True),
                StructField("VALUE", StringType(), False),
                StructField("REDUCTIONS", StringType(), False),
            ]
        ),
        "container": "landingzone",
        "location": "scenario/scenario_targets_WEO_NEW.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    'geographies_landingzone': {
        'columns':  StructType([
            StructField('ID', StringType(), False),
            StructField('Name', StringType(), True),
            StructField('Shortname', StringType(), True),
            StructField('Latitude', StringType(), True),
            StructField('Longitude', StringType(), True),
            StructField('Geographical_Classification', StringType(), True),
            StructField('Contained_and_Overlapping_Geographies',
                        StringType(), True),
        ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/Geographies.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "undefined_ao_landingzone": {
        "columns": StructType(
            [
                StructField("Activity UUID", StringType(), True),
                StructField("EcoQuery URL", StringType(), True),
                StructField("Activity Name", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Time Period", StringType(), True),
                StructField("Special Activity Type", StringType(), True),
                StructField("Sector", StringType(), True),
                StructField("ISIC Classification", StringType(), True),
                StructField("ISIC Section", StringType(), True),
                StructField("Product UUID", StringType(), True),
                StructField("Product Group", StringType(), True),
                StructField("Product Name", StringType(), True),
                StructField("CPC Classification", StringType(), True),
                StructField("Unit", StringType(), True),
                StructField("Product Information", StringType(), True),
                StructField("CAS Number", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/Undefined AO.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    'cut_off_ao_landingzone': {
        'columns':  StructType([
            StructField('Activity UUID & Product UUID',
                        StringType(), True),
            StructField('Activity UUID', StringType(), True),
            StructField('EcoQuery URL', StringType(), True),
            StructField('Activity Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time Period', StringType(), True),
            StructField('Special Activity Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC Classification', StringType(), True),
            StructField('ISIC Section', StringType(), True),
            StructField('Product UUID', StringType(), True),
            StructField('Reference Product Name', StringType(), True),
            StructField('CPC Classification', StringType(), True),
            StructField('HS2017 Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product Information', StringType(), True),
            StructField('CAS Number', StringType(), True),
            StructField('Cut-Off Classification', StringType(), True)
        ]
        ),
        'container': 'landingzone',
        'location': 'ecoInvent/Cut-Off AO.csv',
        'type': 'multiline',
        'partition_column': '',
        'quality_checks': []
    },
    'en15804_ao_landingzone': {
        'columns':  StructType([
            StructField('Activity UUID & Product UUID',
                        StringType(), True),
            StructField('Activity UUID', StringType(), True),
            StructField('EcoQuery URL', StringType(), True),
            StructField('Activity Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time Period', StringType(), True),
            StructField('Special Activity Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC Classification', StringType(), True),
            StructField('ISIC Section', StringType(), True),
            StructField('Product UUID', StringType(), True),
            StructField('Reference Product Name', StringType(), True),
            StructField('CPC Classification', StringType(), True),
            StructField('HS2017_Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product Information', StringType(), True),
            StructField('CAS Number', StringType(), True),
            StructField('Cut-Off Classification', StringType(), True)
        ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/EN15804 AO.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    'apos_ao_landingzone': {
        'columns':  StructType([
            StructField('Activity UUID & Product UUID',
                        StringType(), True),
            StructField('Activity UUID', StringType(), True),
            StructField('EcoQuery URL', StringType(), True),
            StructField('Activity Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time Period', StringType(), True),
            StructField('Special Activity Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC Classification', StringType(), True),
            StructField('ISIC Section', StringType(), True),
            StructField('Product UUID', StringType(), True),
            StructField('Reference Product Name', StringType(), True),
            StructField('CPC Classification', StringType(), True),
            StructField('HS2017_Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product Information', StringType(), True),
            StructField('CAS Number', StringType(), True),
            StructField('APOS Classification', StringType(), True)

        ]
        ),
        'container': 'landingzone',
        'location': 'ecoInvent/APOS AO.csv',
        'type': 'multiline',
        'partition_column': '',
        'quality_checks': []
    },
    'consequential_ao_landingzone': {
        'columns':  StructType([
            StructField('Activity UUID & Product UUID',
                        StringType(), True),
            StructField('Activity UUID', StringType(), True),
            StructField('EcoQuery URL', StringType(), True),
            StructField('Activity Name', StringType(), True),
            StructField('Geography', StringType(), True),
            StructField('Time Period', StringType(), True),
            StructField('Special Activity Type', StringType(), True),
            StructField('Technology Level', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('ISIC Classification', StringType(), True),
            StructField('ISIC Section', StringType(), True),
            StructField('Product UUID', StringType(), True),
            StructField('Reference Product Name', StringType(), True),
            StructField('CPC Classification', StringType(), True),
            StructField('Unit', StringType(), True),
            StructField('Product Information', StringType(), True),
            StructField('CAS Number', StringType(), True)
        ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/Consequential AO.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "lcia_methods_landingzone": {
        "columns": StructType(
            [
                StructField("Method Name", StringType(), True),
                StructField("Status", StringType(), True),
                StructField("Method Version", StringType(), True),
                StructField("Further Documentation", StringType(), True),
                StructField(
                    "Links to Characterization Factor Successes", StringType(), True
                ),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/LCIA Methods.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "impact_categories_landingzone": {
        "columns": StructType(
            [
                StructField("Resources - Emissions - Total",
                            StringType(), True),
                StructField("Main impact/damage category", StringType(), True),
                StructField(
                    "Inventory - Midpoint - Endpoint - AoP", StringType(), True
                ),
                StructField("Area of Protection (AoP)", StringType(), True),
                StructField("Used in EN15804", StringType(), True),
                StructField("Method", StringType(), True),
                StructField("Category", StringType(), True),
                StructField("Indicator", StringType(), True),
                StructField("Unit", StringType(), True),
                StructField("Category name in method", StringType(), True),
                StructField("Indicator name in method", StringType(), True),
                StructField("Unit in method", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/Impact Categories.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "intermediate_exchanges_landingzone": {
        "columns": StructType(
            [
                StructField("ID", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Unit Name", StringType(), True),
                StructField("CAS Number", StringType(), True),
                StructField("Comment", StringType(), True),
                StructField("By-product Classification", StringType(), True),
                StructField("CPC Classification", StringType(), True),
                StructField("Product Information", StringType(), True),
                StructField("Synonym", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/Intermediate Exchanges.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "elementary_exchanges_landingzone": {
        "columns": StructType(
            [
                StructField("ID", StringType(), False),
                StructField("Name", StringType(), True),
                StructField("Compartment", StringType(), True),
                StructField("Sub Compartment", StringType(), True),
                StructField("Unit Name", StringType(), True),
                StructField("CAS Number", StringType(), True),
                StructField("Comment", StringType(), True),
                StructField("Synonym", StringType(), True),
                StructField("Formula", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/Elementary Exchanges.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "cut-off_cumulative_LCIA_v_X_landingzone": {
        "columns": StructType(
            [
                StructField("Activity UUID_Product UUID", StringType(), False),
                StructField("Activity Name", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Reference Product Name", StringType(), True),
                StructField("Reference Product Unit", StringType(), True),
                StructField(
                    "IPCC 2021 climate change global warming potential (GWP100) kg CO2-Eq",
                    StringType(),
                    True,
                ),
            ]
        ),
        "container": "landingzone",
        # file that is created by extracting certain columns from the licensed data
        # the file name will change based on the availability of the new version
        "location": "ecoInvent/cut-off_cumulative_LCIA_v10.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "ep_ei_matcher_landingzone": {
        "columns": StructType(
            [
                StructField("group_var", StringType(), False),
                StructField("ep_id", StringType(), True),
                StructField("ep_country", StringType(), True),
                StructField("ep_main_act", StringType(), True),
                StructField("ep_clustered", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), True),
                StructField("multi_match", StringType(), True),
                StructField("completion", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "tiltIndicatorBefore/ep_ei_matcher.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "mapper_ep_ei_landingzone": {
        "columns": StructType(
            [
                StructField("country", StringType(), True),
                StructField("main_activity", StringType(), True),
                StructField("clustered", StringType(), True),
                StructField("activity_uuid_product_uuid", StringType(), False),
                StructField("multi_match", StringType(), True),
                StructField("completion", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "tiltIndicatorAfter/mapper_ep_ei.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "emission_profile_company_landingzone": {
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/emission_profile_company",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "emission_profile_product_landingzone": {
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/emission_profile_product",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "emission_upstream_profile_company_landingzone": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("company_city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("emission_usptream_profile_share",
                            DoubleType(), True),
                StructField("emission_usptream_profile", StringType(), True),
                StructField("benchmark", StringType(), True),
                StructField("matching_certainty_company_average",
                            StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("address", StringType(), True),
                StructField("main_activity", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/emissions_profile_upstream_at_company_level",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "emission_upstream_profile_product_landingzone": {
        "columns": StructType(
            [
                StructField("companies_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("emission_usptream_profile", StringType(), True),
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/emissions_profile_upstream_at_product_level",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "sector_profile_company_landingzone": {
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/sector_profile_at_company_level",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "sector_profile_product_landingzone": {
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/sector_profile_at_product_level",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "sector_upstream_profile_company_landingzone": {
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/sector_profile_upstream_at_company_level",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "sector_upstream_profile_product_landingzone": {
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
            ]
        ),
        "container": "landingzone",
        "location": "wrapperOutput/sector_profile_upstream_at_product_level",
        "type": "parquet",
        "partition_column": "batch",
        "quality_checks": [],
    },
    "isic_4_digit_codes_landingzone": {
        "columns": StructType(
            [
                StructField("ISIC Rev 4 label", StringType(), True),
                StructField("Code", StringType(), True),
                StructField("Section (1-digit)", StringType(), True),
                StructField("Division (2-digit)", StringType(), True),
                StructField("Group (3-digit)", StringType(), True),
                StructField("Inclusions", IntegerType(), True),
                StructField("Exclusions", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "activityCodes/ISIC4DigitCodes.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "SBI_activities_landingzone": {
        "columns": StructType(
            [
                StructField("SBI", StringType(), False),
                StructField("Omschrijving", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "mappers/sbi.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "tiltLedger_landingzone": {
        "columns": StructType(
            [
                StructField("CPC_Code", StringType(), True),
                StructField("CPC_Name", StringType(), True),
                StructField("ISIC_Code", StringType(), True),
                StructField("ISIC_Name", StringType(), True),
                StructField("Activity_Type", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Distance", StringType(), True),
                StructField("Manual_Review", StringType(), True),
                StructField("Verified_Source", StringType(), True)
            ]
        ),
        "container": "landingzone",
        "location": "tiltLedger/20240729_Ledger_v53.csv",
        "type": "csv_pipe_sep",
        "partition_column": "",
        "quality_checks": [],
    },
    "inputProducts_landingzone": {
        "columns": StructType(
            [
                StructField("Activity_UUID_Product_UUID", StringType(), True),
                StructField("Input_Activity_UUID_Product_UUID",
                            StringType(), True),
                StructField("Product_Name", StringType(), True),
                # Here we are using a not string type since it is coming from a parquet file.
                StructField("Amount", DoubleType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/input_products_v_310.parquet",
        "type": "parquet",
        "partition_column": "",
        "quality_checks": [],
    },
    "emissionData_landingzone": {
        "columns": StructType(
            [
                StructField("Activity_UUID_Product_UUID", StringType(), True),
                StructField("elementaryExchangeId", StringType(), True),
                StructField("output_unit", StringType(), True),
                StructField("emission", StringType(), True),
                # Here we are using a not string type since it is coming from a parquet file.
                StructField("amount", DoubleType(), True),
                StructField("emissions_unit", StringType(), True),
                # Here we are using a not string type since it is coming from a parquet file.
                StructField("carbon_allocation", DoubleType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/elementary_exchanges_v_310.parquet",
        "type": "parquet",
        "partition_column": "",
        "quality_checks": [],
    },
    "scope_2_and_3_mandatory_ghgs_landingzone": {
        "columns": StructType(
            [
                StructField("Activity_Name", StringType(), True),
                StructField("Geography", StringType(), True),
                StructField("Reference_Product", StringType(), True),
                StructField("scope_2kg_CO2_eq_kWh", DoubleType(), True),
                StructField("scope_3_+_transmission_and_distribution_losseskg_CO2_eq_kWh", DoubleType(), True),
                StructField("scope_3kg_CO2_eq_kWh", DoubleType(), True),
                StructField("scope_3_transmission_and_distribution_losseskg_CO2_eq_kWh", DoubleType(), True),
                StructField("comment", StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "ecoInvent/scope_2_and_3_mandatory_ghgs.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "markus_companies_landingzone": {
        "columns": StructType(
            [
                StructField('ID Nummer', StringType(), True),
                StructField('Firmenname', StringType(), True),
                StructField('Adresse', StringType(), True),
                StructField('PLZ', StringType(), True),
                StructField('Stadt', StringType(), True),
                StructField('Land', StringType(), True),
                StructField('Tatigkeitsbeschreibung', StringType(), True),
                StructField('1 Zusatzliche Beschreibung', StringType(), True),
                StructField('2 Zusatzliche Beschreibung', StringType(), True),
                StructField('3 Zusatzliche Beschreibung', StringType(), True),
                StructField('4 Zusatzliche Beschreibung', StringType(), True),
                StructField('ONACE Haupttatigkeit Code', StringType(), True),
                StructField('ONACE Haupttatigkeit Beschreibung',
                            StringType(), True),
                StructField('1 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('1 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('2 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('2 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('3 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('3 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('4 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('4 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('5 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('5 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('6 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('6 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('7 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('7 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('8 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('8 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
                StructField('9 ONACE Nebenttatigkeit Code',
                            StringType(), True),
                StructField('9 ONACE Nebenttatigkeit Beschreibung',
                            StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "markus_companies/Markus Company Extract.csv",
        "type": "multiline",
        "partition_column": "",
        "quality_checks": [],
    },
    "markus_matches_landingzone": {
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
                StructField('which activity type', StringType(), True),
                StructField('match Jule', StringType(), True),
                StructField('match Anne', StringType(), True),
                StructField('Tilman', StringType(), True),
            ]
        ),
        "container": "landingzone",
        "location": "markus_companies/markus_companies_20240823 - Final matches Anne.csv",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    },
    "nace2_isic4_mapper_landingzone": {
        "columns": StructType(
            [
                StructField("NACE2code", StringType(), True),
                StructField("NACE2part", StringType(), True),
                StructField("ISIC4code", StringType(), True),
                StructField("ISIC4part", StringType(), True)
            ]
        ),
        "container": "landingzone",
        "location": "mappers/NACE2_ISIC4.txt",
        "type": "csv",
        "partition_column": "",
        "quality_checks": [],
    }
}
