"""
   This module retrieves the table definition from a predefined dictionary of schemas and their tables.

   Args:
      schema_name (str): The name of the layer to look up in the dictionary (e.g., landingzone, raw, datamodel)
      table_name (str): The name of the table within the specified schema to retrieve.

   Returns:
      dict: The definition of the specified table within the given schema.

   Raises:
      ValueError: If the specified schema_name does not exist in the dictionary, or
                  if the table_name is not provided or does not exist within the specified schema.

   Note:
      - The function requires 'all_table_definitions' to be a predefined dictionary
         where the key is the schema name and the value is another dictionary representing
         tables and their definitions stored in other modules.
      - The table definitions are returned as dictionaries.

"""

from functions.landingzone_schema import landingzone_schema
from functions.raw_schema import raw_schema
from functions.monitoring_schema import monitoring_schema
from functions.datamodel_schema import datamodel_schema
from functions.enriched_datamodel_schema import enriched_datamodel_schema


def get_table_definition(schema_name: str, table_name: str = '') -> dict:

    # Add dictionary
    all_table_definitions = {
        'landingzone_layer': landingzone_schema,
        'raw_layer': raw_schema,
        'monitoring_layer': monitoring_schema,
        'datamodel_layer': datamodel_schema,
        'enriched_layer': enriched_datamodel_schema
    }

    if table_name in ['monitoring_values', 'record_trace']:
        schema_name = 'monitoring_layer'

    # Check if layer exists in the dictionary
    if schema_name not in all_table_definitions:
        raise ValueError(
            f"Value {schema_name} does not exist"
        )
    else:
        # Check if table X exists in layer Y in the dictionary
        if table_name not in all_table_definitions[schema_name]:
            raise ValueError(
                f"Table {table_name} in {schema_name} does not exist"
            )
        else:
            return all_table_definitions[schema_name][table_name]
