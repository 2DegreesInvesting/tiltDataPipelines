from landingzone_schema import landingzone_schema
from raw_schema import raw_schema
from monitoring_schema import monitoring_schema

def get_table_definition(schema_name: str, table_name: str = '') -> dict:
   # print(schema_name)

   # add dictionary
   all_table_definitions = {
   'landingzone_layer' : landingzone_schema,
   'raw_layer' : raw_schema,
   'monitoring_layer' : monitoring_schema
}
   if table_name == 'monitoring_values':
      schema_name = 'monitoring_layer'
   # print(table_name)
   # landingzone_layer = all_table_definitions['landingzone_layer']
   # test = landingzone_layer[table_name]
   
   for layer in all_table_definitions:
      print(f"Dictionary: {layer}")
      for schema_name, table_name in all_table_definitions[schema_name].items():
         if schema_name in all_table_definitions:
         # print(f"  Key: {schema_name}, Value: {table_name}")
         # return schema_name, table_name
   # schema = all_table_definitions.get(schema_name)

   # Check if the layer exists
   # for schema_name in all_table_definitions:
   #    if schema_name in test:
   #       pass
   #    else:
   #       raise ValueError(
   #          f"Value {schema_name} does not exist"
   #       )

            if not table_name:
               return schema_name

            return table_name
      else:
               raise ValueError(
         f"Value {layer} does not exist"
                  )

 # layer exists - error - layer doesn't exist
 # if table exists in the layer -> error - table X in layer X doesn't exist
   


schema_name = 'monitoring_layer'
table_name = 'monitoring_values'
print(get_table_definition(schema_name, table_name))
print(schema_name)