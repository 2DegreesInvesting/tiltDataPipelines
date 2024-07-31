from datamodel_schema import datamodel_schema


export_string = ""

for table in datamodel_schema:

    column_string = ""

    json_table_columns = datamodel_schema[table]['columns'].jsonValue()[
        'fields']
    for column in json_table_columns:
        column_string += f'{column["name"]} {column["type"]}\n'

    table_string = f'Table {table} {{\n{column_string}}}\n'

    export_string += table_string


with open("schema.dbml", "w") as text_file:
    text_file.write(export_string)
