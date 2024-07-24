
def create_catalog_table(table_name: str, schema: dict) -> str:
    """
    Creates a SQL string to recreate a table in Delta Lake format.

    This function constructs a SQL string that can be used to create a table in Delta Lake format.
    The table is created with the provided name and schema. If the schema includes a partition column,
    the table is partitioned by that column.

    Parameters
    ----------
    table_name : str
        The name of the table to be created.
    schema : dict
        The schema of the table to be created. The schema should be a dictionary with a 'columns' key
        containing a list of dictionaries, each representing a column. Each column dictionary should
        have 'name', 'type', and 'nullable' keys. The schema can optionally include a 'partition_column'
        key with the name of the column to partition the table by.

    Returns
    -------
    str
        A SQL string that can be used to create the table in Delta Lake format.
    """

    if not schema["columns"]:
        raise ValueError("The provided schema can not be empty")

    create_catalog_table_string = ""

    # Build a SQL string to recreate the table in the most up to date format
    create_catalog_table_string = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    for i in schema["columns"]:
        col_info = i.jsonValue()
        # If column is an array, for example
        if isinstance(col_info["type"], dict):
            col_type_parent = col_info["type"]["type"]
            col_type_child = col_info["type"]["elementType"]

            col_type = f"{col_type_parent}<{col_type_child}>"
        else:
            col_type = col_info["type"]

        col_string = f"`{col_info['name']}` {col_type} {'NOT NULL' if not col_info['nullable'] else ''},"
        create_catalog_table_string += col_string

    create_catalog_table_string = create_catalog_table_string[:-1] + ")"
    create_catalog_table_string += " USING DELTA "
    if schema["partition_column"]:
        create_catalog_table_string += (
            f"PARTITIONED BY (`{schema['partition_column']}` STRING)"
        )

    return create_catalog_table_string


def create_catalog_schema(environment: str, schema: dict) -> str:
    """
    Creates a catalog schema if it doesn't already exist.

    Args:
        environment (str): The environment in which the schema should be created.
        schema (dict): A dictionary containing the schema details, including the container name.

    Returns:
        str: The SQL string for creating the catalog schema.
    """

    create_catalog_schema_string = (
        f'CREATE SCHEMA IF NOT EXISTS {environment}.{schema["container"]};'
    )
    create_catalog_schema_owner = (
        f'ALTER SCHEMA {environment}.{schema["container"]} SET OWNER TO tiltDevelopers;'
    )

    return create_catalog_schema_string, create_catalog_schema_owner


def create_catalog_table_owner(table_name: str) -> str:
    """
    Creates a SQL string to set the owner of a table.

    Args:
        table_name (str): The name of the table.

    Returns:
        str: The SQL string to set the owner of the table.
    """

    create_catalog_table_owner_string = (
        f"ALTER TABLE {table_name} SET OWNER TO tiltDevelopers"
    )

    return create_catalog_table_owner_string


def get_user_group_users(user_group_name: str) -> list:

    user_groups_dict = {
        'tiltDevelopers': [
            'alexjang@2degrees-investing.org',
            'letizialavanzini@2degrees-investing.org',
            'lyho@deloitte.nl',
            's.kruthoff@2degrees-investing.org',
            'selsabrouty@deloitte.nl',
            'y.sherstyuk@2degrees-investing.org'
        ]
    }

    if user_group_name not in user_groups_dict:
        raise ValueError(f"User group {user_group_name} does not exist")

    users = user_groups_dict.get(user_group_name)

    return users


def create_user_group(user_group_name: str) -> str:

    user_list = get_user_group_users(user_group_name)

    sql_string = f"CREATE GROUP {user_group_name} WITH USER "

    for user in user_list:

        sql_string += f"`{user}`,"

    sql_string = sql_string[:-1] + ";"

    return sql_string


def delte_user_group(user_group_name: str) -> str:

    sql_string = f"DROP GROUP {user_group_name};"

    return sql_string


def assign_user_group_table(user_group_name: str, table_name: str):

    sql_string = f"ALTER TABLE `{table_name}` SET OWNER TO {user_group_name};"

    return sql_string
