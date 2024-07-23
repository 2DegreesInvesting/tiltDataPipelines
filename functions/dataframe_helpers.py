import re
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def create_map_column(dataframe: DataFrame, dataframe_name: str) -> DataFrame:
    """
    Creates a new column in the dataframe with a map containing the dataframe name as the key and the 'tiltRecordID' column as the value.

    Args:
        dataframe (DataFrame): The input dataframe.
        dataframe_name (str): The name of the dataframe.

    Returns:
        DataFrame: The dataframe with the new map column.
    """
    dataframe = dataframe.withColumn(
        f"map_{dataframe_name}",
        F.create_map(F.lit(dataframe_name), F.array(F.col("tiltRecordID"))),
    )
    return dataframe


def create_sha_values(data_frame: DataFrame, col_list: list) -> DataFrame:
    """
    Creates SHA values for the specified columns in the DataFrame.

    Args:
        data_frame (DataFrame): The input DataFrame.
        col_list (list): The list of column names to create SHA values for.

    Returns:
        DataFrame: The DataFrame with the 'shaValue' column added, containing the SHA values.

    """
    data_frame = data_frame.withColumn(
        "shaValue", F.sha2(F.concat_ws("|", *col_list), 256)
    )

    return data_frame


def create_table_path(environment: str, schema: dict, partition_name: str = "") -> str:
    """
    Creates a table path based on the given environment, schema, and optional partition name.

    Args:
        environment (str): The environment name.
        schema (dict): The schema dictionary containing container, location, and partition_column.
        partition_name (str, optional): The name of the partition. Defaults to ''.

    Returns:
        str: The table path.

    """
    if schema["container"] == "landingzone":
        if partition_name:
            return f"abfss://{schema['container']}@storagetilt{environment}.dfs.core.windows.net/{schema['location']}/{schema['partition_column']}={partition_name}"

        return f"abfss://{schema['container']}@storagetilt{environment}.dfs.core.windows.net/{schema['location']}/"
    return ""


def create_table_name(environment: str, container: str, location: str) -> str:
    """
    Creates a table name by combining the environment, container, and location.

    Args:
        environment (str): The environment name.
        container (str): The container name.
        location (str): The location name.

    Returns:
        str: The formatted table name.
    """
    return f"`{environment}`.`{container}`.`{location.replace('.','')}`"


def clean_column_names(data_frame: DataFrame) -> DataFrame:
    """
    Cleans the column names of a DataFrame by removing special characters,
    replacing spaces with underscores, and removing parentheses.

    Args:
        data_frame (DataFrame): The input DataFrame with column names to be cleaned.

    Returns:
        DataFrame: The DataFrame with cleaned column names.
    """
    for col in data_frame.columns:
        if re.search(r"[-\\\/\(\)\s]", col):
            new_col_name = re.sub(r"[-\\\/]", " ", col)
            new_col_name = re.sub(r"[\(\)]", "", new_col_name)
            new_col_name = re.sub(r"\s+", "_", new_col_name)
            data_frame = data_frame.withColumnRenamed(col, new_col_name)
    return data_frame


def apply_scd_type_2(new_table: DataFrame, existing_table: DataFrame) -> DataFrame:
    """
    Applies Slowly Changing Dimension (SCD) Type 2 logic to merge new and existing dataframes.

    Args:
        new_table (DataFrame): The new dataframe containing the updated records.
        existing_table (DataFrame): The existing dataframe containing the current records.

    Returns:
        DataFrame: The merged dataframe with updated records based on SCD Type 2 logic.
    """
    # Determine the processing date
    processing_date = F.current_date()
    future_date = F.lit("2099-12-31")
    map_col = ""
    from_to_list = [F.col("from_date"), F.col("to_date")]

    # Check if the new table contains a map column
    if [col for col in new_table.columns if col.startswith("map_")]:
        # This is supposed to check if we are creating the the monitoring_valus table
        if not "signalling_id" in existing_table.columns:
            map_col = [
                col for col in new_table.columns if col.startswith("map_")][0]
            existing_table = existing_table.withColumn(
                map_col, F.create_map().cast("Map<String, Array<String>>")
            )
            from_to_list += [F.col(map_col)]
        else:
            map_col = "map_monitoring_values"

    # Select the columns that contain values that should be compared
    value_columns = [
        F.col(col_name)
        for col_name in new_table.columns
        if col_name not in ["tiltRecordID", "from_date", "to_date"]
        and col_name != map_col
    ]

    old_closed_records = existing_table.filter(F.col("to_date") != future_date).select(
        value_columns + from_to_list
    )
    old_df = existing_table.filter(F.col("to_date") == future_date).select(
        value_columns + from_to_list
    )

    # Add the SHA representation of the old records and rename to unique name
    old_df = create_sha_values(old_df, value_columns)
    old_df = old_df.withColumnRenamed("shaValue", "shaValueOld")

    # Add the SHA representation of the incoming records and rename to unique name
    new_data_frame = create_sha_values(new_table, value_columns)
    new_data_frame = new_data_frame.withColumn("from_date", processing_date).withColumn(
        "to_date", F.to_date(future_date)
    )
    new_data_frame = new_data_frame.withColumnRenamed(
        "shaValue", "shaValueNew")

    # Join the SHA values of both tables together
    combined_df = new_data_frame.select(F.col("shaValueNew")).join(
        old_df.select("shaValueOld"),
        on=old_df.shaValueOld == new_data_frame.shaValueNew,
        how="full",
    )

    # Set the base of all records to the already expired/ closed records
    all_records = old_closed_records
    # Records that did not change are taken from the existing set of data
    identical_records = combined_df.filter(
        (F.col("shaValueOld").isNotNull()) & (F.col("shaValueNew").isNotNull())
    )
    if identical_records.count() > 0:
        identical_records = combined_df.filter(
            (F.col("shaValueOld").isNotNull()) & (
                F.col("shaValueNew").isNotNull())
        ).join(old_df, on="shaValueOld", how="inner")
        identical_records = identical_records.select(
            value_columns + from_to_list)
        all_records = all_records.union(identical_records)

    # Records that do not exist anymore are taken from the existing set of data
    # Records are closed by filling the to_date column with the current date
    closed_records = combined_df.filter(
        (F.col("shaValueOld").isNotNull()) & (F.col("shaValueNew").isNull())
    )
    if closed_records.count() > 0:
        closed_records = combined_df.filter(
            (F.col("shaValueOld").isNotNull()) & (
                F.col("shaValueNew").isNull())
        ).join(old_df, on="shaValueOld", how="inner")
        closed_records = closed_records.select(value_columns + from_to_list)
        closed_records = closed_records.withColumn("to_date", processing_date)
        all_records = all_records.union(closed_records)

    # Records that are new are taken from the new set of data
    new_records = combined_df.filter(
        (F.col("shaValueOld").isNull()) & (F.col("shaValueNew").isNotNull())
    )
    if new_records.count() > 0:
        new_records = combined_df.filter(
            (F.col("shaValueOld").isNull()) & (
                F.col("shaValueNew").isNotNull())
        ).join(new_data_frame, on="shaValueNew", how="inner")
        new_records = new_records.select(value_columns + from_to_list)
        all_records = all_records.union(new_records)

    return all_records


def assign_signalling_id(
    monitoring_values_df: DataFrame, existing_monitoring_df: DataFrame
) -> DataFrame:

    max_issue = (
        existing_monitoring_df.fillna(0, subset="signalling_id")
        .select(F.max(F.col("signalling_id")).alias("max_signalling_id"))
        .collect()[0]["max_signalling_id"]
    )
    if not max_issue:
        max_issue = 0
    existing_monitoring_df = (
        existing_monitoring_df.select(
            [F.col(c).alias(c + "_old")
             for c in existing_monitoring_df.columns]
        )
        .select(
            [
                "signalling_id_old",
                "column_name_old",
                "check_name_old",
                "table_name_old",
                "check_id_old",
            ]
        )
        .distinct()
    )
    w = Window().partitionBy("table_name").orderBy(F.col("check_id"))
    join_conditions = [
        monitoring_values_df.table_name == existing_monitoring_df.table_name_old,
        monitoring_values_df.column_name == existing_monitoring_df.column_name_old,
        monitoring_values_df.check_name == existing_monitoring_df.check_name_old,
        monitoring_values_df.check_id == existing_monitoring_df.check_id_old,
    ]
    monitoring_values_intermediate = monitoring_values_df.join(
        existing_monitoring_df, on=join_conditions, how="left"
    )

    existing_signalling_id = monitoring_values_intermediate.where(
        F.col("signalling_id_old").isNotNull()
    )
    non_existing_signalling_id = monitoring_values_intermediate.where(
        F.col("signalling_id_old").isNull()
    )
    non_existing_signalling_id = non_existing_signalling_id.withColumn(
        "signalling_id", F.row_number().over(w) + F.lit(max_issue)
    )

    monitoring_values_intermediate = existing_signalling_id.union(
        non_existing_signalling_id
    )
    monitoring_values_intermediate = monitoring_values_intermediate.withColumn(
        "signalling_id", F.coalesce(
            F.col("signalling_id_old"), F.col("signalling_id"))
    )
    monitoring_values_df = monitoring_values_intermediate.select(
        [
            "signalling_id",
            "check_id",
            "column_name",
            "check_name",
            "total_count",
            "valid_count",
            "table_name",
        ]
    )

    return monitoring_values_df


def structure_postcode(postcode: str) -> str:
    """Structure raw postcode to be in the format '1234 AB'.

    Args:
        postcode (str): Raw postcode string, at least length of 6

    Returns:
        str: Structured postcode in the predefined format of '1234 AB'
    """

    # Use regex to extract the numbers and letters from the postcode
    num = F.regexp_extract(postcode, r"^(\d{4})", 1)
    alph = F.regexp_extract(postcode, r"([A-Za-z]{2})$", 1)

    # Format postcode into `1234 AB`
    return F.concat(num, F.lit(" "), F.upper(alph))


def format_postcode(postcode: str, city: str) -> str:
    """Format Europages postcode to match the postcodes of Company.Info for
    consistency

    Args:
        postcode (str): Raw Europages company postcode
        city (str): Raw Europages company city

    Returns:
        str: Europages postcode formatted alike Company.Info
    """
    # postcode mostly looks like: '1234'
    # city mostly looks like: 'ab city_name'

    # if postcode and city are identical, take the postcode; otherwise concatenate the two into '1234ab city_name'
    reference = F.when(postcode == city, postcode).otherwise(
        F.concat(postcode, city))

    # if reference is just the city or NA, just just return empty string
    reference = F.when(
        ~reference.isin(["etten-leur", "kruiningen"]) | reference.isNotNull(),
        structure_postcode(F.split(reference, " ")[0]),
    ).otherwise("")

    return reference


def keep_one_name(default_name: str, statutory_name: str) -> str:
    """Prioritise the statutory name for a Company.Info if available, otherwise
    keep the default institution name for the company name.

    Args:
        default_name (str): Default institution name of Company.Info company
        statutory_name (str|None): Statutory name of Company.Info company

    Returns:
        str: Either the default_name or statutory_name
    """
    # Take the statutory name (on KvK) if available, otherwise take the default name (from their marketing database)
    name = F.when(statutory_name.isNotNull(), F.lower(statutory_name)).otherwise(
        F.lower(default_name)
    )

    return name
