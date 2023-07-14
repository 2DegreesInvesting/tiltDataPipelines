import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType
import re


@F.udf(returnType=BooleanType())
def dq_isnull(col: str) -> bool:
    """
    Check if a given column is null or empty.

    Args:
        col (str): The column to be checked.

    Returns:
        bool: True if the column is null or empty, False otherwise.
    """
    if col:
        return True
    else:
        return False


@F.udf(returnType=BooleanType())
def dq_format(col: str, format: str) -> bool:
    """
    Check if a given column matches a specified format using regular expressions.

    Args:
        col (str): The column to be checked.
        format (str): The regular expression format to match against.

    Returns:
        bool: True if the column matches the specified format, False otherwise.
    """
    if re.match(format, col):
        return False
    else:
        return True
