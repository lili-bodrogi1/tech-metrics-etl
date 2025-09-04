def not_null(columns):
    """
    Return an SQL condition for a NOT NULL check in a list of columns
    """
    return " OR ".join([f"{col} IS NULL" for col in columns])

def is_number(column):
    """
    SQL to check if a column is a number (or convertible)
    """
    return f"TRY_TO_NUMBER({column}) IS NULL"

def positive_value(column):
    """
    Check if value >= 0
    """
    return f"{column} < 0"

def is_date(column):
    """
    If column is a valid date
    """
    return f"TRY_TO_DATE({column}) IS NULL"

def date_constraint(metric_col, collection_col):
    """
    SQL to check metric_date <= collection_date
    """
    return f"{metric_col} > {collection_col}"