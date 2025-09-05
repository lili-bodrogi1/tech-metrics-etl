from .validation import not_null, is_number, positive_value, is_date, date_constraint

def error_condition():
    """
    All validations into one SQL condition for ERR table
    """
    conditions = [
        not_null(["TECHNOLOGY", "VALUE", "METRIC_DATE", "COLLECTION_DATE"]),
        is_number("VALUE"),
        positive_value("VALUE"),
        is_date("METRIC_DATE"),
        date_constraint("METRIC_DATE", "COLLECTION_DATE")
    ]
    return " OR ".join(conditions)

def move_err_rows(connection, source_table="TECH_METRICS.STG.METRICS", err_table="TECH_METRICS.ERR.METRICS_FAILS"):

    condition = error_condition()

    cur = connection.cursor()
    cur.execute(f"SELECT COALESCE(MAX(COLLECTION_DATE), '1900-01-01') FROM {err_table}")
    last_err_date = cur.fetchone()[0]

    sql = f"""
    INSERT INTO {err_table}(
        TECHNOLOGY,
        SOURCE,
        METRIC_NAME,
        VALUE,
        METRIC_DATE,
        COLLECTION_DATE,
        Err_reason
    )
    SELECT 
        TECHNOLOGY,
        SOURCE,
        METRIC_NAME,
        VALUE,
        METRIC_DATE,
        COLLECTION_DATE,
        CASE
            WHEN TECHNOLOGY IS NULL THEN 'Technology name is NULL'
            WHEN VALUE IS NULL THEN 'Value is NULL'
            WHEN COLLECTION_DATE IS NULL THEN 'collection date is NULL'
            WHEN METRIC_DATE IS NULL THEN 'Metric date is NULL'
            WHEN TRY_TO_NUMBER(VALUE) IS NULL THEN 'VALUE not a number'
            WHEN VALUE < 0 THEN 'Value is negative'
            WHEN TRY_TO_DATE(METRIC_DATE) IS NULL THEN 'Metric date is not accepted'
            WHEN METRIC_DATE > COLLECTION_DATE THEN 'Invalid metric date'
            ELSE 'Unknown'
        END AS Err_reason
    FROM {source_table}
    WHERE {condition}
    AND COLLECTION_DATE > '{last_err_date}'
    """
    connection.cursor().execute(sql)


def move_good_rows(connection, source_table="TECH_METRICS.STG.METRICS", err_table="TECH_METRICS.ERR.METRICS_FAILS"):

    sql = f"""
    CREATE TABLE IF NOT EXISTS TECH_METRICS.SILVER.METRICS(
    TECHNOLOGY STRING,
    SOURCE STRING,
    METRIC_NAME STRING,
    VALUE NUMBER,
    METRIC_DATE STRING,
    COLLECTION_DATE TIMESTAMP
    );
    """
    connection.cursor().execute(sql)

    cur = connection.cursor()
    cur.execute(f"SELECT COALESCE(MAX(COLLECTION_DATE), '1900-01-01') FROM TECH_METRICS.SILVER.METRICS")
    silver_last_load = cur.fetchone()[0]

    sql = f"""
    INSERT INTO TECH_METRICS.SILVER.METRICS
    SELECT *
    FROM {source_table} s
    WHERE NOT EXISTS (
        SELECT 1 FROM {err_table} e
        WHERE e.TECHNOLOGY = s.TECHNOLOGY
          AND e.METRIC_NAME = s.METRIC_NAME
          AND e.COLLECTION_DATE = s.COLLECTION_DATE
    )
    AND s.COLLECTION_DATE > '{silver_last_load}';
    """
    connection.cursor().execute(sql)
