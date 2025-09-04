import snowflake.connector

def get_connection():
  conn = snowflake.connector.connect(
      user="NIL193",
      password="SnFl452+O_kE21",
      account="qnzvtpj-sxb12589",
      warehouse="SCALETECH_WH",
      database="TECH_METRICS",
      schema="STA"
  )
  return conn


def create_stg_table(cursor):
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS TECH_METRICS.STG.metrics(
    TECHNOLOGY STRING,
    SOURCE STRING,
    METRIC_NAME STRING,
    VALUE NUMBER,
    METRIC_DATE STRING,
    COLLECTION_DATE TIMESTAMP
  )
""")
  
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS TECH_METRICS.ERR.metrics_fails(
    TECHNOLOGY STRING,
    SOURCE STRING,
    METRIC_NAME STRING,
    VALUE NUMBER,
    METRIC_DATE STRING,
    COLLECTION_DATE TIMESTAMP,
    ERR_REASON STRING
  )
""")
  
def insert_stg(connection, dataset):
  import pandas as pd
  from snowflake.connector.pandas_tools import write_pandas

  success, nchunks, nrows, _ = write_pandas(
    connection,
    dataset,
    table_name="METRICS",
    database="TECH_METRICS",
    schema="STG"
  )
  print("insert done")
