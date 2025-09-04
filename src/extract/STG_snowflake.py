import snowflake.connector

def get_connection():
  conn = snowflake.connector.connect(
      user="NIL193",
      password="SnFl452+O_kE21",
      account="qnzvtpj-sxb12589",
      warehouse="SCALETECH_WH",
      database="TECH_METRICS",
      #schema=schema
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
  
def insert_stg(cursor, dataset, batch_size=1000):
    
    if hasattr(dataset, "to_dict"):
        records = dataset.to_dict(orient="records")
    else:
        records = dataset

    columns = ["TECHNOLOGY", "SOURCE", "METRIC_NAME", "VALUE", "METRIC_DATE", "COLLECTION_DATE"]
    col_str = ", ".join(columns)

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    for batch in chunks(records, batch_size):
        values_list = []
        for row in batch:
            vals = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    vals.append("NULL")
                elif isinstance(val, str):
                    vals.append(f"'{val.replace('\'', '\'\'')}'")
                elif isinstance(val, (int, float)):
                    vals.append(str(val))
                else:
                    vals.append(f"'{str(val)}'")
            values_list.append(f"({', '.join(vals)})")
        
        values_str = ",\n".join(values_list)
        sql = f"INSERT INTO TECH_METRICS.STG.METRICS ({col_str}) VALUES\n{values_str}"
        cursor.execute(sql)
