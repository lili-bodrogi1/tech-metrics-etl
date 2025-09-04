from prefect import flow, task
import snowflake.connector
from .move_data import move_err_rows, move_good_rows

@task
def move_bad_task(connection):
    move_err_rows(connection)

@task
def move_good_task(connection):
    move_good_rows(connection)

@flow(name="validate_and_move_metrics")
def validation_flow():
    # Snowflake connection
    conn = snowflake.connector.connect(
        user="USER",
        password="PASSWORD",
        account="ACCOUNT",
        warehouse="WAREHOUSE",
        database="TECH_METRICS",
        schema="STG"
    )
    move_bad_task(conn)
    move_good_task(conn)