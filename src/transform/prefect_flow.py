from prefect import flow, task
import snowflake.connector
from .move_data import move_err_rows, move_good_rows
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def move_bad_task(connection):
    move_err_rows(connection)

@task(cache_policy=NO_CACHE)
def move_good_task(connection):
    move_good_rows(connection)

@flow(name="validate_and_move_metrics")
def validation_flow(conn):
    # Snowflake connection
    move_bad_task(conn)
    move_good_task(conn)