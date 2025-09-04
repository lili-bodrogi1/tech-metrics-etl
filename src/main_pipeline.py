from prefect import flow
from extract.extract import main_flow as extract_flow
from transform.prefect_flow import validation_flow as validate_flow
from extract.STG_snowflake import get_connection

@flow(name="full_tech_metrics_pipeline")
def full_pipeline():
    #staging = get_connection("STG")

    #extract_flow(connection)

    connection = get_connection()

    validate_flow(connection)

if __name__ == "__main__":
    full_pipeline()