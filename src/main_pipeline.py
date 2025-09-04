from prefect import flow
from extract.extract import main_flow as extract_flow
from transform.prefect_flow import validation_flow as validate_flow

@flow(name="full_tech_metrics_pipeline")
def full_pipeline():
    # 1️⃣ Extract
    extract_flow()

    # 2️⃣ Validate + move data
    validate_flow()

if __name__ == "__main__":
    full_pipeline()