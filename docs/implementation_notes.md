# Implementation Notes

## Original Plan
- I originally considered using **Apache Airflow** as the orchestrator. 
- However, setting up Airflow required a full local environment (with scheduler, webserver, database). 
- Given the limited time and the requirement for a public repo, I decided to use **Prefect** instead - however this is was fully new to me.

## Prefect Limitations
- The free version of Prefect Cloud does not allow creating a custom workspace or work pool.
- As a result, I could not set the pipeline to run automatically once per day.
- Currently, the flows can only be triggered manually by running the main_pipeline.py.
- Running Prefect requires a login (`prefect cloud login --key ...`).

## Great Expectations
- I wanted to add **data validation** with Great Expectations.
- This was also new to me, but as I was exploring the technologies you mentioned, I wanted to give it a try.
- On GitHub Codespaces, installing the latest version failed and due to Python 3.x compatibility issues I ended it unable to use it.
- Because of that, I had to drop this feature, although the original plan included it.

## Outcome
- Despite these limitations, the pipeline extracts, transforms, and loads data into Snowflake as required.
- Data is available in the `STG`, `SILVER` and `GOLD` layers, and pre-defined views are created for analytics teams.
