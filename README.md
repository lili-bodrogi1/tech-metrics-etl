# Tech Metrics ETL Pipeline

## Objective
This project collects, cleans, and aggregates data about popular data engineering technologies from GitHub and PyPI, and stores it in Snowflake.

## Data Sources
- **GitHub API:** Repository metrics (Stars, Forks, Open Issues)
- **PyPI API:** Package downloads (daily, weekly, monthly)

## Technologies Monitored
- Apache Airflow
- dbt
- Apache Spark
- Pandas
- SQLAlchemy
- Great Expectations
- Prefect
- Apache Kafka
- Snowflake-connector-python
- DuckDB

## Architecture
- **Staging:** Collect raw data from GitHub and PyPI APIs.
- **Silver Layer:** Store clean and validated raw metrics.
- **Gold Layer:** Aggregate metrics by day/week/month, ready for analytics.
- **Views:** Predefined SQL views for daily, weekly, monthly growth and summaries.

## Snowflake Setup
- Database: `TECH_METRICS`
- Warehouse: `SCALETECH_WH`
- Tables:
  - `STG.METRICS` (fully raw data)
  - `SILVER.METRICS` (clean, validated raw data)
  - `GOLD.GITHUB_DAILY` (aggregated GitHub metrics)
  - `GOLD.PYPI_DAILY` (aggregated PyPI downloads)
  - `ERR.METRICS_FAILS` (data which fails in silver layer validation)
- Views:
  - GitHub growth (daily/weekly)
  - PyPI growth (daily/weekly/monthly)
  - Combined technical popularity view

## How to Run
1. most of the used dependecies can be found in the requirements.txt
2. The code uses Prefect (in my case Prefect Cloud) to run, so possibly a Linked prefect account is needed.
3. run the main_pipeline.py. It should collect all raw data and then validate and then load it into the gold layers' table.
