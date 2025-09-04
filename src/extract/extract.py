from prefect import flow, task
from github_api import get_github_metrics
from pypi_api import get_pypi_api
from STG_snowflake import get_connection, create_stg_table, insert_stg
from dataset import merge_data
import pandas as pd
import json
from datetime import timedelta

connection = get_connection()
cur = connection.cursor()
create_stg_table(cur)

@task
def fetch_github_task(tech):
    return get_github_metrics(tech)

@task
def fetch_pypi_task(tech):
    return get_pypi_api(tech)

@task
def merge_task(git_data, pypi_data):
    return merge_data(
        git_data,
        pypi_data,
        "stargazers_count",
        "forks_count",
        "open_issues_count"
    )

@task
def insert_stg_task(dataset):
    connection
    insert_stg(connection, dataset)

@flow(name="tech_metrics_pipeline")
def main_flow():

    with open("src/extract/technologies.json") as f:
        tech_list = json.load(f)["technologies"]

    for tech in tech_list:
        git_data = fetch_github_task.submit(tech)
        pypi_data = fetch_pypi_task.submit(tech)
        dataset = merge_task.submit(git_data.result(), pypi_data.result())
        insert_stg_task.submit(dataset.result())

if __name__ == "__main__":
    main_flow()