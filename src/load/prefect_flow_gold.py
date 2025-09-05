from prefect import flow, task
from .github_table import github_daily
from .pypi_table import pypi_daily
from .views import github_views, pypi_views, technical_view
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def github_daily_task(connection):
    github_daily(connection)

@task(cache_policy=NO_CACHE)
def pypi_daily_task(connection):
    pypi_daily(connection)

@task(cache_policy=NO_CACHE)
def github_views_task(connection, result):
    github_views(connection, result)

@task(cache_policy=NO_CACHE)
def pypi_views_task(connection, result):
    pypi_views(connection, result)

@task(cache_policy=NO_CACHE)
def technical_views_task(connection,pypi,git):
    technical_view(connection,pypi,git)

@flow(name="validate_and_move_metrics")
def gold_flow(conn):
    # Snowflake connection
    gitG = github_daily_task.submit(conn)
    pypi_G = pypi_daily_task.submit(conn)
    github_views_task.submit(conn,gitG.result())
    pypi_views_task.submit(conn,pypi_G.result())
    technical_views_task.submit(conn,pypi_G.result(),gitG.result())