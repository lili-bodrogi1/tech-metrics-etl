import pandas as pd
from datetime import datetime, timedelta

today = datetime.today()
first_of_this_month = today.replace(day=1)
last_month_date = (first_of_this_month - timedelta(days=1)).replace(day=1)
last_week_date = today - timedelta(days=today.weekday() + 7)

def merge_data(git_data, data_pypi, metricname1, metricname2, metricname3):
    data1 = []
    for metric, value in {
        "stars": git_data.get(metricname1, 0),
        "forks": git_data.get(metricname2, 0),
        "open_issues": git_data.get(metricname3, 0),
    }.items():
        data1.append({
            "TECHNOLOGY": git_data["name"],
            "SOURCE": "GitHub API",
            "METRIC_NAME": metric,
            "VALUE": value,
            "METRIC_DATE": datetime.strptime(git_data["updated_at"], '%Y-%m-%dT%H:%M:%SZ').date(),
            "COLLECTION_DATE": today.strftime('%Y-%m-%d %H:%M:%S')
        })

    data2 = []
    for metric, (value, date) in {
        "last_month_downloads": (data_pypi["data"]["last_month"], last_month_date),
        "last_week_downloads": (data_pypi["data"]["last_week"], last_week_date),
        "last_day_downloads": (data_pypi["data"]["last_day"], today - timedelta(days=1)),
    }.items():
        data2.append({
            "TECHNOLOGY": data_pypi["package"],
            "SOURCE": "pypi API",
            "METRIC_NAME": metric,
            "VALUE": value,
            "METRIC_DATE": date.date(),
            "COLLECTION_DATE": today.strftime('%Y-%m-%d %H:%M:%S')
        })

    dfAll = pd.DataFrame(data1 + data2)
    return dfAll


