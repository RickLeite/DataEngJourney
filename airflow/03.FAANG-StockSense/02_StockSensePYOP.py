from airflow.decorators import dag, task

import airflow.utils.dates

from datetime import datetime

from urllib import request
import os

from airflow.models.param import Param


@dag(
    dag_id="PyOP-StockSense",
    schedule=None,
    start_date=airflow.utils.dates.days_ago(2),
    end_date=datetime(2023, 6, 4),
    params={"output_path": Param("~/data/StockSense/wikipageviews.gz")},
)
def task_flow():
    @task()
    def get_data(**context):
        logical_date = context["logical_date"]
        output_path = context["params"]["output_path"]
        output_path = os.path.expanduser(output_path)

        year, month, day, hour, *_ = logical_date.timetuple()
        url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year}-{month:0>2}/"
            f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        print("PRINTING THE URL")
        print(url)

        request.urlretrieve(url, output_path)

    get_data()


task_flow()
