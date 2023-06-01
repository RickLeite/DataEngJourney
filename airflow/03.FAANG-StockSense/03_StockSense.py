from airflow.decorators import dag, task

import airflow.utils.dates

from datetime import datetime

from urllib import request
import os

from airflow.models.param import Param

from airflow.operators.bash import BashOperator


@dag(
    dag_id="StockSense-v4",
    schedule=None,
    start_date=datetime(2023, 4, 4),
    params={
        "output_data_folder": Param("~/data/StockSense/"),
        "output_file": Param("wikipageviews"),
        "pagenames": Param(["Facebook", "Amazon", "Apple", "Microsoft", "Google"]),
    },
)
def task_flow():
    @task()
    def get_data(**context):
        logical_date = context["logical_date"]
        output_folder = os.path.expanduser(context["params"]["output_data_folder"])
        output_file = context["params"]["output_file"] + ".gz"
        output_path = os.path.join(output_folder, output_file)

        os.makedirs(output_folder, exist_ok=True)

        year, month, day, hour, *_ = logical_date.timetuple()
        url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year}-{month:0>2}/"
            f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        print("PRINTING THE URL")
        print(url)

        request.urlretrieve(url, output_path)

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip --force {{ params.output_data_folder }}{{ params.output_file }}.gz",
    )

    @task()
    def _fetch_pageviews(**context):
        pagenames = context["params"]["pagenames"]
        output_folder = os.path.expanduser(context["params"]["output_data_folder"])
        output_file = context["params"]["output_file"]
        output_path = os.path.join(output_folder, output_file)

        result = dict.fromkeys(pagenames, 0)

        with open(output_path, "r") as f:
            for line in f:
                domain_code, page_title, view_counts, _ = line.split(" ")
                if domain_code == "en" and page_title in pagenames:
                    result[page_title] = view_counts

        return result

    get_data() >> extract_gz >> _fetch_pageviews()


task_flow()
