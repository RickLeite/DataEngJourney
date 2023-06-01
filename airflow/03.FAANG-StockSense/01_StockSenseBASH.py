from airflow.decorators import dag, task

import airflow.utils.dates

from airflow.operators.bash import BashOperator

from datetime import datetime


@dag(
    dag_id="BASH-StockSense",
    schedule="@once",
    start_date=airflow.utils.dates.days_ago(0),
)
def task_flow():
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "mkdir -p ~/data/StockSense &&"
            "curl -o ~/data/StockSense/wikipageviews.gz "
            " https://dumps.wikimedia.org/other/pageviews/"
            "{{ ds.split('-')[0] }}/"
            "{{ ds.split('-')[0] }}-"
            "{{ ds.split('-')[1] }}/"
            "pageviews-{{ ds.split('-')[0] }}"
            "{{ ds.split('-')[1] }}"
            "{{ ds.split('-')[2] }}-"
            "{{ ts[11:13] }}0000.gz"
        ),
    )

    get_data


task_flow()
