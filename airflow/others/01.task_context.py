from airflow.decorators import dag, task

import airflow.utils.dates


@dag(
    dag_id="print_task_context",
    schedule="@once",
    start_date=airflow.utils.dates.days_ago(0),
)
def task_flow():
    @task
    def _print_context(**context):
        print(context)

    _print_context()


task_flow()
