from typing import Dict
from airflow.decorators import task, dag, task_group
from datetime import datetime
from airflow.utils.task_group import TaskGroup

from groups.process_tasks import process_tasks

default_args = {"start_date": datetime(2023, 1, 1)}


@task(do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "Samsung"
    partner_country = "ASIA"

    return {"name": partner_name, "country": partner_country}


@dag(
    dag_id="03_TaskGroup",
    description="TaskGroup From File",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "TaskGroup"],
)
def my_dag():
    partner_settings = extract()

    process_tasks(partner_settings)


my_dag()
