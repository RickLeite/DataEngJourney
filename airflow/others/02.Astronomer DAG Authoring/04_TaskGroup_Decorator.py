from typing import Dict
from airflow.decorators import task, dag, task_group
from datetime import datetime
from airflow.utils.task_group import TaskGroup

default_args = {"start_date": datetime(2023, 1, 1)}


@task(do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "WEX"
    partner_country = "Maine, EUA"

    return {"name": partner_name, "country": partner_country}


@task
def process_a(p_name, p_country):
    print(p_name)
    print(p_country)


@task
def process_b(p_name, p_country):
    print(p_name)
    print(p_country)


@dag(
    dag_id="04_task_group_decorator",
    description="TaskGroup ex",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "TaskGroup", "Decorator"],
)
def my_dag():
    partner_settings = extract()
    partner_name = partner_settings["name"]
    partner_country = partner_settings["country"]

    @task_group(group_id="process_tasks")
    def process_tasks(p_name, p_country):
        process_a(p_name, p_country)
        process_b(p_name, p_country)

    process_tasks(partner_name, partner_country)


my_dag()
