from typing import Dict
from airflow.decorators import task, dag
from datetime import datetime
from airflow.utils.task_group import TaskGroup

default_args = {"start_date": datetime(2023, 1, 1)}


@task(do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "Samsung"
    partner_country = "ASIA"

    return {"name": partner_name, "country": partner_country}


@task
def process_one(p_name, p_country):
    print(p_name)
    print(p_country)


@task
def process_two(p_name, p_country):
    print(p_name)
    print(p_country)


@task
def process_three(p_name, p_country):
    print(p_name)
    print(p_country)


@dag(
    dag_id="02_TaskGroup",
    description="TaskGroup ex",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "TaskGroup"],
)
def my_dag():
    partner_settings = extract()

    with TaskGroup(group_id="process_tasks") as process_tasks:
        process_one(partner_settings["name"], partner_settings["country"])
        process_two(partner_settings["name"], partner_settings["country"])
        process_three(partner_settings["name"], partner_settings["country"])


my_dag()
