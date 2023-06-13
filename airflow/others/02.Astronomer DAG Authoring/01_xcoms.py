""" Example DAG demonstrating push/pull of XComs."""
from airflow.decorators import task, dag, task_group
from datetime import datetime
from typing import Dict

default_args = {"start_date": datetime(2023, 1, 1)}


@task
def extract_one():
    partner_name = "Astronomer"
    partner_country = "USA"

    return partner_name, partner_country


@task
def get_extract_one(returned):
    p_name, p_country = returned
    print(p_name)
    print(p_country)


@task
def extract_two():
    partner_name = "Netflix"
    partner_country = "EU"

    return {"name": partner_name, "country": partner_country}


@task
def get_extract_two(returned):
    p_name = returned["name"]
    p_country = returned["country"]
    print(p_name)
    print(p_country)


@task(multiple_outputs=True, do_xcom_push=False)
def extract_three():
    partner_name = "Amazon"
    partner_country = "SA"

    return {"name": partner_name, "country": partner_country}


@task
def get_extract_three(p_name, p_country):
    print(p_name)
    print(p_country)


@task(do_xcom_push=False)
def extract_four() -> Dict[str, str]:
    partner_name = "Samsung"
    partner_country = "ASIA"

    return {"name": partner_name, "country": partner_country}


@task
def get_extract_four(p_name, p_country):
    print(p_name)
    print(p_country)


@dag(
    dag_id="01_xcom",
    description="SubDAG example",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "xcom"],
)
def my_dag():
    get_extract_one(extract_one())

    get_extract_two(extract_two())

    partner_settings = extract_three()
    partner_name = partner_settings["name"]
    partner_country = partner_settings["country"]
    get_extract_three(partner_name, partner_country)

    f_partner_settings = extract_four()
    p_name = f_partner_settings["name"]
    p_country = f_partner_settings["country"]
    get_extract_four(p_name, p_country)


my_dag()
