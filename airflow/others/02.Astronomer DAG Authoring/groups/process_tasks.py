from airflow.utils.task_group import TaskGroup
from airflow.decorators import task


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


@task
def check_one():
    print("check one")


@task
def check_two():
    print("check two")


@task
def check_three():
    print("check three")


def process_tasks(partner_settings):
    with TaskGroup(group_id="process_tasks") as process_tasks:
        with TaskGroup(group_id="check_tasks") as check_tasks:
            check_one()
            check_two()
            check_three()
        (
            process_one(partner_settings["name"], partner_settings["country"])
            >> check_tasks
        )
        (
            process_two(partner_settings["name"], partner_settings["country"])
            >> check_tasks
        )
        (
            process_three(partner_settings["name"], partner_settings["country"])
            >> check_tasks
        )
    return process_tasks
