from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain


@dag(
    dag_id="dependencies",
    schedule=None,
    start_date=datetime(2023, 4, 4),
)
def task_flow():
    @task()
    def fetch_weather():
        pass

    @task()
    def clean_weather():
        pass

    @task()
    def fetch_sales():
        pass

    @task()
    def clean_sales():
        pass

    @task()
    def join_data():
        pass

    @task()
    def train_model():
        pass

    @task()
    def deploy_model():
        pass

    start = EmptyOperator(task_id="start")

    # start >> [fetch_weather(), fetch_sales()]
    #
    # [clean_weather(), clean_sales()] >> join_data()
    #
    # join_data() >> train_model() >> deploy_model()

    (
        chain(
            start,
            [fetch_weather(), fetch_sales()],
            [clean_weather(), clean_sales()],
            join_data(),
            train_model(),
            deploy_model(),
        )
    )


task_flow()
