from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.operators.latest_only import LatestOnlyOperator


ERP_SWITCH_DATE = datetime(2023, 6, 7, tzinfo=timezone.utc)


@dag(
    dag_id="latest_only_Operator",
    schedule_interval="@yearly",
    start_date=datetime(2017, 4, 4),
    end_date=datetime(2037, 6, 7),
)
def task_flow():
    latest_only = LatestOnlyOperator(task_id="latest_only")

    @task
    def fetch_sales_old():
        pass

    @task
    def fetch_sales_new():
        pass

    def _pick_erp_system(**context):
        logical_date = context["logical_date"]

        if logical_date > ERP_SWITCH_DATE:
            print("SAP S/4HANA Cloud 2208")
            return "fetch_sales_new"
        else:
            print("Oracle E-Business Suite 12.2")
            return "fetch_sales_old"

    pick_erp_system = BranchPythonOperator(
        task_id="pick_the_erp_system",
        python_callable=_pick_erp_system,
    )

    @task
    def clean_sales_old():
        pass

    @task
    def clean_sales_new():
        pass

    @task
    def fetch_weather():
        pass

    @task
    def clean_weather():
        pass

    join_branch = EmptyOperator(
        task_id="join_erp_branch",
        trigger_rule="none_failed",
    )

    @task
    def join_data():
        print("Joining datasets")
        pass

    @task
    def train_model():
        print("Training model")
        pass

    @task
    def deploy_model():
        print("Deploying model")
        pass

    start = EmptyOperator(task_id="start")

    start >> pick_erp_system

    pick_erp_system >> fetch_sales_old() >> clean_sales_old() >> join_branch

    pick_erp_system >> fetch_sales_new() >> clean_sales_new() >> join_branch

    # we only run deploy_model() if lastest_only() succeeds

    (
        [start >> fetch_weather() >> clean_weather(), join_branch]
        >> join_data()
        >> train_model()
        >> [latest_only >> deploy_model()]
    )


task_flow()
