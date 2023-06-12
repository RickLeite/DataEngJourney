from datetime import datetime, timezone
import uuid
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain

ERP_SWITCH_DATE = datetime(2023, 6, 7, tzinfo=timezone.utc)


@dag(
    dag_id="xcom",
    schedule_interval=None,
    start_date=datetime(2023, 4, 4),
)
def task_flow():
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

    @task(
        trigger_rule="none_failed",
    )
    def clean_sales():
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
    def train_model(**context):
        model_id = str(uuid.uuid4())
        context["task_instance"].xcom_push(key="model_id", value=model_id)

        print(f"Training model {model_id}")

        pass

    @task(
        trigger_rule="none_failed",
    )
    def deploy_model(**context):
        model_id = context["task_instance"].xcom_pull(
            task_ids="train_model", key="model_id"
        )
        print(f"Deploying model {model_id}")

        pass

    start = EmptyOperator(task_id="start")

    # fmt: off
    start >> pick_erp_system >> [fetch_sales_old(), fetch_sales_new()] >> clean_sales() >> join_branch
    [start >> fetch_weather() >> clean_weather(), join_branch] >> join_data() >> train_model() >> deploy_model()


task_flow()
