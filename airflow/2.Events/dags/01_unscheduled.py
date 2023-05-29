"""

This Python file demonstrates an older method of defining DAGs, 
    which has been superseded by a more updated approach in subsequent files.


Example:
The PythonOperator in this file is being replaced by decorators, 
    which provide a more concise and intuitive way to define tasks in a DAG.

 
This File:
 The code in this file showcases an example from the book "Data Pipelines with Apache Airflow ".

The purpose of this file is to illustrate how DAGs were traditionally defined in Apache Airflow. 
    It serves as a comparison to subsequent files that showcase an updated approach for defining DAGs, like using decorators.

    
(I modified the file anyway, not identical to the book's code.)

"""


from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dlocation = "~/airflow"

dag = DAG(
    dag_id="01_unscheduled",
    schedule_interval="@daily",
    start_date=datetime(2023, 5, 1),
    end_date=datetime(2023, 5, 31),
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        f"mkdir -p {dlocation}/data && "
        f"curl -o {dlocation}/data/events.json "
        "http://127.0.0.1:5000/events"
    ),
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    print(f"Reading events from {input_path}")
    events = pd.read_json(input_path)
    print(f"Calculating stats")
    stats = (
        events.groupby(["date", "user"])
        .size()
        .reset_index(name="n_events")
        .sort_values(["date", "n_events"], ascending=[True, False])
    )
    print(f"Writing stats to {output_path}")
    stats.to_csv(output_path, index=False)
    print("Done!")


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": f"{dlocation}/data/events.json",
        "output_path": f"{dlocation}/data/stats.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats
