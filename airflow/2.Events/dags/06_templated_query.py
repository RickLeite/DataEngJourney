import pandas as pd
import os

from airflow.operators.bash import BashOperator

from airflow.decorators import dag, task
from airflow.models.param import Param

import pendulum


@dag(
    dag_id="events_templated_query",
    schedule="@daily",
    start_date=pendulum.datetime(year=2023, month=5, day=4),
    end_date=pendulum.datetime(year=2023, month=5, day=7),
    render_template_as_native_obj=True,
    params={
        "input_path": Param("~/data/events/{{ ds }}.json", type="string"),
        "output_path": Param("~/data/stats.csv", type="string"),
    },
)
def task_flow():
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "echo site final: 'http://127.0.0.1:5000/events?start_date={{data_interval_start.strftime('%Y-%m-%d')}}&end_date={{data_interval_end.strftime('%Y-%m-%d')}}' "
            " &&"
            "mkdir -p ~/data/events/ && "
            "curl -o ~/data/events/{{ ds }}.json "
            " 'http://127.0.0.1:5000/events?start_date={{data_interval_start.strftime('%Y-%m-%d')}}&end_date={{data_interval_end.strftime('%Y-%m-%d')}}' "
        ),
    )

    @task()
    def _calculate_stats(**context):
        """Calculates event statistics."""

        jinja_env = context["dag"].get_template_env()
        input_path = jinja_env.from_string(context["params"]["input_path"]).render(
            context
        )
        output_path = jinja_env.from_string(context["params"]["output_path"]).render(
            context
        )

        print(f"Reading events from {input_path}")
        events = pd.read_json(input_path)
        print(f"Calculating stats")
        stats = (
            events.groupby(["date", "user"])
            .size()
            .reset_index(name="n_events")
            .sort_values(["date", "n_events"], ascending=[True, False])
        )
        print(stats.tail(5))
        print(f"Verifying {output_path}")

        output_file = os.path.expanduser(output_path)

        if not os.path.isfile(output_file):
            print("File not exists, creating it.")
            stats.to_csv(output_file, index=False)
        else:
            print("File exists, just appending to it.")
            stats.to_csv(output_file, mode="a", header=False, index=False)

        print("Done!")

    fetch_events >> _calculate_stats()


task_flow()
