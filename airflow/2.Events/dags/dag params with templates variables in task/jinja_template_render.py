"""

jinja2.Template from Jinja2 to render variable templates.


Usage:

    Create Template objects using the input_path and output_path variable templates. 
    We then call the render() method of those objects and pass the ds variable 
    as a named argument to replace the {{ ds }} variable in the template.

"""


import pandas as pd

from airflow.operators.bash import BashOperator

from airflow.decorators import dag, task
from airflow.models.param import Param

import pendulum

from jinja2 import Template


@dag(
    dag_id="jinja_template_render_params",
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

        input_path_template = Template(context["params"]["input_path"])
        output_path_template = Template(context["params"]["output_path"])

        input_path = input_path_template.render(ds=context["ds"])
        output_path = output_path_template.render(ds=context["ds"])

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
        print(f"Writing stats to {output_path}")
        stats.to_csv(output_path, mode="a", index=False)
        print("Done!")

    fetch_events >> _calculate_stats()


task_flow()
