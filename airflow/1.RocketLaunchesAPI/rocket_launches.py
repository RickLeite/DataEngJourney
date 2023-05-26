import airflow
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.utils import dates

import json
from pathlib import Path

import requests


# Instantiating a DAG object
rocket_dag = DAG(
    dag_id="download_rocket_launches",
    start_date=dates.days_ago(14),
    schedule_interval="@daily",
)


# BashOperator to run Bash command
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json 'https://ll.thespacedevs.com/2.2.0/launch/upcoming/'",
    # DAG task assigned
    dag=rocket_dag,
)


def _get_pictures():
    Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as file:
        launches = json.load(file)
        image_urls = [launch["image"] for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as F_image:
                    F_image.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests.exceptions.MissingSchema:
                print(
                    f"Encountered an error when downloading {image_url}, appears to be an invalid URL."
                )
            except requests.exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


get_pictures = PythonOperator(
    task_id="get_the_pictures_of_rockets",
    python_callable=_get_pictures,
    dag=rocket_dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=rocket_dag,
)

# order of task execution
download_launches >> get_pictures >> notify
