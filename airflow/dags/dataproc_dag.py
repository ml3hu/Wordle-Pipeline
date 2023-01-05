import os
import logging

from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False
}

CLUSTER_NAME = 'wordle-pipeline-cluster'
REGION = 'us-east4'
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
PYSPARK_URI = os.environ.get("PYSPARK_URI")

CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512}
    },
    'worker_config': {
        'num_instances': 4,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512}
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
        "args": [
            "--input_solutions=gs://wordle_data_lake_wordle-pipeline/raw/wordle_solution/*/*",
            "--input_tweets=gs://wordle_data_lake_wordle-pipeline/raw/tweets/*",
            "--output_solutions=gs://wordle_data_lake_wordle-pipeline/data/solutions.parquet",
            "--output_tweets=gs://wordle_data_lake_wordle-pipeline/data/tweets.parquet"
        ],},
}

with DAG(
    'dataproc_dag_v011',
    default_args=default_args,
    description='Dataproc cluster creation and deletion for Spark jobs',
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
    )

    # test_env = PythonOperator(
    #     task_id='test_env',
    #     python_callable=lambda: logging.info(os.environ.get("PYSPARK_URI")),
    # )

    submit_job = DataprocSubmitJobOperator(
        task_id='submit_job',
        job=PYSPARK_JOB,
        project_id=PROJECT_ID,
        region=REGION,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
    )

    create_cluster >> submit_job >> delete_cluster