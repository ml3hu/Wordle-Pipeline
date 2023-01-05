import os
import logging

import pandas as pd
from time import strptime
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file, dest_file):
    if src_file.endswith(".parquet"):
        return
    elif not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    logging.info("reading file")
    table = pd.read_csv(src_file)
    logging.info("writing file")
    table.to_parquet(dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    logging.info('establishing client')
    client = storage.Client()
    logging.info('client established')
    bucket = client.bucket(bucket)
    logging.info('bucket established')
    blob = bucket.blob(object_name)
    logging.info('blob object created')
    ## For slow upload speed
    storage.blob._DEFAULT_CHUNKSIZE = 2097152 # 1024 * 1024 B * 2 = 2 MB
    storage.blob._MAX_MULTIPART_SIZE = 2097152 # 2 MB

    blob.upload_from_filename(local_file)
    logging.info('file uploaded')

def scrape(filename):
    logging.info('searching for file')
    if os.path.isfile(filename):
        return

    logging.info('scraping file')
    tables = pd.read_html('https://wordfinder.yourdictionary.com/wordle/answers/')

    while tables:
        table = tables.pop()
        if table['Wordle #'][0] <= 195:
            year = 2021
        else:
            year = 2022
        table['Year'] = year
        month_abbrev = table['Date'][0].split('. ')[0]
        month = strptime(month_abbrev, '%b').tm_mon
        table.rename(columns={'Wordle #': 'wordle_id'}, inplace=True)
        logging.info('writing file')
        if os.path.isfile(filename):
            logging.info('file concurrently added')
        else:
            table.to_csv(f'{AIRFLOW_HOME}/data/wordle_solution_{year}-{month:02d}.csv')


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def parquetize_upload_dag(
    dag,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template,
):
    with dag:
        scrape_task = PythonOperator(
            task_id="scrape_task",
            python_callable=scrape,
            op_kwargs={
                "filename": local_csv_path_template
            },
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm -f {local_csv_path_template} {local_parquet_path_template}"
        )

        scrape_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task


TWEETS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/data/tweets.csv'
TWEETS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/data/tweets.parquet'
TWEETS_GCS_PATH_TEMPLATE = "raw/tweets/tweets.parquet"

tweets_data_dag = DAG(
    dag_id="tweets_data_v4",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['wordle-pipeline'],
)

parquetize_upload_dag(
    dag=tweets_data_dag,
    local_csv_path_template=TWEETS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=TWEETS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=TWEETS_GCS_PATH_TEMPLATE,
)

SOLUTION_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/data/wordle_solution_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
SOLUTION_FILE_TEMPLATE = AIRFLOW_HOME + '/data/wordle_solution_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
SOLUTION_GCS_PATH_TEMPLATE = "raw/wordle_solution/{{ execution_date.strftime(\'%Y\') }}/wordle_solution_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

wordle_solutions_dag = DAG(
    dag_id="wordle_solutions_v9",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=5,
    tags=['wordle-pipeline'],
)

parquetize_upload_dag(
    dag=wordle_solutions_dag,
    local_csv_path_template=SOLUTION_CSV_FILE_TEMPLATE,
    local_parquet_path_template=SOLUTION_FILE_TEMPLATE,
    gcs_path_template=SOLUTION_GCS_PATH_TEMPLATE,
)