from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator

import json
from datetime import datetime, timedelta

from py.load_bigquery import bq_pull


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2014, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# create dag and schedule a load interval every day at midnight (7am UTC)
with DAG('bigquery', default_args=default_args, 
         schedule_interval=timedelta(days=1)) as dag:

    # first task is to extract from GCP
    task_bq_pull = PythonOperator(
        task_id = 'pull_bigquery_dataset',
        provide_context=True,
        python_callable = bq_pull,
        dag=dag
    )

    # new task to collect

    task_one = BigQueryGetDataOperator(
        task_id='bq_get_data',
        dataset_id='bigquery-public-data',
        table_id='austin_311.311_service_requests',
        max_results='5',
        bigquery_conn_id='my_gcp_connection'
    )

